package goldb

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

//------------------------------------
const (
	TestTable = iota + 1
)

func TestStorage_Close(t *testing.T) {
	store := NewStorage(fmt.Sprintf("%s/test-goldb-%x.db", os.TempDir(), rand.Int()), nil)
	defer store.Drop()

	err1 := store.Close()
	err2 := store.Close()

	assert.NotNil(t, store)
	assert.NoError(t, err1)
	assert.NoError(t, err2)
}

func TestContext_Fetch(t *testing.T) {
	store := NewStorage(fmt.Sprintf("%s/test-goldb-%x.db", os.TempDir(), rand.Int()), nil)
	defer store.Drop()

	// put data
	store.Exec(func(tr *Transaction) {
		tr.PutVar(Key(TestTable, "A", 1), "Alice")
		tr.PutVar(Key(TestTable, "B", 2), "Bob")
		tr.PutVar(Key(TestTable, "C", 3), "Cat")
		tr.PutVar(Key(TestTable, "A", 4), "Alina")
	})

	// query all rows of TestTable
	q := NewQuery(TestTable)
	store.Fetch(q, nil)

	// query rows of TestTable where second part of key is "A"
	qA := NewQuery(TestTable, "A")
	store.Fetch(qA, nil)

	// query rows of TestTable where second part of key is "A" and third part more than 1
	qA2 := NewQuery(TestTable, "A").Offset(1)
	store.Fetch(qA2, nil)

	assert.Equal(t, 4, int(q.NumRows))
	assert.Equal(t, 2, int(qA.NumRows))
	assert.Equal(t, 1, int(qA2.NumRows))
}

func fileExists(path string) bool {
	st, _ := os.Stat(path)
	return st != nil
}

func TestStorage_Vacuum(t *testing.T) {
	store := NewStorage(fmt.Sprintf("%s/test-goldb-%x.db", os.TempDir(), rand.Int()), nil)
	defer store.Drop()

	const countRows = 3000

	// insert test data
	log.Println("Storage.Vacuum: inserting test data...")
	for i := 0; i < countRows; i++ {
		store.Exec(func(tr *Transaction) {
			tr.PutVar(Key(TestTable, "LongLongLongKey%d", i*15551%countRows), "The string value")
		})
	}

	// reindex db
	sizeBefore := store.Size()
	log.Println("Storage.Vacuum: start reindexing.  Storage-size: ", sizeBefore)

	err := store.Vacuum()

	sizeAfter := store.Size()
	log.Println("Storage.Vacuum: finish reindexing. Storage-size: ", sizeAfter)

	// asserts
	assert.NoError(t, err)
	assert.True(t, sizeAfter < sizeBefore/50)
	assert.True(t, fileExists(store.dir))
	assert.False(t, fileExists(store.dir+".reindex"))
	assert.False(t, fileExists(store.dir+".old"))
}

func TestStorage_Vacuum_Parallel(t *testing.T) {
	store := NewStorage(fmt.Sprintf("%s/test-goldb-%x.db", os.TempDir(), rand.Int()), nil)
	defer store.Drop()

	// insert test data
	const countRows = 1000
	for i := 0; i < countRows; i++ {
		store.Exec(func(tr *Transaction) {
			tr.PutVar(Key(TestTable, i), "First value")
		})
	}

	// parallel start reindexing
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err := store.Vacuum()
		assert.NoError(t, err)
		wg.Done()
	}()

	// update all rows
	for i := 0; i < countRows; i++ {
		err := store.Exec(func(tr *Transaction) {
			tr.PutVar(Key(TestTable, i), "Second value")
		})
		assert.NoError(t, err)
	}

	// wait reindexing
	wg.Wait()

	// check data
	for i := 0; i < countRows; i++ {
		var val string
		store.GetVar(Key(TestTable, i), &val)
		assert.Equal(t, "Second value", val)
	}
}
