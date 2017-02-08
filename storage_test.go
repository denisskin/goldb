package goldb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

//------------------------------------
const (
	TabTest = iota + 1
	TabUsers
	IdxUserName
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
		tr.PutVar(Key(TabTest, "A", 1), "Alice")
		tr.PutVar(Key(TabTest, "B", 2), "Bob")
		tr.PutVar(Key(TabTest, "C", 3), "Cat")
		tr.PutVar(Key(TabTest, "A", 4), "Alina")
	})

	// query all rows of TabTest
	q := NewQuery(TabTest)
	store.Fetch(q, nil)

	// query rows of TabTest where second part of key is "A"
	qA := NewQuery(TabTest, "A")
	store.Fetch(qA, nil)

	// query rows of TabTest where second part of key is "A" and third part more than 1
	qA2 := NewQuery(TabTest, "A").Offset(1)
	store.Fetch(qA2, nil)

	assert.Equal(t, 4, q.NumRows)
	assert.Equal(t, 2, qA.NumRows)
	assert.Equal(t, 1, qA2.NumRows)
}
