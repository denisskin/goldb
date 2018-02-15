package goldb

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"xnet/std/sys"

	"github.com/stretchr/testify/assert"
)

//------------------------------------
func TestStorage_Dump(t *testing.T) {
	store := NewStorage(fmt.Sprintf("%s/test-goldb-%x.db", os.TempDir(), rand.Int()), nil)
	defer store.Drop()
	putTestValues(store, 100)

	err := store.Dump(store.dir+".dump", nil)

	t.Logf(`
		  DB-size: %d
		Dump-size: %d`,
		store.Size(), sys.FileSize(store.dir+".dump"),
	)

	assert.NoError(t, err)
}

func TestStorage_Restore(t *testing.T) {
	store1 := NewStorage(fmt.Sprintf("%s/test-goldb-%x.db", os.TempDir(), rand.Int()), nil)
	store2 := NewStorage(fmt.Sprintf("%s/test-goldb-%x.db", os.TempDir(), rand.Int()), nil)
	defer store1.Drop()
	defer store2.Drop()
	putTestValues(store1, 100)
	dumpFilePath := store1.dir + ".dump"
	store1.Dump(dumpFilePath, nil)

	err := store2.Restore(dumpFilePath)

	assert.NoError(t, err)

	store1.Fetch(NewQuery(TestTable), func(rec Record) error {
		val, err := store2.Get(rec.Key)

		assert.NoError(t, err)
		assert.Equal(t, rec.Value, val)
		return err
	})
}

func putTestValues(store *Storage, n int) {
	store.Exec(func(tr *Transaction) {
		for i := 0; i < n; i++ {
			tr.PutVar(
				Key(TestTable, "Key", i),
				fmt.Sprintf("Value %x", i),
			)
		}
	})
}
