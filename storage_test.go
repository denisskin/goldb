package goldb

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

//------------------------------------
const (
	TabTest = iota + 1
	TabUsers
	IdxUserName
)

func TestTransaction_PutVar(t *testing.T) {
	store := NewStorage(fmt.Sprintf("/tmp/test-goldb-%x.db", rand.Int()), nil)
	defer store.Drop()

	// put vars
	err := store.Exec(func(tr *Transaction) {
		tr.PutVar(Key(TabTest, "keyA"), "Alice") // string
		tr.PutVar(Key(TabTest, "keyB"), 0xB0b)   // int
	})

	// get vars
	var v1 string
	var v2 int
	store.GetVar(Key(TabTest, "keyA"), &v1)
	store.GetVar(Key(TabTest, "keyB"), &v2)

	assert.NoError(t, err)
	assert.Equal(t, "Alice", v1)
	assert.Equal(t, 0xB0b, v2)
}

func TestContext_Fetch(t *testing.T) {
	store := NewStorage(fmt.Sprintf("/tmp/test-goldb-%x.db", rand.Int()), nil)
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

func TestTransaction_Get(t *testing.T) {
	store := NewStorage(fmt.Sprintf("/tmp/test-goldb-%x.db", rand.Int()), nil)
	defer store.Drop()

	var a, b, c struct {
		v   string
		ok  bool
		err error
	}
	key := Key(TabTest, "id")
	err := store.Exec(func(tr *Transaction) {
		tr.PutVar(key, "Alice")

		// get data from transaction
		a.ok, a.err = tr.GetVar(key, &a.v)

		// get data from storage (before commit)
		b.ok, b.err = store.GetVar(key, &b.v)
	})
	// get data from storage (after commit)
	c.ok, c.err = store.GetVar(key, &c.v)

	assert.NoError(t, err)
	assert.NoError(t, a.err)
	assert.NoError(t, b.err)
	assert.NoError(t, c.err)
	assert.True(t, a.ok)
	assert.False(t, b.ok)
	assert.True(t, c.ok)
	assert.Equal(t, "Alice", a.v)
	assert.Equal(t, "", b.v)
	assert.Equal(t, "Alice", c.v)
}

func TestTransaction_Discard(t *testing.T) {
	store := NewStorage(fmt.Sprintf("/tmp/test-goldb-%x.db", rand.Int()), nil)
	defer store.Drop()

	var a, b struct {
		v   string
		ok  bool
		err error
	}
	key := Key(TabTest, "id")
	err := store.Exec(func(tr *Transaction) {
		tr.PutVar(key, "Alice")

		a.ok, a.err = tr.GetVar(key, &a.v) // get from transaction

		tr.Fail(errors.New("transaction-fail")) // discard transaction
	})
	b.ok, b.err = store.GetVar(key, &b.v) // get from storage

	assert.Error(t, err)
	assert.NoError(t, b.err)
	assert.True(t, a.ok)
	assert.False(t, b.ok)
	assert.Equal(t, "Alice", a.v)
	assert.Equal(t, "", b.v)
}

func TestTransaction_IncSequence(t *testing.T) {
	store := NewStorage(fmt.Sprintf("/tmp/test-goldb-%x.db", rand.Int()), nil)
	defer store.Drop()

	var a, b, c, d uint64

	store.Exec(func(tr *Transaction) {
		a = tr.SequenceNextVal(TabTest)
		b = tr.SequenceNextVal(TabTest)
	})
	store.Exec(func(tr *Transaction) {
		c = tr.SequenceNextVal(TabTest)
		tr.Fail(errors.New("transaction-fail")) // discard transaction
	})
	store.Exec(func(tr *Transaction) {
		d = tr.SequenceNextVal(TabTest)
	})

	assert.True(t, b == a+1)
	assert.True(t, c == b+1)
	assert.True(t, d == b+1)
}
