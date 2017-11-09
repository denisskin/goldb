package goldb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecord_DecodeKey(t *testing.T) {
	rec := NewRecord(Key(123, "Зазеркалье", 0x4567, []byte("Alice")), &User{"Alice", 22})

	var (
		s   string
		num int
		bb  []byte
	)
	tableID := int(rec.Table())
	rec.DecodeKey(&s, &num, &bb)

	assert.Equal(t, tableID, 123)
	assert.Equal(t, "Зазеркалье", s)
	assert.Equal(t, 0x4567, num)
	assert.Equal(t, []byte("Alice"), bb)
	assert.Equal(t, []byte("Alice"), bb)
}

func TestRecord_Decode(t *testing.T) {
	rec := NewRecord(Key(123, 0x456), &User{"Alice", 22})

	var user User
	rowID := rec.RowID()
	rec.Decode(&user)

	assert.EqualValues(t, 0x456, rowID)
	assert.Equal(t, User{"Alice", 22}, user)
}
