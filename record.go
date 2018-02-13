package goldb

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/denisskin/bin"
)

type Record struct {
	Key   []byte
	Value []byte
}

var errInvalidKeyData = errors.New("goldb: invalid key data")

func NewRecord(key []byte, v interface{}) Record {
	return Record{key, encodeValue(v)}
}

func (r Record) String() string {
	return fmt.Sprintf("record(%x:%x)", r.Key, r.Value)
}

//------- key ---------
func (r Record) Table() Entity {
	id, _ := decodeUint(r.Key)
	return Entity(id)
}

func (r Record) DecodeKey(vv ...interface{}) error {
	buf := bin.NewBuffer(r.Key)
	buf.ReadVarInt64() // read tableID
	for _, v := range vv {
		if str, ok := v.(*string); ok { // special case - read string in Key
			if n := bytes.IndexByte(r.Key[int(buf.CntRead):], 0); n < 0 {
				return errInvalidKeyData
			} else {
				s := make([]byte, n+1)
				if _, err := buf.Read(s); err == nil {
					*str = string(s[:n])
				}
			}
		} else {
			buf.ReadVar(v)
		}
	}
	return buf.Error()
}

func (r Record) KeyOffset(q *Query) []byte {
	return r.Key[len(q.filter):]
}

//------ value --------------
func (r Record) RowID() (id uint64) {
	var idx int64
	bin.Decode(r.Key, &idx, &id)
	return
}

func (r Record) Decode(v interface{}) error {
	return bin.Decode(r.Value, v)
}

func (r Record) ValueID() (id uint64) {
	r.Decode(&id)
	return
}

func (r Record) ValueStr() (v string) {
	r.Decode(&v)
	return
}

func (r Record) ValueInt() (v int64) {
	r.Decode(&v)
	return
}
