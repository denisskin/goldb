package goldb

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"

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
	id, err := decodeUint(r.Key)
	panicOnErr(err)
	return Entity(id)
}

func (r Record) DecodeKey(vv ...interface{}) {
	buf := bin.NewBuffer(r.Key)
	buf.ReadVarInt64() // read tableID
	panicOnErr(buf.Error())
	for _, v := range vv {
		if str, ok := v.(*string); ok { // special case - read string in Key
			if n := bytes.IndexByte(r.Key[int(buf.CntRead):], 0); n < 0 {
				panic(errInvalidKeyData)
			} else {
				s := make([]byte, n+1)
				if _, err := buf.Read(s); err == nil {
					*str = string(s[:n])
				}
			}
		} else {
			buf.ReadVar(v)
		}
		panicOnErr(buf.Error())
	}
}

func (r Record) KeyOffset(q *Query) []byte {
	return r.Key[len(q.filter):]
}

//------ value --------------
func (r Record) RowID() (id uint64) {
	panicOnErr(bin.Decode(r.Key, new(int64), &id))
	return
}

func (r Record) Decode(v interface{}) {
	panicOnErr(decodeValue(r.Value, v))
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

func (r Record) ValueBigInt() (v *big.Int) {
	r.Decode(&v)
	return
}
