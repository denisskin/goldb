package goldb

import (
	"crypto/sha256"
	"github.com/denisskin/bin"
)

type Entity int

type key []byte

func PKey(tableID Entity, id uint64) key {
	return append(encKey(int(tableID)), encKey(id)...)
}

func Key(entityID Entity, v ...interface{}) key {
	k := encKey(int(entityID))
	for _, val := range v {
		k = append(k, encKey(val)...)
	}
	return k
}

func HashKey(entityID Entity, v ...interface{}) key {
	k := encKey(int(entityID))
	for _, val := range v {
		hash := sha256.Sum256(append(k, encKey(val)...))
		k = hash[:]
	}
	return k
}

func (k key) bytes() []byte {
	return []byte(k)
}

func encKey(v interface{}) []byte {
	switch val := v.(type) {
	case []byte:
		return val
	case string:
		return append([]byte(val), 0x00)
	}
	w := bin.NewBuffer(nil)
	w.WriteVar(v)
	return w.Bytes()
}

func EncodeData(v interface{}) []byte {
	w := bin.NewBuffer(nil)
	if obj, ok := v.(bin.BinEncoder); ok {
		obj.BinEncode(&w.Writer)
	} else {
		w.WriteVar(v)
	}
	return w.Bytes()
}

func DecodeData(data []byte, v interface{}) error {
	r := bin.NewBuffer(data)
	if obj, ok := v.(bin.BinDecoder); ok {
		obj.BinDecode(&r.Reader)
	} else {
		r.ReadVar(v)
	}
	return r.Error()
}

func EncodeID(id uint64) []byte {
	buf := bin.NewBuffer(nil)
	buf.WriteVar(id)
	return buf.Bytes()
}

func DecodeID(data []byte) (uint64, error) {
	r := bin.NewBuffer(data)
	return r.ReadVarUint()
}
