package goldb

import (
	"crypto/sha256"

	"github.com/denisskin/bin"
)

type Entity int

func PKey(tableID Entity, id uint64) []byte {
	return append(encKey(int(tableID)), encKey(id)...)
}

func Key(entityID Entity, v ...interface{}) []byte {
	k := encKey(int(entityID))
	for _, val := range v {
		k = append(k, encKey(val)...)
	}
	return k
}

func HashKey(entityID Entity, v ...interface{}) []byte {
	k := encKey(int(entityID))
	for _, val := range v {
		hash := sha256.Sum256(append(k, encKey(val)...))
		k = hash[:]
	}
	return k
}

func encKey(v interface{}) []byte {
	switch val := v.(type) {
	case []byte:
		return val
	case string:
		return append([]byte(val), 0x00)
	}
	return bin.Encode(v)
}

func EncodeData(v interface{}) []byte {
	if obj, ok := v.(bin.Encoder); ok {
		return obj.Encode()
	}
	return bin.Encode(v)
}

func DecodeData(data []byte, v interface{}) error {
	if obj, ok := v.(bin.Decoder); ok {
		return obj.Decode(data)
	}
	return bin.Decode(data, v)
}

func EncodeID(id uint64) []byte {
	buf := bin.NewBuffer(nil)
	buf.WriteVarUint64(id)
	return buf.Bytes()
}

func DecodeID(data []byte) (uint64, error) {
	r := bin.NewBuffer(data)
	return r.ReadVarUint64()
}
