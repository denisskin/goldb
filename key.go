package goldb

import "github.com/denisskin/bin"

type Entity int

func Key(entityID Entity, vv ...interface{}) []byte {
	w := bin.NewBuffer(nil)
	w.WriteVarInt(int(entityID))
	for _, v := range vv {
		encodeKeyValue(w, v)
	}
	return w.Bytes()
}

func PrimaryKey(tableID Entity, id uint64) []byte {
	return Key(tableID, id)
}

func encodeKeyValue(w *bin.Buffer, v interface{}) *bin.Buffer {
	if s, ok := v.(string); ok {
		w.Write(append([]byte(s), 0x00))
	} else {
		w.WriteVar(v)
	}
	return w
}

func encodeValue(v interface{}) []byte {
	if obj, ok := v.(bin.Encoder); ok {
		return obj.Encode()
	}
	return bin.Encode(v)
}

func decodeValue(data []byte, v interface{}) error {
	if obj, ok := v.(bin.Decoder); ok {
		return obj.Decode(data)
	}
	return bin.Decode(data, v)
}

func encodeUint(id uint64) []byte {
	buf := bin.NewBuffer(nil)
	buf.WriteVarUint64(id)
	return buf.Bytes()
}

func decodeUint(data []byte) (uint64, error) {
	r := bin.NewBuffer(data)
	return r.ReadVarUint64()
}
