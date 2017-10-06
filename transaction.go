package goldb

import "github.com/syndtr/goleveldb/leveldb"

type Transaction struct {
	Context
	tr   *leveldb.Transaction
	seq  map[Entity]uint64
	Data interface{}
}

func (t *Transaction) Discard() {
	t.tr.Discard()
}

func (t *Transaction) Commit() error {
	return t.tr.Commit()
}

func (t *Transaction) Fail(err error) {
	panic(err)
}

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

const tabSequences Entity = 0x7fffffff

func (t *Transaction) SequenceCurVal(tab Entity) (seq uint64) {
	if t.seq == nil {
		t.seq = map[Entity]uint64{}
	}
	seq, ok := t.seq[tab]
	if ok {
		return
	}
	var key = Key(tabSequences, int(tab))
	t.GetVar(key, &seq)
	t.seq[tab] = seq
	return
}

func (t *Transaction) SequenceNextVal(tab Entity) (seq uint64) {
	seq = t.SequenceCurVal(tab) + 1
	t.seq[tab] = seq
	var key = Key(tabSequences, int(tab))
	t.PutVar(key, seq)
	return seq
}

func (t *Transaction) Put(key, data []byte) {
	if err := t.tr.Put(key, data, t.WriteOptions); err != nil {
		t.Fail(err)
	}
}

func (t *Transaction) PutID(key []byte, id uint64) {
	t.Put(key, encodeUint(id))
}

func (t *Transaction) PutInt(key []byte, num int64) {
	t.PutVar(key, num)
}

func (t *Transaction) PutVar(key []byte, v interface{}) {
	t.Put(key, encodeValue(v))
}

// Increment increments int-value by key
func (t *Transaction) Increment(key []byte, delta int64) (v int64) {
	if _, err := t.GetVar(key, &v); err != nil {
		t.Fail(err)
	}
	v += delta
	t.Put(key, encodeValue(v))
	return
}

func (t *Transaction) Delete(key []byte) {
	if err := t.tr.Delete(key, t.WriteOptions); err != nil {
		t.Fail(err)
	}
}
