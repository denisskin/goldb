package goldb

import "github.com/syndtr/goleveldb/leveldb"

type Transaction struct {
	Context
	storage *Storage
	tr      *leveldb.Transaction
	err     error
	Data    interface{}
}

func newTransaction(storage *Storage) *Transaction {
	t := &Transaction{
		storage: storage,
	}
	t.tr, t.err = storage.db.OpenTransaction()
	t.Context.qCtx = t.tr
	t.Context.ReadOptions = storage.ReadOptions
	t.Context.WriteOptions = storage.WriteOptions
	return t
}

func (t *Transaction) Discard() {
	// clear sequences cache
	t.storage.seq = map[Entity]uint64{}

	t.tr.Discard()
}

func (t *Transaction) Commit() error {
	t.err = t.tr.Commit()
	return t.err
}

func (t *Transaction) Error() error {
	return t.err
}

func (t *Transaction) Fail(err error) {
	t.err = err
}

const tabSequences Entity = 0x7fffffff

func (t *Transaction) SequenceCurVal(tab Entity) (seq uint64) {
	var key = Key(tabSequences, int(tab))
	if seq = t.storage.seq[tab]; seq != 0 {
		return
	}
	if _, err := t.GetVar(key, &seq); err != nil {
		t.Fail(err)
		return
	}
	t.storage.seq[tab] = seq
	return
}

func (t *Transaction) SequenceNextVal(tab Entity) (seq uint64) {
	seq = t.SequenceCurVal(tab) + 1
	var key = Key(tabSequences, int(tab))
	t.PutVar(key, seq)
	t.storage.seq[tab] = seq
	return seq
}

func (t *Transaction) Put(key, data []byte) error {
	if t.err != nil {
		return t.err
	}
	t.err = t.tr.Put(key, data, t.WriteOptions)
	return t.err
}

func (t *Transaction) PutID(key []byte, id uint64) error {
	return t.Put(key, EncodeID(id))
}

func (t *Transaction) PutInt(key []byte, num int64) error {
	return t.PutVar(key, num)
}

func (t *Transaction) PutVar(key []byte, v interface{}) error {
	return t.Put(key, EncodeData(v))
}

// Increment increments int-value by key
func (t *Transaction) IncInt(key []byte, inc int64) (v int64, err error) {
	if _, err = t.GetVar(key, &v); err == nil {
		v += inc
		err = t.Put(key, EncodeData(v))
	}
	return
}

func (t *Transaction) Del(key []byte) error {
	if t.err != nil {
		return t.err
	}
	t.err = t.tr.Delete(key, t.WriteOptions)
	return t.err
}
