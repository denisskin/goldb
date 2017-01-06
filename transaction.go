package goldb

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type Transaction struct {
	context
	//storage *Storage
	tr  *leveldb.Transaction
	err error
}

func newTransaction(storage *Storage) *Transaction {
	t := &Transaction{
	//storage: storage,
	}
	t.tr, t.err = storage.db.OpenTransaction()
	t.context.qCtx = t.tr
	return t
}

func (t *Transaction) Discard() {
	t.tr.Discard()
}

func (t *Transaction) Commit() error {
	t.err = t.tr.Commit()

	if t.err != nil {
		//log.Printf("      %s !!! LevelDB.COMMIT-ERROR: %v", t.storage.tag, t.err)
	}
	return t.err
}

func (t *Transaction) Error() error {
	return t.err
}

func (t *Transaction) Fail(err error) {
	t.err = err
}

func (t *Transaction) Del(key []byte) error {
	if t.err != nil {
		return t.err
	}
	//	log.Printf("      %s DB.del(%x)", t.storage.tag, k.bytes())

	t.err = t.tr.Delete(key, nil)
	return t.err
}

func (t *Transaction) Put(key []byte, v interface{}) error {
	if t.err != nil {
		return t.err
	}
	t.err = t.tr.Put(key, EncodeData(v), nil)
	return t.err
}

func (t *Transaction) PutID(key []byte, id uint64) error {
	return t.Put(key, EncodeData(id))
}
