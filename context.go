package goldb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Context interface {
	NewQuery(idxID Entity, filterVal ...interface{}) *Query
	Get(pk key, v interface{}) (bool, error)
	GetUint(k key) (v uint64, err error)
}

type context struct {
	qCtx queryContext
}

type queryContext interface {
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
}

func (c *context) NewQuery(idxID Entity, filterVal ...interface{}) *Query {
	return newQuery(c.qCtx, idxID, filterVal...)
}

func (c *context) getVal(pk key, v interface{}) (bool, error) {
	data, err := c.qCtx.Get(pk.bytes(), nil)

	if err != nil && err != leveldb.ErrNotFound {
		//log.Printf("      !!! LevelDB.GET-ERROR: %v", err)
	}
	if err == nil {
		err = DecodeData(data, v)
	}
	if err != nil {
		if err == leveldb.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *context) Get(pk key, v interface{}) (bool, error) {
	return c.getVal(pk, v)
}

func (c *context) GetUint(k key) (v uint64, err error) {
	_, err = c.getVal(k, &v)
	return
}

func (c *context) GetInt(k key) (v uint64, err error) {
	_, err = c.getVal(k, &v)
	return
}
