package goldb

import (
	"bytes"
	"errors"
	"math/big"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Context is context of reading data via get or fetch-methods.
// Context is implemented by Transaction and Storage
type Context struct {
	qCtx         queryContext
	fPanicOnErr  bool
	rmx          sync.RWMutex
	ReadOptions  *opt.ReadOptions
	WriteOptions *opt.WriteOptions
}

type queryContext interface {
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
}

// Get returns raw data by key
func (c *Context) Get(key []byte) ([]byte, error) {
	c.rmx.RLock()
	defer c.rmx.RUnlock()

	data, err := c.qCtx.Get(key, c.ReadOptions)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	if err != nil && c.fPanicOnErr {
		panic(err)
	}
	return data, err
}

// GetInt returns uint64-data by key
func (c *Context) GetInt(key []byte) (num int64, err error) {
	_, err = c.GetVar(key, &num)
	return
}

// GetBigInt returns bigint-number by key
func (c *Context) GetBigInt(key []byte) (num *big.Int, err error) {
	_, err = c.GetVar(key, &num)
	return
}

// GetID returns uint64-data by key
func (c *Context) GetID(key []byte) (v uint64, err error) {
	data, err := c.Get(key)
	if err != nil {
		return
	}
	if data == nil {
		return 0, nil
	}
	v, err = decodeUint(data)
	if err != nil && c.fPanicOnErr {
		panic(err)
	}
	return
}

// GetStr returns string-data by key
func (c *Context) GetStr(key []byte) (s string, err error) {
	_, err = c.GetVar(key, &s)
	return
}

// GetVar get data by key and unmarshal to to variable;
// Returns true when data by key existed
func (c *Context) GetVar(key []byte, v interface{}) (bool, error) {
	if data, err := c.Get(key); err != nil {
		return false, err
	} else if data == nil {
		return false, nil
	} else if err = decodeValue(data, v); err != nil {
		if c.fPanicOnErr {
			panic(err)
		}
		return false, err
	} else {
		return true, nil
	}
}

// GetNumRows fetches data by query and calculates count rows
func (c *Context) GetNumRows(q *Query) (numRows uint64, err error) {
	err = c.execute(q, nil)
	numRows = q.NumRows
	return
}

// Exists returns true when exists results by query
func (c *Context) Exists(q *Query) (ok bool, err error) {
	var qCopy = *q
	qCopy.Limit(1)
	err = c.execute(&qCopy, nil)
	ok = qCopy.NumRows > 0
	return
}

// Fetch fetches data by query
func (c *Context) Fetch(q *Query, fnRecord func(rec Record) error) error {
	return c.execute(q, fnRecord)
}

// FetchID fetches uint64-ID by query
func (c *Context) FetchID(q *Query, fnRow func(id uint64) error) error {
	return c.execute(q, func(rec Record) error {
		if id, err := decodeUint(rec.Value); err != nil {
			return err
		} else {
			return fnRow(id)
		}
	})
}

var errBreak = errors.New("break of fetching")

// Break breaks fetching
func Break() {
	panic(errBreak)
}

// QueryValue returns first row-value by query
func (c *Context) QueryValue(q *Query, v interface{}) error {
	q.Limit(1)
	return c.Fetch(q, func(rec Record) error {
		rec.Decode(v)
		return nil
	})
}

// QueryIDs returns slice of row-id by query
func (c *Context) QueryIDs(q *Query) (ids []uint64, err error) {
	err = c.FetchID(q, func(id uint64) error {
		ids = append(ids, id)
		return nil
	})
	return
}

// QueryID returns first row-id by query
func (c *Context) QueryID(q *Query) (id uint64, err error) {
	err = c.QueryValue(q, &id)
	return
}

//------ private ------
var tail1024 = bytes.Repeat([]byte{255}, 1024)

func (c *Context) execute(q *Query, fnRow func(rec Record) error) (err error) {
	q.NumRows = 0
	pfx := q.filter
	pfxLen := len(pfx)
	start := append(pfx, q.offset...)
	nStart := len(start)
	limit := q.limit
	if limit < 0 {
		limit = 1e15
	}
	skipFirst := len(q.offset) > 0

	var iter iterator.Iterator
	var iterNext func() bool
	var fnRecordFilter = q.fnFilter

	c.rmx.RLock()
	defer c.rmx.RUnlock()

	if !q.desc { // ask
		iter = c.qCtx.NewIterator(&util.Range{Start: start}, nil)
		iterNext = func() bool { return iter.Next() }

	} else { // desc
		iter = c.qCtx.NewIterator(nil, nil)
		iter.Seek(append(start, tail1024...))
		iterNext = func() bool { return iter.Prev() }
	}

	defer func() {
		if r, _ := recover().(error); r != nil && r != errBreak {
			err = r
		}
		iter.Release()
		if err == nil {
			err = iter.Error()
		}
		if err != nil && c.fPanicOnErr {
			panic(err)
		}
	}()

	for limit > 0 && iterNext() {
		key := iter.Key()
		if !bytes.HasPrefix(key, pfx) {
			break
		}
		if skipFirst { // skip first record if record.key == startOffset
			if len(key) >= nStart && bytes.Equal(key[:nStart], start) {
				continue
			}
			skipFirst = false
		}
		val := iter.Value()
		if fnRecordFilter != nil && !fnRecordFilter(Record{key, val}) {
			continue
		}
		q.offset = key[pfxLen:]
		limit--
		if fnRow != nil {
			if err = fnRow(Record{key, val}); err != nil {
				break
			}
		}
		q.NumRows++
	}
	return
}
