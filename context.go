package goldb

import (
	"bytes"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Context is context of reading data via get or fetch-methods.
// Context is implemented by Transaction and Storage
type Context struct {
	qCtx         queryContext
	ReadOptions  *opt.ReadOptions
	WriteOptions *opt.WriteOptions
}

type queryContext interface {
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
}

// Get returns raw data by key
func (c *Context) Get(key []byte) ([]byte, error) {
	data, err := c.qCtx.Get(key, c.ReadOptions)
	if err != nil && err != leveldb.ErrNotFound {
		return nil, nil
	}
	return data, err
}

// GetNum returns uint64-data by key
func (c *Context) GetInt(key []byte) (num int64, err error) {
	_, err = c.GetVar(key, &num)
	return
}

// GetID returns uint64-data by key
func (c *Context) GetID(key []byte) (id uint64, err error) {
	data, err := c.Get(key)
	if err == nil {
		id, err = DecodeID(data)
	}
	return
}

// GetVar get data by key and unmarshal to to variable;
// Returns true when data by key existed
func (c *Context) GetVar(key []byte, v interface{}) (bool, error) {
	data, err := c.Get(key)
	if err == leveldb.ErrNotFound {
		return false, nil
	}
	if err == nil {
		err = DecodeData(data, v)
	}
	if err != nil {
		return false, err
	}
	return true, nil
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

// Fetch fetches raw-data by query
func (c *Context) Fetch(q *Query, fnRow func(value []byte) error) error {
	return c.execute(q, fnRow)
}

// FetchID fetches uint64-ID by query
func (c *Context) FetchID(q *Query, fnRow func(id uint64) error) error {
	return c.execute(q, func(v []byte) error {
		if id, err := DecodeID(v); err != nil {
			return err
		} else {
			return fnRow(id)
		}
	})
}

// FetchObject fetches object by query
func (c *Context) FetchObject(q *Query, obj interface{}, fnRow func() error) error {
	return c.execute(q, func(data []byte) error {
		if err := DecodeData(data, obj); err != nil {
			return err
		} else {
			return fnRow()
		}
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
	q.Limit(1)
	ids, err := c.QueryIDs(q)
	if len(ids) > 0 {
		return ids[0], err
	}
	return 0, err
}

//------ private ------
var tail1024 = bytes.Repeat([]byte{255}, 1024)

func (c *Context) execute(q *Query, fnRow func([]byte) error) (err error) {
	q.NumRows = 0
	pfx := q.filter
	pfxLen := len(pfx)
	start := append(pfx, q.offset...)
	nStart := len(start)
	limit := q.limit
	skipFirst := len(q.offset) > 0

	var iter iterator.Iterator
	var iterNext func() bool
	var rowID uint64
	fnRecordFilter := q.recFilter

	if !q.desc { // ask
		iter = c.qCtx.NewIterator(&util.Range{Start: start}, nil)
		iterNext = func() bool { return iter.Next() }

	} else { // desc
		iter = c.qCtx.NewIterator(nil, nil)
		iter.Seek(append(start, tail1024...))
		iterNext = func() bool { return iter.Prev() }
	}

	defer func() {
		iter.Release()
		if err == nil {
			err = iter.Error()
		}
	}()

	for limit > 0 && iterNext() {
		key := iter.Key()
		if !bytes.HasPrefix(key, pfx) {
			break
		}
		if skipFirst { // skip first record if record.key == startOffset
			//if q.strongOffset { // todo: use only strong compare (returns actual offset after each query)
			//	skipFirst = false
			//	if bytes.Equal(key, start) {
			//		continue
			//	}
			//} else {
			if len(key) >= nStart && bytes.Equal(key[:nStart], start) {
				continue
			}
			skipFirst = false
			//}
		}
		val := iter.Value()
		if fnRecordFilter != nil {
			if rowID, err = DecodeID(val); err != nil {
				break
			} else if !fnRecordFilter(rowID) {
				continue
			}
		}
		q.offset = key[pfxLen:]
		limit--
		if fnRow != nil {
			if err = fnRow(val); err != nil {
				break
			}
		}
		q.NumRows++
	}
	return
}
