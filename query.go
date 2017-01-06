package goldb

import (
	"bytes"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Query struct {
	ctx queryContext

	// query params
	filter    []byte
	offset    []byte
	desc      bool
	limit     int
	recFilter func(id uint64) bool

	// results
	NumRows int
}

func newQuery(ctx queryContext, idxID Entity, filterVal ...interface{}) *Query {
	return &Query{
		ctx:    ctx,
		filter: Key(idxID, filterVal...),
		limit:  100000,
	}
}

func (q *Query) SubQuery(idxID Entity, filterVal ...interface{}) *Query {
	return newQuery(q.ctx, idxID, filterVal...)
}

func (q *Query) String() string {
	return fmt.Sprintf("{filter:%x, offset:%x, limit:%d, desc:%v}", q.filter, q.offset, q.limit, q.desc)
}

func (q *Query) First() *Query {
	return q.Limit(1).OrderAsk()
}

func (q *Query) Last() *Query {
	return q.Limit(1).OrderDesc()
}

func (q *Query) Limit(limit int) *Query {
	q.limit = limit
	return q
}

func (q *Query) Offset(offset interface{}) *Query {
	q.offset = encKey(offset)
	return q
}

func (q *Query) OrderAsk() *Query {
	q.desc = false
	return q
}

func (q *Query) OrderDesc() *Query {
	q.desc = true
	return q
}

func (q *Query) FilterRecord(fn func(id uint64) bool) *Query {
	q.recFilter = fn
	return q
}

func (q *Query) CurrentOffset() []byte {
	return q.offset
}

var tail1024 = bytes.Repeat([]byte{255}, 1024)

//func (q *Query) NumRows() (int, error) {
//	err := q.execute(nil)
//	return q.NumRows, err
//}

//func (q *Query) GetOne() (v interface{}, err error) {
//	return nil, nil
//}

func (q *Query) GetIDs() (ids []uint64, err error) {
	err = q.FetchIDs(func(id uint64) error {
		ids = append(ids, id)
		return nil
	})
	return
}

func (q *Query) GetID() (id uint64, err error) {
	q.Limit(1)
	ids, err := q.GetIDs()
	if len(ids) > 0 {
		return ids[0], err
	}
	return 0, err
}

func (q *Query) Fetch(fnRow func(value []byte) error) error {
	return q.execute(fnRow)
}

func (q *Query) FetchIDs(fnRow func(id uint64) error) error {
	return q.execute(func(v []byte) error {
		if id, err := decodeID(v); err != nil {
			return err
		} else {
			return fnRow(id)
		}
	})
}

func (q *Query) execute(fnRow func([]byte) error) (err error) {

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
		iter = q.ctx.NewIterator(&util.Range{Start: start}, nil)
		iterNext = func() bool { return iter.Next() }

	} else { // desc
		iter = q.ctx.NewIterator(nil, nil)
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
			if rowID, err = decodeID(val); err != nil {
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

/* todo:

func (q *Query) Iterate( fn func(key, data []byte) error) error {
	return q.execQuery(s.tag, s.db, func(data []byte) error {
		return fn(q.offset, data)
	})
}

func (q *Query) IterateByIndex( fn func(rowID uint64) error) error {
	return q.execQuery(s.tag, s.db, func(data []byte) error {
		if rowID, err := decodeID(data); err != nil {
			return err
		} else {
			return fn(rowID)
		}
	})
}
//func (q *Query)  IterateByTable( obj DBObject, fn func(id uint64) error) error {
//	return q.execQuery(s.tag, s.db, func(data []byte) error {
//		if rowID, err := decodeID(q.offset); err != nil {
//			return err
//		} else if err := DecodeData(data, obj); err != nil {
//			return err
//		} else {
//			return fn(rowID)
//		}
//	})
//}

*/
