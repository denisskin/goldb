package goldb

import "fmt"

type Query struct {
	// query params
	filter    []byte
	offset    []byte
	desc      bool
	limit     int64
	recFilter func(id uint64) bool

	// results
	NumRows uint64
}

func NewQuery(idxID Entity, filterVal ...interface{}) *Query {
	return &Query{
		filter: Key(idxID, filterVal...),
		limit:  -1,
	}
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

func (q *Query) Limit(limit int64) *Query {
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

func (q *Query) Order(desc bool) *Query {
	q.desc = desc
	return q
}

func (q *Query) FilterRecord(fn func(id uint64) bool) *Query {
	q.recFilter = fn
	return q
}

func (q *Query) CurrentOffset() []byte {
	return q.offset
}
