package goldb

import "fmt"

type Query struct {
	// query params
	filter    []byte
	offset    []byte
	desc      bool
	limit     int
	recFilter func(id uint64) bool

	// results
	NumRows int
}

func NewQuery(idxID Entity, filterVal ...interface{}) *Query {
	return &Query{
		filter: Key(idxID, filterVal...),
		limit:  100000,
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
