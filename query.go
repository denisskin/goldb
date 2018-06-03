package goldb

import (
	"fmt"

	"github.com/denisskin/bin"
)

type Query struct {
	// query params
	filter   []byte
	offset   []byte
	desc     bool
	limit    int64
	fnFilter func(Record) bool

	// results
	NumRows uint64
}

func NewQuery(idxID Entity, filterVal ...interface{}) *Query {
	return &Query{
		filter: Key(idxID, filterVal...),
		limit:  -1,
	}
}

func (q *Query) AddFilter(filterVal ...interface{}) *Query {
	q.filter = append(q.filter, Key(0, filterVal...)[1:]...)
	return q
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
	q.offset = encodeKeyValue(bin.NewBuffer(nil), offset).Bytes()
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

func (q *Query) FilterFn(fn func(Record) bool) *Query {
	q.fnFilter = fn
	return q
}

func (q *Query) CurrentOffset() []byte {
	return q.offset
}
