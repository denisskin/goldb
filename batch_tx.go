package goldb

import (
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb"
)

func (s *Storage) ExecBatch(fn func(tx *Transaction)) error {
	atomic.AddInt64(&s.cntWaitingTrans, 1)
	defer atomic.AddInt64(&s.cntWaitingTrans, -1)

	// put tx to batch
	s.batchMx.Lock()
	if s.batchExst == nil {
		s.batchExst = make(chan struct{})
		go s.startBatchSync()
	}
	if s.batchCl == nil { // new batch
		s.batchCl, s.batchErr = make(chan struct{}), new(error)
	}
	cl, pErr := s.batchCl, s.batchErr
	s.batchTxs = append(s.batchTxs, fn)
	if len(s.batchTxs) == 1 {
		close(s.batchExst)
	}
	s.batchMx.Unlock()
	//---

	<-cl // waiting for batch commit
	return *pErr
}

func (s *Storage) startBatchSync() {
	defer func() {
		s.batchMx.Lock()
		s.batchExst = nil
		s.batchMx.Unlock()
	}()
	for {
		<-s.batchExst // waiting for batch txs

		// pop all txs
		s.batchMx.Lock()
		txs, cl, pErr := s.batchTxs, s.batchCl, s.batchErr
		s.batchExst, s.batchTxs, s.batchCl, s.batchErr = make(chan struct{}), nil, nil, nil
		s.batchMx.Unlock()
		//

		// commit
		err := s.Exec(func(t *Transaction) {
			for _, fn := range txs {
				fn(t)
			}
		})

		*pErr = err
		close(cl)

		if err == leveldb.ErrClosed {
			break
		}
	}
}
