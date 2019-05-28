package goldb

import (
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

func (s *Storage) ExecBatch(fn func(tx *Transaction)) error {
	atomic.AddInt64(&s.cntWaitingTrans, 1)
	defer atomic.AddInt64(&s.cntWaitingTrans, -1)

	var cl chan struct{}
	var pErr *error

	// put tx to batch
	s.batchMx.Lock()
	if !s.batchSync {
		s.batchSync = true
		go s.startBatchSync()
	}
	if cl, pErr = s.batchCl, s.batchErr; cl == nil { // new batch
		cl, pErr = make(chan struct{}), new(error)
		s.batchCl, s.batchErr = cl, pErr
	}
	s.batchTxs = append(s.batchTxs, fn)
	s.batchMx.Unlock()
	//---

	<-cl // waiting for batch commit
	return *pErr
}

func (s *Storage) startBatchSync() {
	defer func() {
		s.batchMx.Lock()
		s.batchSync = false
		s.batchMx.Unlock()
	}()
	for {
		// pop all txs
		s.batchMx.Lock()
		txs, cl, pErr := s.batchTxs, s.batchCl, s.batchErr
		s.batchTxs, s.batchCl, s.batchErr = nil, nil, nil
		s.batchMx.Unlock()
		//
		if len(txs) == 0 {
			time.Sleep(time.Millisecond)
			continue
		}
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
