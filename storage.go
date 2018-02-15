package goldb

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Storage struct {
	Context
	dir string
	db  *leveldb.DB
	op  *opt.Options
	mx  sync.Mutex
}

func NewStorage(dir string, op *opt.Options) (s *Storage) {
	dir = strings.TrimSuffix(dir, "/")

	s = &Storage{
		dir: dir,
		op:  op,
	}

	if err := s.Open(); err != nil {
		if errors.IsCorrupted(err) {
			// try to recover files
			if err := s.Recover(); err != nil {
				log.Println("!!! db.Storage.Recover-ERROR: ", err)
			}
			if err := s.Open(); err != nil {
				panic(err)
			}
		} else {
			panic(err)
		}
	}
	return
}

func (s *Storage) Open() error {
	db, err := leveldb.OpenFile(s.dir, s.op)
	if err != nil {
		return err
	}
	s.db = db
	s.Context.qCtx = db
	return nil
}

func (s *Storage) Recover() error {
	if db, err := leveldb.RecoverFile(s.dir, nil); err != nil {
		return err
	} else {
		return db.Close()
	}
}

func (s *Storage) Close() error {
	if s.db != nil {
		if err := s.db.Close(); err != leveldb.ErrClosed {
			return err
		}
	}
	return nil
}

func (s *Storage) Drop() error {
	if err := s.Close(); err != nil {
		return err
	}
	return os.RemoveAll(s.dir)
}

func (s *Storage) Size() (size int64) {
	s.rmx.RLock()
	defer s.rmx.RUnlock()

	filepath.Walk(s.dir, func(_ string, info os.FileInfo, err error) error {
		if info != nil && !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return
}

func (s *Storage) Truncate() error {
	s.mx.Lock()
	defer s.mx.Unlock()

	s.rmx.Lock() // wait for all readers
	defer s.rmx.Unlock()

	if err := s.Drop(); err != nil {
		return err
	}
	return s.Open()
}

// Exec executes transaction.
// The executing transaction can be discard by methods tx.Fail(err) or by panic(err)
func (s *Storage) Exec(fn func(tx *Transaction)) (err error) {
	s.mx.Lock()
	defer s.mx.Unlock()

	t := &Transaction{}
	t.tr, t.err = s.db.OpenTransaction()
	t.Context.qCtx = t.tr
	t.Context.ReadOptions = s.ReadOptions
	t.Context.WriteOptions = s.WriteOptions

	defer func() {
		if e, _ := recover().(error); e != nil {
			t.Discard()
			err = e
		}
	}()
	if t.err != nil {
		return t.err
	}
	fn(t)
	if t.err == nil {
		t.Commit()
	} else {
		t.Discard()
	}
	return t.err
}

func (s *Storage) Vacuum() (err error) {
	s.mx.Lock()
	defer s.mx.Unlock()

	tmpDir := s.dir + ".tmp"
	oldDir := s.dir + ".old"

	defer os.RemoveAll(tmpDir)
	os.RemoveAll(tmpDir)
	os.RemoveAll(oldDir)

	// copy db-data to new tmpDB
	if err = s.copyDataToNewDB(tmpDir); err != nil {
		return
	}

	s.rmx.Lock() // wait for all readers
	defer s.rmx.Unlock()

	// close old db
	if err = s.db.Close(); err != nil {
		return
	}

	// move db dirs
	if err = os.Rename(s.dir, oldDir); err != nil {
		return
	}
	if err = os.Rename(tmpDir, s.dir); err != nil {
		return
	}

	// open new db
	if err = s.Open(); err != nil {
		return
	}

	os.RemoveAll(oldDir)

	return
}

func (s *Storage) copyDataToNewDB(dir string) (err error) {
	db, err := leveldb.OpenFile(dir, s.op)
	if err != nil {
		return
	}
	defer db.Close()

	iterator := s.db.NewIterator(&util.Range{}, s.ReadOptions)
	defer iterator.Release()

	var tr *leveldb.Transaction
	defer func() {
		if err == nil {
			err = iterator.Error()
		}
		if tr != nil {
			tr.Discard()
		}
	}()
	for i := 0; iterator.Next(); i++ {
		if err = iterator.Error(); err != nil {
			return
		}
		if i%10000 == 0 {
			if tr != nil {
				if err = tr.Commit(); err != nil {
					return
				}
			}
			if tr, err = db.OpenTransaction(); err != nil {
				return
			}
		}
		// put values to new DB
		key := iterator.Key()
		val := iterator.Value()
		if err = tr.Put(key, val, s.WriteOptions); err != nil {
			return
		}
	}
	if tr != nil {
		if err = tr.Commit(); err != nil {
			return
		}
		tr = nil
	}
	return
}

func (s *Storage) Put(key, data []byte) error {
	return s.Exec(func(tr *Transaction) {
		tr.Put(key, data)
	})
}

func (s *Storage) PutID(key []byte, id uint64) error {
	return s.Exec(func(tr *Transaction) {
		tr.PutID(key, id)
	})
}

func (s *Storage) PutInt(key []byte, num int64) error {
	return s.Exec(func(tr *Transaction) {
		tr.PutInt(key, num)
	})
}

func (s *Storage) PutVar(key []byte, v interface{}) error {
	return s.Exec(func(tr *Transaction) {
		tr.PutVar(key, v)
	})
}

func (s *Storage) Del(key []byte) error {
	return s.Exec(func(tr *Transaction) {
		tr.Del(key)
	})
}

func (s *Storage) RemoveByQuery(q *Query) error {
	return s.Exec(func(tr *Transaction) {
		tr.Fetch(q, func(rec Record) error {
			return tr.Del(rec.Key)
		})
	})
}
