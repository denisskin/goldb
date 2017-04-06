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
	seq map[Entity]uint64
	mx  sync.Mutex
}

func NewStorage(dir string, op *opt.Options) (s *Storage) {
	dir = strings.TrimSuffix(dir, "/")

	s = &Storage{
		dir: dir,
		op:  op,
		seq: map[Entity]uint64{},
	}

	if err := s.Open(); err == nil {
		return

	} else if !errors.IsCorrupted(err) {
		panic(err)
	}

	// try to recover files
	if err := s.Recover(); err != nil {
		log.Println("!!! db.Storage.Recover-ERROR: ", err)
	}
	if err := s.Open(); err != nil {
		panic(err)
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

func (s *Storage) Size() (size uint64) {
	filepath.Walk(s.dir, func(_ string, info os.FileInfo, err error) error {
		if info != nil && !info.IsDir() {
			size += uint64(info.Size())
		}
		return err
	})
	return
}

func (s *Storage) Truncate() error {
	s.mx.Lock()
	defer s.mx.Unlock()

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

	t := newTransaction(s)
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

func (s *Storage) Reindex() (err error) {
	s.mx.Lock()
	defer s.mx.Unlock()

	tmpDir := s.dir + ".reindex"
	oldDir := s.dir + ".old"

	defer os.RemoveAll(tmpDir)
	os.RemoveAll(tmpDir)
	os.RemoveAll(oldDir)

	dbOld := s.db
	dbNew, err := leveldb.OpenFile(tmpDir, s.op)
	if err != nil {
		return
	}

	iterator := dbOld.NewIterator(&util.Range{}, s.ReadOptions)

	var tr *leveldb.Transaction
	defer func() {
		iterator.Release()
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
			if tr, err = dbNew.OpenTransaction(); err != nil {
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

	if err = dbNew.Close(); err != nil {
		return
	}

	if err = os.Rename(s.dir, oldDir); err != nil {
		return
	}
	if err = os.Rename(tmpDir, s.dir); err != nil {
		return
	}

	// reopen db
	dbNew, err = leveldb.OpenFile(s.dir, s.op)
	if err != nil {
		return
	}
	s.Context.qCtx = dbNew
	s.db = dbNew
	dbOld.Close()

	os.RemoveAll(oldDir)

	return
}
