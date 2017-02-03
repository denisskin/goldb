package goldb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"log"
	"os"
	"path/filepath"
)

type Storage struct {
	Context
	dir string
	db  *leveldb.DB
	op  *opt.Options
	seq map[Entity]uint64
}

func NewStorage(dir string, op *opt.Options) (s *Storage) {
	s = &Storage{
		dir: dir,
		op:  op,
		seq: map[Entity]uint64{},
	}
	if err := s.Open(); err == nil {
		return
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
	// TODO: RecoverFile ???

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

func (s *Storage) Truncate() error {
	tr, _ := s.db.OpenTransaction()
	defer tr.Discard()

	if err := s.Drop(); err != nil {
		return err
	}
	return s.Open()
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

// Exec executes transaction.
// The executing transaction can be discard by methods tx.Fail(err) or by panic(err)
func (s *Storage) Exec(fn func(tx *Transaction)) (err error) {
	t := s.OpenTransaction()
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

// OpenTransaction opens transaction
func (s *Storage) OpenTransaction() *Transaction {
	return newTransaction(s)
}
