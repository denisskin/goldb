package goldb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
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

func NewStorage(dir string, op *opt.Options) *Storage {
	s := &Storage{
		dir: dir,
		op:  op,
		seq: map[Entity]uint64{},
	}
	if err := s.Open(); err != nil {
		panic(err)
	}
	return s
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

func (s *Storage) Close() error {
	return s.db.Close()
}

func (s *Storage) Truncate() error {
	if err := s.Drop(); err != nil {
		return err
	}
	return s.Open()
}

func (s *Storage) Drop() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(s.dir); err != nil {
		return err
	}
	return nil
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
