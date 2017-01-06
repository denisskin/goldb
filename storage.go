package goldb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"os"
	"path/filepath"
)

type Storage struct {
	context
	seq uint64
	dir string
	db  *leveldb.DB
}

func NewStorage(dir string) *Storage {
	s := &Storage{
		dir: dir,
	}
	if err := s.Open(); err != nil {
		panic(err)
	}
	return s
}

func (s *Storage) Open() error {
	// TODO: RecoverFile ???
	//opt.Options{}

	db, err := leveldb.OpenFile(s.dir, nil)
	if err != nil {
		return err
	}
	s.db = db
	s.context.qCtx = db
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

func (s *Storage) Exec(fn func(*Transaction)) (err error) {
	defer func() {
		if e, _ := recover().(error); e != nil {
			err = e
		}
	}()
	t := s.OpenTransaction()
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

func (s *Storage) OpenTransaction() *Transaction {
	return newTransaction(s)
}
