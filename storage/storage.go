package storage

import (
	"fmt"
	"strconv"

	"github.com/syndtr/goleveldb/leveldb"
	ldbiter "github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	offsetKey = "__offset"
)

// Iterator provides iteration access to the stored values.
type Iterator interface {
	// Next advances the iterator to the next key.
	Next() bool
	// Key gets the current key. If the iterator is exhausted, key will return
	// nil.
	Key() []byte
	// Value gets the current value.
	Value() ([]byte, error)
	// Release releases the iterator. After release, the iterator is not usable
	// anymore.
	Release()
	// Seek for a key in the iterator
	Seek(key []byte) bool
}

// Storage abstracts the interface for a persistent local storage
type Storage interface {
	Has(string) (bool, error)
	Get(string) ([]byte, error)
	Set(string, []byte) error
	Delete(string) error
	SetOffset(value int64) error
	GetOffset(defValue int64) (int64, error)
	Iterator() (Iterator, error)
	IteratorWithRange(start, limit []byte) (Iterator, error)
	MarkRecovered() error
	Recovered() bool
	Open() error
	Close() error
}

// store is the common interface between a transaction and db instance
type store interface {
	Has([]byte, *opt.ReadOptions) (bool, error)
	Get([]byte, *opt.ReadOptions) ([]byte, error)
	Put([]byte, []byte, *opt.WriteOptions) error
	Delete([]byte, *opt.WriteOptions) error
	NewIterator(*util.Range, *opt.ReadOptions) ldbiter.Iterator
}

type storage struct {
	// store is the active store, either db or tx
	store store
	db    *leveldb.DB
	// tx is the transaction used for recovery
	tx *leveldb.Transaction

	currentOffset int64
}

// New creates a new Storage backed by LevelDB.
func New(db *leveldb.DB) (Storage, error) {
	tx, err := db.OpenTransaction()
	if err != nil {
		return nil, fmt.Errorf("error opening leveldb transaction: %v", err)
	}

	return &storage{
		store: tx,
		db:    db,
		tx:    tx,
	}, nil
}

// Iterator returns an iterator that traverses over a snapshot of the storage.
func (s *storage) Iterator() (Iterator, error) {
	snap, err := s.db.GetSnapshot()
	if err != nil {
		return nil, err
	}

	return &iterator{
		iter: s.store.NewIterator(nil, nil),
		snap: snap,
	}, nil
}

// Iterator returns an iterator that traverses over a snapshot of the storage.
func (s *storage) IteratorWithRange(start, limit []byte) (Iterator, error) {
	snap, err := s.db.GetSnapshot()
	if err != nil {
		return nil, err
	}

	if limit != nil && len(limit) > 0 {
		return &iterator{
			iter: s.store.NewIterator(&util.Range{Start: start, Limit: limit}, nil),
			snap: snap,
		}, nil
	}
	return &iterator{
		iter: s.store.NewIterator(util.BytesPrefix(start), nil),
		snap: snap,
	}, nil

}

func (s *storage) Has(key string) (bool, error) {
	return s.store.Has([]byte(key), nil)
}

func (s *storage) Get(key string) ([]byte, error) {
	if has, err := s.store.Has([]byte(key), nil); err != nil {
		return nil, fmt.Errorf("error checking for existence in leveldb (key %s): %v", key, err)
	} else if !has {
		return nil, nil
	}

	value, err := s.store.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("error getting from leveldb (key %s): %v", key, err)
	}
	return value, nil
}

func (s *storage) GetOffset(defValue int64) (int64, error) {
	data, err := s.Get(offsetKey)
	if err != nil {
		return 0, err
	}

	if data == nil {
		return defValue, nil
	}

	value, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error decoding offset: %v", err)
	}

	return value, nil
}

func (s *storage) Set(key string, value []byte) error {
	if err := s.store.Put([]byte(key), value, nil); err != nil {
		return fmt.Errorf("error setting to leveldb (key %s): %v", key, err)
	}
	return nil
}

func (s *storage) SetOffset(offset int64) error {
	if offset > s.currentOffset {
		s.currentOffset = offset
	}

	return s.Set(offsetKey, []byte(strconv.FormatInt(offset, 10)))
}

func (s *storage) Delete(key string) error {
	if err := s.store.Delete([]byte(key), nil); err != nil {
		return fmt.Errorf("error deleting from leveldb (key %s): %v", key, err)
	}

	return nil
}

func (s *storage) MarkRecovered() error {
	if s.store == s.db {
		return nil
	}

	s.store = s.db
	return s.tx.Commit()
}

func (s *storage) Recovered() bool {
	return s.store == s.db
}

func (s *storage) Open() error {
	return nil
}

func (s *storage) Close() error {
	if s.store == s.tx {
		if err := s.tx.Commit(); err != nil {
			return fmt.Errorf("error closing transaction: %v", err)
		}
	}

	return s.db.Close()
}
