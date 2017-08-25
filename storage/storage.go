package storage

import (
	"fmt"

	"github.com/lovoo/goka/codec"

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
	// Value gets the current value. Value is decoded with the codec given to the
	// storage.
	Value() (interface{}, error)
	// Release releases the iterator. After release, the iterator is not usable
	// anymore.
	Release()
}

// Storage abstracts the interface for a persistent local storage
type Storage interface {
	Has(string) (bool, error)
	Get(string) (interface{}, error)
	Set(string, interface{}) error
	SetEncoded(string, []byte) error
	Delete(string) error
	SetOffset(value int64) error
	GetOffset(defValue int64) (int64, error)
	Iterator() Iterator
	MarkRecovered() error
	Open() error
	Close() error
	Sync()
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

	codec         Codec
	offsetCodec   Codec
	currentOffset int64
}

// New creates a new Storage backed by LevelDB.
func New(db *leveldb.DB, c Codec) (Storage, error) {
	tx, err := db.OpenTransaction()
	if err != nil {
		return nil, fmt.Errorf("error opening leveldb transaction: %v", err)
	}

	return &storage{
		store: tx,
		db:    db,
		tx:    tx,

		codec:       c,
		offsetCodec: &codec.Int64{},
	}, nil
}

func (s *storage) Iterator() Iterator {
	return &iterator{
		iter:  s.store.NewIterator(nil, nil),
		codec: s.codec,
	}
}

func (s *storage) Has(key string) (bool, error) {
	return s.store.Has([]byte(key), nil)
}

func (s *storage) get(key string, codec Codec) (interface{}, error) {
	if has, err := s.store.Has([]byte(key), nil); err != nil {
		return nil, fmt.Errorf("error checking for existence in leveldb (key %s): %v", key, err)
	} else if !has {
		return nil, nil
	}

	data, err := s.store.Get([]byte(key), nil)
	if err != nil {
		return nil, fmt.Errorf("error getting from leveldb (key %s): %v", key, err)
	}

	value, err := codec.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("error decoding (key %s): %v", key, err)
	}

	return value, nil
}

func (s *storage) Get(key string) (interface{}, error) {
	return s.get(key, s.codec)
}

func (s *storage) GetOffset(defValue int64) (int64, error) {
	val, err := s.get(offsetKey, s.offsetCodec)
	if err != nil {
		return 0, err
	}
	if val == nil {
		return defValue, nil
	}
	return val.(int64), nil
}

func (s *storage) set(key string, value interface{}, codec Codec) error {
	data, err := codec.Encode(value)
	if err != nil {
		return fmt.Errorf("error encoding (key %s): %v", key, err)
	}

	return s.SetEncoded(key, data)
}

func (s *storage) Set(key string, value interface{}) error {
	return s.set(key, value, s.codec)
}

func (s *storage) SetOffset(offset int64) error {
	if offset > s.currentOffset {
		s.currentOffset = offset
	}

	return s.set(offsetKey, offset, s.offsetCodec)
}

func (s *storage) SetEncoded(key string, data []byte) error {
	if err := s.store.Put([]byte(key), data, nil); err != nil {
		return fmt.Errorf("error setting to leveldb (key %s): %v", key, err)
	}

	return nil
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

func (s *storage) Sync() {}
