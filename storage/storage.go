package storage

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	offsetKey                    = "__offset"
	recoveryCommitOffsetInterval = 30 * time.Second
)

// Iterator provides iteration access to the stored values.
type Iterator interface {
	// Next moves the iterator to the next key-value pair and whether such a pair
	// exists. Caller should check for possible error by calling Error after Next
	// returns false.
	Next() bool
	// Err returns the error that stopped the iteration if any.
	Err() error
	// Key returns the current key. Caller should not keep references to the
	// buffer or modify its contents.
	Key() []byte
	// Value returns the current value. Caller should not keep references to the
	// buffer or modify its contents.
	Value() ([]byte, error)
	// Release releases the iterator. After release, the iterator is not usable
	// anymore.
	Release()
	// Seek moves the iterator to the begining of a key-value pair sequence that
	// is greater or equal to the given key. It returns whether at least one of
	// such key-value pairs exist. Next must be called after seeking to access
	// the first pair.
	Seek(key []byte) bool
}

// Storage is the interface Goka expects from a storage implementation.
// Implementations of this interface must be safe for any number of concurrent
// readers with one writer.
type Storage interface {
	// Opens/Initialize the storage
	Open() error

	// Close closes the storage.
	Close() error
	// Has returns whether the given key exists in the database.
	Has(key string) (bool, error)

	// Get returns the value associated with the given key. If the key does not
	// exist, a nil will be returned.
	Get(key string) ([]byte, error)

	// Set stores a key-value pair.
	Set(key string, value []byte) error

	// Delete deletes a key-value pair from the storage.
	Delete(key string) error

	// GetOffset gets the local offset of the storage.
	GetOffset(def int64) (int64, error)

	// SetOffset sets the local offset of the storage.
	SetOffset(offset int64) error

	// MarkRecovered marks the storage as recovered. Recovery message throughput
	// can be a lot higher than during normal operation. This can be used to switch
	// to a different configuration after the recovery is done.
	MarkRecovered() error

	// Iterator returns an iterator that traverses over a snapshot of the storage.
	Iterator() (Iterator, error)

	// Iterator returns a new iterator that iterates over the key-value
	// pairs. Start and limit define a half-open range [start, limit). If either
	// is nil, the range will be unbounded on the respective side.
	IteratorWithRange(start, limit []byte) (Iterator, error)
}

type storage struct {
	offset    int64
	recovered chan struct{}
	close     chan struct{}
	closeWg   sync.WaitGroup
	closed    chan struct{}

	db *leveldb.DB
}

// New creates a new Storage backed by LevelDB.
func New(db *leveldb.DB) (Storage, error) {
	return &storage{
		db:        db,
		recovered: make(chan struct{}),
		close:     make(chan struct{}),
		closed:    make(chan struct{}),
	}, nil
}

// Iterator returns an iterator that traverses over a snapshot of the storage.
func (s *storage) Iterator() (Iterator, error) {
	return &iterator{
		iter: s.db.NewIterator(nil, &opt.ReadOptions{
			DontFillCache: true,
		}),
	}, nil
}

// Iterator returns an iterator that traverses over a snapshot of the storage.
func (s *storage) IteratorWithRange(start, limit []byte) (Iterator, error) {
	if len(limit) > 0 {
		return &iterator{
			iter: s.db.NewIterator(&util.Range{Start: start, Limit: limit}, nil),
		}, nil
	}
	return &iterator{
		iter: s.db.NewIterator(util.BytesPrefix(start), nil),
	}, nil
}

func (s *storage) Has(key string) (bool, error) {
	return s.db.Has([]byte(key), nil)
}

func (s *storage) Get(key string) ([]byte, error) {
	value, err := s.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("error getting from leveldb (key %s): %v", key, err)
	}
	return value, nil
}

func (s *storage) GetOffset(defValue int64) (int64, error) {
	// if we're recovered, read offset from the storage, otherwise
	// read our local copy, as it is probably newer
	select {
	case <-s.recovered:
	default:

		localOffset := atomic.LoadInt64(&s.offset)
		// if it's 0, it's either really 0 or just has never been loaded from storage,
		// so only return if != 0
		if localOffset != 0 {
			return localOffset, nil
		}
	}

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
	if err := s.db.Put([]byte(key), value, nil); err != nil {
		return fmt.Errorf("error setting to leveldb (key %s): %v", key, err)
	}
	return nil
}

func (s *storage) putOffset(offset int64) error {
	return s.Set(offsetKey, []byte(strconv.FormatInt(offset, 10)))
}

func (s *storage) SetOffset(offset int64) error {
	atomic.StoreInt64(&s.offset, offset)

	select {
	case <-s.recovered:
		return s.putOffset(offset)
	default:
	}
	return nil
}

func (s *storage) Delete(key string) error {
	if err := s.db.Delete([]byte(key), nil); err != nil {
		return fmt.Errorf("error deleting from leveldb (key %s): %v", key, err)
	}

	return nil
}

func (s *storage) MarkRecovered() error {
	curOffset := atomic.LoadInt64(&s.offset)
	if curOffset != 0 {
		err := s.putOffset(curOffset)
		if err != nil {
			return err
		}
	}
	close(s.recovered)
	return nil
}

func (s *storage) Open() error {
	go func() {
		syncTicker := time.NewTicker(recoveryCommitOffsetInterval)
		defer syncTicker.Stop()

		for {
			select {
			case <-s.close:
				return
			case <-s.recovered:
				return
			case <-syncTicker.C:
				curOffset := atomic.LoadInt64(&s.offset)
				if curOffset != 0 {
					s.putOffset(curOffset)
				}
			}
		}
	}()

	return nil
}

func (s *storage) Close() error {
	close(s.close)
	defer close(s.closed)
	curOffset := atomic.LoadInt64(&s.offset)
	if curOffset != 0 {
		s.putOffset(curOffset)
	}
	s.closeWg.Wait()

	return s.db.Close()
}
