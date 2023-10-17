package storage

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	ldbiter "github.com/syndtr/goleveldb/leveldb/iterator"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	recoveryCommitOffsetInterval = 30 * time.Second
)

type levelDBStorage struct {
	offset    int64
	recovered chan struct{}
	close     chan struct{}
	closeWg   sync.WaitGroup
	closed    chan struct{}

	db *leveldb.DB
}

// New creates a new Storage backed by LevelDB.
func New(db *leveldb.DB) (Storage, error) {
	return &levelDBStorage{
		db:        db,
		recovered: make(chan struct{}),
		close:     make(chan struct{}),
		closed:    make(chan struct{}),
	}, nil
}

// Iterator returns an iterator that traverses over a snapshot of the storage.
func (s *levelDBStorage) Iterator() (Iterator, error) {
	return &iterator{
		iter: s.db.NewIterator(nil, &opt.ReadOptions{
			DontFillCache: true,
		}),
	}, nil
}

// Iterator returns an iterator that traverses over a snapshot of the storage.
func (s *levelDBStorage) IteratorWithRange(start, limit []byte) (Iterator, error) {
	if len(limit) > 0 {
		return &iterator{
			iter: s.db.NewIterator(&util.Range{Start: start, Limit: limit}, nil),
		}, nil
	}
	return &iterator{
		iter: s.db.NewIterator(util.BytesPrefix(start), nil),
	}, nil
}

func (s *levelDBStorage) Has(key string) (bool, error) {
	return s.db.Has([]byte(key), nil)
}

func (s *levelDBStorage) Get(key string) ([]byte, error) {
	value, err := s.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("error getting from leveldb (key %s): %v", key, err)
	}
	return value, nil
}

func (s *levelDBStorage) GetOffset(defValue int64) (int64, error) {
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

func (s *levelDBStorage) Set(key string, value []byte) error {
	if err := s.db.Put([]byte(key), value, nil); err != nil {
		return fmt.Errorf("error setting to leveldb (key %s): %v", key, err)
	}
	return nil
}

func (s *levelDBStorage) putOffset(offset int64) error {
	return s.Set(offsetKey, []byte(strconv.FormatInt(offset, 10)))
}

func (s *levelDBStorage) SetOffset(offset int64) error {
	atomic.StoreInt64(&s.offset, offset)

	select {
	case <-s.recovered:
		return s.putOffset(offset)
	default:
	}
	return nil
}

func (s *levelDBStorage) Delete(key string) error {
	if err := s.db.Delete([]byte(key), nil); err != nil {
		return fmt.Errorf("error deleting from leveldb (key %s): %v", key, err)
	}

	return nil
}

func (s *levelDBStorage) MarkRecovered() error {
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

func (s *levelDBStorage) Open() error {
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

func (s *levelDBStorage) Close() error {
	close(s.close)
	defer close(s.closed)
	curOffset := atomic.LoadInt64(&s.offset)
	if curOffset != 0 {
		s.putOffset(curOffset)
	}
	s.closeWg.Wait()

	return s.db.Close()
}

// iterator wraps an Iterator implementation and handles the value decoding and
// offset key skipping.
type iterator struct {
	iter ldbiter.Iterator
}

func (i *iterator) Next() bool {
	next := i.iter.Next()
	if string(i.iter.Key()) == offsetKey {
		next = i.iter.Next()
	}

	return next
}

func (i *iterator) Err() error {
	return i.iter.Error()
}

func (i *iterator) Key() []byte {
	return i.iter.Key()
}

func (i *iterator) Value() ([]byte, error) {
	data := i.iter.Value()
	if data == nil {
		return nil, nil
	}

	return data, nil
}

func (i *iterator) Release() {
	i.iter.Release()
}

func (i *iterator) Seek(key []byte) bool {
	return i.iter.Seek(key)
}
