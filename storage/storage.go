package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/syndtr/goleveldb/leveldb/iterator"

	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/logger"
	"github.com/lovoo/goka/snapshot"
)

const (
	mxSnapshotRatio     = "snapshot_updateratio"
	mxSnapshotFlushsize = "snapshot_flushsize"
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
	Open() error
	Close() error
	Sync()
}

// DefaultStorageSnapshotInterval is the default interval, in which snapshots are being stored to
// disks in the storage.
const DefaultStorageSnapshotInterval = 1 * time.Minute

type storage struct {
	log              logger.Logger
	db               *levelDbStorage
	codec            Codec
	offsetCodec      Codec
	snap             *snapshot.Snapshot
	snapshotInterval time.Duration
	flusherDone      chan bool
	flusherCancel    chan bool

	mOffset       sync.Mutex
	currentOffset int64
}

type iter struct {
	current  int
	keys     []string
	snapshot *snapshot.Snapshot

	iter    iterator.Iterator
	storage *storage
}

func (i *iter) snapExhausted() bool {
	return len(i.keys) <= i.current
}

func (i *iter) Next() bool {
	i.current++

	// find the next non deleted snapshot value
	for !i.snapExhausted() {
		if string(i.Key()) == offsetKey {
			i.current++
			continue
		}

		if val, err := i.Value(); err != nil {
			// TODO (franz): sure we want a panic? Not returning the error?
			i.storage.log.Panicf("error getting snapshot value in next: %v", err)
		} else if val != nil {
			return true
		}

		i.current++
	}

	// find next value in db that was not in snapshot
	for i.iter.Next() {
		if string(i.Key()) == offsetKey {
			continue
		}

		if !i.snapshot.Has(string(i.Key())) {
			return true
		}
	}

	return false
}

func (i *iter) Key() []byte {
	if i.snapExhausted() {
		return i.iter.Key()
	}

	return []byte(i.keys[i.current])
}

func (i *iter) Value() (interface{}, error) {
	if i.snapExhausted() {
		dec, err := i.storage.codec.Decode(i.iter.Value())
		if err != nil {
			return nil, fmt.Errorf("error decoding value (key: %s): %v", string(i.Key()), err)
		}
		return dec, nil
	}

	val, enc, err := i.snapshot.Get(string(i.Key()), i.storage.stateCloner(i.storage.codec))
	if err != nil {
		return nil, fmt.Errorf("error getting value in iterator: %v", err)
	}

	if enc {
		return i.storage.codec.Decode(val.([]byte))
	}

	return val, nil
}

func (i *iter) Release() {
	i.iter.Release()
}

// New news new
func New(log logger.Logger, fn string, c Codec, m metrics.Registry, snapshotInterval time.Duration) (Storage, error) {
	db, err := newLeveldbStorage(log, fn, true)
	if err != nil {
		return nil, err
	}

	snapshotRatio := metrics.GetOrRegisterGaugeFloat64(mxSnapshotRatio, m)
	snapshotFlushsize := metrics.GetOrRegisterGauge(mxSnapshotFlushsize, m)
	snap := snapshot.New(1)
	snap.MetricsHook = func(numUpdates int64, numElements int) {
		if numElements == 0 {
			snapshotRatio.Update(0)
		} else {
			snapshotRatio.Update(float64(numUpdates) / float64(numElements))
		}
		snapshotFlushsize.Update(int64(numElements))
	}
	return &storage{
		log:              log,
		db:               db,
		codec:            c,
		offsetCodec:      &codec.Int64{},
		snap:             snap,
		snapshotInterval: snapshotInterval,
		flusherDone:      make(chan bool),
		flusherCancel:    make(chan bool),
	}, nil
}

func (s *storage) Iterator() Iterator {
	return &iter{-1, s.snap.Keys(), s.snap, s.db.Iterator(), s}
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

func (s *storage) get(key string, codec Codec) (interface{}, error) {
	// is entry in snapshot?
	value, encoded, err := s.snap.Get(key, s.stateCloner(codec))
	if err != nil {
		return nil, fmt.Errorf("Error retrieving value from local snapshot: %v", err)
	}
	if value != nil {
		if !encoded {
			return value, nil
		}
		return codec.Decode(value.([]byte))
	}

	storedValue, err := s.db.Get(key)
	if err != nil {
		return nil, fmt.Errorf("Error retrieving value for %s from local storage: %v", key, err)
	}
	if storedValue == nil {
		return nil, nil
	}

	decodedValue, err := codec.Decode(storedValue)
	if err != nil {
		return nil, fmt.Errorf("Error decoding value from storage for key %s: %v", key, err)
	}
	return decodedValue, nil
}

func (s *storage) Has(key string) (bool, error) {
	value, _, err := s.snap.Get(key, s.stateCloner(s.codec))
	if err != nil {
		return false, fmt.Errorf("Error getting value from snapshot: %v", err)
	}
	if value != nil {
		return true, nil
	}

	localStoreHas, err := s.db.Has(key)
	if err != nil {
		return false, fmt.Errorf("Error checking whether the local storage has value %s: %v", key, err)
	}
	return localStoreHas, nil
}

func (s *storage) SetEncoded(key string, data []byte) error {
	s.snap.Set(key, data, true, s.stateFlush, nil)
	return nil
}

func (s *storage) Set(key string, value interface{}) error {
	// store the raw value in the snapshot
	s.snap.Set(key, value, false, s.stateFlush, nil)
	return nil
}

func (s *storage) Delete(key string) error {
	s.snap.Set(key, nil, false, s.stateDelete, nil)
	return nil
}

func (s *storage) SetOffset(offset int64) error {
	s.mOffset.Lock()
	defer s.mOffset.Unlock()
	if offset > s.currentOffset {
		s.currentOffset = offset
	}
	return nil
}

func (s *storage) Open() error {
	go func() {
		s.storageSnapshotFlusher()
		close(s.flusherDone)
	}()
	return nil
}

func (s *storage) Close() error {
	s.snap.Cancel()
	close(s.flusherCancel)
	<-s.flusherDone
	return s.db.Close()
}

func (s *storage) storageSnapshotFlusher() {
	ticker := time.NewTicker(s.snapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.mOffset.Lock()
			offset := s.currentOffset
			s.mOffset.Unlock()

			err := s.snap.Flush(func() error {
				off, err := s.offsetCodec.Encode(offset)
				if err != nil {
					return fmt.Errorf("Error encoding offset after flush: %v", err)
				}
				return s.db.Set(offsetKey, off)
			})
			if err != nil {
				s.log.Printf("%v", err)
			}
		case <-s.flusherCancel:
			return
		}
	}

}

func (s *storage) stateFlush(key string, value interface{}, encoded bool) {
	var (
		err          error
		encodedValue []byte
	)
	if encoded {
		var isByte bool
		encodedValue, isByte = value.([]byte)
		if !isByte {
			err = fmt.Errorf("Encoded value is not of type []byte but %T", value)
		}
	} else {
		encodedValue, err = s.codec.Encode(value)
	}
	if err != nil {
		s.log.Printf("Error encoding state (key=%s) for writing to local storage: %v", key, err)
		return
	}

	err = s.db.Set(key, encodedValue)
	if err != nil {
		s.log.Printf("Error writing state (key=%s) to local storage: %v", key, err)
	}
}

func (s *storage) stateDelete(key string, value interface{}, encoded bool) {
	if err := s.db.Delete(key); err != nil {
		s.log.Printf("error deleting from local storage (key %s)", key)
	}
}

func (s *storage) stateCloner(c Codec) func(key string, value interface{}, encoded bool) (interface{}, error) {
	return func(key string, value interface{}, encoded bool) (interface{}, error) {

		// if encoded, just copy the byte-slice and return the copy.
		if encoded {
			encodedData, isByte := value.([]byte)
			if !isByte {
				return nil, fmt.Errorf("Encoded value is not of type []byte but %T", value)
			}
			dst := make([]byte, len(encodedData))
			copy(dst, encodedData)
			return dst, nil
		}

		// if not encoded, we need to re-encode to get a proper copy
		encodedValue, err := c.Encode(value)
		if err != nil {
			return nil, fmt.Errorf("Error encoding value for clone: %v", err)
		}
		decoded, err := c.Decode(encodedValue)
		if err != nil {
			return nil, fmt.Errorf("Error decoding value for clone: %v", err)
		}
		return decoded, nil
	}
}

func (s *storage) Sync() {
	s.snap.Check()
}
