package badger

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/storage"
)

const defaultDiscardRatio = 0.5

type Storage struct {
	db      *badger.DB
	txn     *badger.Txn
	m       sync.RWMutex
	txns    int64
	maxTxns int64

	currentOffset int64
	recovered     int64
}

func New(db *badger.DB, maxTxns int) (storage.Storage, error) {
	return &Storage{
		db:      db,
		txn:     db.NewTransaction(true),
		maxTxns: int64(maxTxns),
	}, nil
}

func (s *Storage) Close() error {
	errs := new(goka.Errors)
	if err := s.txn.Commit(nil); err != nil {
		errs.Collect(err)
	}
	if err := s.db.Close(); err != nil {
		errs.Collect(err)
	}
	return errs.ErrorOrNil()
}

func (s *Storage) Iterator() (storage.Iterator, error) {
	txn := s.db.NewTransaction(false)
	return &iterator{
		txn:  txn,
		iter: txn.NewIterator(badger.DefaultIteratorOptions),
	}, nil
}

func (s *Storage) Has(key string) (bool, error) {
	v, err := s.Get(key)
	return v != nil, err
}

func (s *Storage) Get(key string) ([]byte, error) {
	s.m.RLock()
	defer s.m.RUnlock()
	item, err := s.txn.Get([]byte(key))
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (s *Storage) Set(key string, value []byte) error {
	s.m.RLock()
	defer s.m.RUnlock()

	if err := s.txn.Set([]byte(key), value); err != badger.ErrTxnTooBig {
		// return if err == nil or err != txnTooBig
		return err
	}
	// transaction is too big, we need to commit it and create a new one
	if err := s.commit(); err != nil {
		return err
	}
	return s.txn.Set([]byte(key), value)
}

func (s *Storage) GetOffset(defValue int64) (int64, error) {
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

func (s *Storage) SetOffset(offset int64) error {
	if offset > s.currentOffset {
		s.currentOffset = offset
	}

	return s.Set(offsetKey, []byte(strconv.FormatInt(offset, 10)))
}

func (s *Storage) Delete(key string) error {
	s.m.RLock()
	defer s.m.RUnlock()
	if err := s.txn.Delete([]byte(key)); err != badger.ErrTxnTooBig {
		// return if err == nil or err != txnTooBig
		return err
	}
	// transaction is too big, we need to commit it and create a new one
	if err := s.commit(); err != nil {
		return err
	}
	return s.txn.Delete([]byte(key))
}

func (s *Storage) MarkRecovered() error {
	atomic.AddInt64(&s.recovered, 1)
	return nil
}

func (s *Storage) Recovered() bool {
	return atomic.LoadInt64(&s.recovered) == 1
}

func (s *Storage) Open() error {
	return nil
}

func (s *Storage) commit() error {
	s.m.RUnlock()
	defer s.m.RLock()
	s.m.Lock()
	defer s.m.Unlock()

	err := s.txn.Commit(nil)
	if err != nil {
		return err
	}
	s.txn = s.db.NewTransaction(true)
	return s.gc()
}

func (s *Storage) gc() error {
	if !s.Recovered() {
		return nil
	}
	if atomic.LoadInt64(&s.txns) < s.maxTxns {
		return nil
	}
	s.txns = 0

	if err := s.db.PurgeOlderVersions(); err != nil {
		return err
	}
	if err := s.db.RunValueLogGC(defaultDiscardRatio); err != badger.ErrNoRewrite {
		return err
	}
	return nil
}
