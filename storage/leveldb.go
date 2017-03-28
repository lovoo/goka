package storage

import (
	"fmt"
	"log"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

const (
	offsetKey                  = "__offset"
	defaultStorageMaxBatchSize = 20000
)

type levelDbStorage struct {
	db *leveldb.DB

	batched bool
	batch   chan batchEntry
	dying   chan bool
	done    chan bool
}

type batchOp int

const (
	opPut = iota
	opDelete
)

type batchEntry struct {
	op    batchOp
	key   []byte
	value []byte
}

// create a new storage object backed by leveldb
func newLeveldbStorage(fn string, batched bool) (*levelDbStorage, error) {
	db, err := leveldb.OpenFile(fn, nil)
	if err != nil {
		return nil, err
	}

	s := &levelDbStorage{
		db:      db,
		batched: batched,
		batch:   make(chan batchEntry, defaultStorageMaxBatchSize),
		dying:   make(chan bool, 1),
		done:    make(chan bool, 1),
	}

	if batched {
		go s.writer()
	}
	return s, nil
}

func (s *levelDbStorage) Iterator() iterator.Iterator {
	return s.db.NewIterator(nil, nil)
}

func (s *levelDbStorage) Has(key string) (bool, error) {
	return s.db.Has([]byte(key), nil)
}

// TODO(diogo) with batches, Get may not see latest Set. We can use a dirty key
// table to fix that.
func (s *levelDbStorage) Get(key string) ([]byte, error) {
	has, err := s.db.Has([]byte(key), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read from leveldb (key %s): %v", key, err)
	}

	if !has {
		return nil, nil
	}

	value, err := s.db.Get([]byte(key), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read from leveldb (key %s): %v", key, err)
	}
	return value, nil
}

func (s *levelDbStorage) Set(key string, data []byte) error {
	// set key in leveldb
	if !s.batched {
		err := s.db.Put([]byte(key), data, nil)
		if err != nil {
			return fmt.Errorf("failed to write to leveldb (key %s): %v", key, err)
		}
		return nil
	}
	s.batch <- batchEntry{op: opPut, key: []byte(key), value: data}

	return nil
}

func (s *levelDbStorage) Delete(key string) error {
	if s.batched {
		s.batch <- batchEntry{op: opDelete, key: []byte(key)}
		return nil
	}

	if err := s.db.Delete([]byte(key), nil); err != nil {
		return fmt.Errorf("error deleting from leveldb (key %s): %v", key, err)
	}

	return nil
}

func (s *levelDbStorage) writer() error {
	batch := &leveldb.Batch{}

	doWrite := func() error {
		if batch.Len() == 0 {
			return nil
		}
		// add messages from the batch channel into the batch
		done := false
		for !done {
			select {
			case e := <-s.batch:
				switch e.op {
				case opPut:
					batch.Put(e.key, e.value)
				case opDelete:
					batch.Delete(e.key)
				default:
					// TODO (franz): sure we want a panic? Not returning the error?
					log.Panicf("unknown batch operation in doWrite: %v", e.op)
				}

				if batch.Len() >= cap(s.batch) {
					done = true
				}
			default:
				// according to the docu, default only runs if the case was not ready
				done = true
			}
		}

		// write batch
		err := s.db.Write(batch, nil)
		if err != nil {
			return fmt.Errorf("failed to write batch to leveldb: %v", err)
		}
		batch.Reset()
		return nil
	}

	for {
		select {
		// wait until one or more messages available
		case e := <-s.batch:
			switch e.op {
			case opPut:
				batch.Put(e.key, e.value)
			case opDelete:
				batch.Delete(e.key)
			default:
				// TODO (franz): sure we want a panic? Not returning the error?
				log.Panicf("unknown batch operation in writer: %v", e.op)
			}

			// if a message is available, put it into the batch. In doWrite we
			// try to dequeue all remainder messages in the channel.
			err := doWrite()
			if err != nil {
				return err
			}

		case <-s.dying:
			err := doWrite()
			s.done <- true
			return err
		}
	}
}

func (s *levelDbStorage) Close() error {
	if s.batched {
		close(s.dying)
		<-s.done
	}
	return s.db.Close()
}
