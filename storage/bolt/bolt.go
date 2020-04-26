package bolt

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/boltdb/bolt"

	"github.com/lovoo/goka/storage"
)

const (
	offsetKey = "__offset"
)

type boltStorage struct {
	client     *bolt.DB
	bucketName []byte
}

// New creates a new Storage backed by Redis.
func New(client *bolt.DB, bucket string) (storage.Storage, error) {
	if client == nil {
		return nil, errors.New("invalid bolt db")
	}

	tx, err := client.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Use the transaction...
	tx.CreateBucket([]byte(bucket))
	if err != nil {
		return nil, err
	}

	// Commit the transaction and check for error.
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &boltStorage{
		client:     client,
		bucketName: []byte(bucket),
	}, nil
}

func (b *boltStorage) Has(key string) (bool, error) {
	hasKey := false
	err := b.client.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(b.bucketName)
		k := []byte(key)
		value := b.Get(k)
		if value != nil {
			hasKey = true
		}
		return nil
	})

	return hasKey, err
}

func (b *boltStorage) Get(key string) ([]byte, error) {
	var value []byte
	err := b.client.View(func(tx *bolt.Tx) error {
		var err error
		buck := tx.Bucket(b.bucketName)
		k := []byte(key)
		value = buck.Get(k)
		if err != nil {
			return err
		}
		return nil
	})

	return value, err
}

func (b *boltStorage) GetOffset(defValue int64) (int64, error) {
	data, err := b.Get(offsetKey)
	if err != nil {
		return 0, err
	}
	if data == nil {
		return defValue, nil
	}

	value, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error decoding bolt offset (%s): %v", string(data), err)
	}
	return value, nil
}

func (b *boltStorage) Set(key string, value []byte) error {
	err := b.client.Update(func(tx *bolt.Tx) error {
		people, err := tx.CreateBucketIfNotExists(b.bucketName)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}

		err = people.Put([]byte(key), value)
		return err
	})
	return err
}

func (b *boltStorage) SetOffset(offset int64) error {
	return b.Set(offsetKey, []byte(strconv.FormatInt(offset, 10)))
}

func (b *boltStorage) Delete(key string) error {
	err := b.client.Update(func(tx *bolt.Tx) error {
		people, err := tx.CreateBucketIfNotExists(b.bucketName)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}

		err = people.Delete([]byte(key))
		return err
	})
	return err
}

// GetAllKeys get all keys from the target bucket
func (b *boltStorage) getAllKeys(pattern string) []string {
	var keys []string

	b.client.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(b.bucketName)
		b.ForEach(func(k, v []byte) error {
			// Due to
			// Byte slices returned from Bolt are only valid during a transaction. Once the transaction has been committed or rolled back then the memory they point to can be reused by a new page or can be unmapped from virtual memory and you'll see an unexpected fault address panic when accessing it.
			// We copy the slice to retain it
			dst := make([]byte, len(k))
			copy(dst, k)

			keys = append(keys, string(dst))
			return nil
		})
		return nil
	})

	return keys
}

func (b *boltStorage) Iterator() (storage.Iterator, error) {
	keys := b.getAllKeys("")
	return &boltIterator{
		current: 0,
		keys:    keys,
		client:  b,
	}, nil
}

func (b *boltStorage) IteratorWithRange(start, limit []byte) (storage.Iterator, error) {
	allKeys := b.getAllKeys("")
	keys := make([]string, 0)
	for _, k := range allKeys {
		if bytes.Compare([]byte(k), start) > -1 && bytes.Compare([]byte(k), limit) < 1 {
			keys = append(keys, k)
		}
	}
	return &boltIterator{
		current: 0,
		keys:    keys,
		client:  b,
	}, nil
}

func (b *boltStorage) Recovered() bool {
	return false
}

func (b *boltStorage) MarkRecovered() error {
	return nil
}

func (b *boltStorage) Open() error {
	return nil
}

func (b *boltStorage) Close() error {
	return nil
}

type boltIterator struct {
	current uint64
	keys    []string
	client  *boltStorage
	hash    string
}

func (i *boltIterator) exhausted() bool {
	return uint64(len(i.keys)) <= i.current
}

func (i *boltIterator) Next() bool {
	i.current++
	if string(i.Key()) == offsetKey {
		i.current++
	}
	return !i.exhausted()
}

func (i *boltIterator) Key() []byte {
	if i.exhausted() {
		return nil
	}
	key := i.keys[i.current]
	return []byte(key)
}

func (i *boltIterator) Err() error {
	return nil
}

func (i *boltIterator) Value() ([]byte, error) {
	if i.exhausted() {
		return nil, nil
	}
	key := i.keys[i.current]
	return i.client.Get(key)
}

func (i *boltIterator) Release() {
	i.current = uint64(len(i.keys))
}

func (i *boltIterator) Seek(key []byte) bool {
	return !i.exhausted()
}
