package storage

import (
	"fmt"
	"path/filepath"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// Builder creates a local storage (a persistent cache) for a topic
// table. Builder creates one storage for each partition of the topic.
type Builder func(topic string, partition int32) (Storage, error)

// DefaultBuilder builds a LevelDB storage with default configuration.
// The database will be stored in the given path.
func DefaultBuilder(path string) Builder {
	return func(topic string, partition int32) (Storage, error) {
		fp := filepath.Join(path, fmt.Sprintf("%s.%d", topic, partition))
		db, err := leveldb.OpenFile(fp, nil)
		if err != nil {
			return nil, fmt.Errorf("error opening leveldb: %v", err)
		}
		return New(db)
	}
}

// BuilderWithOptions builds LevelDB storage with the given options and
// in the given path.
func BuilderWithOptions(path string, opts *opt.Options) Builder {
	return func(topic string, partition int32) (Storage, error) {
		fp := filepath.Join(path, fmt.Sprintf("%s.%d", topic, partition))
		db, err := leveldb.OpenFile(fp, opts)
		if err != nil {
			return nil, fmt.Errorf("error opening leveldb: %v", err)
		}
		return New(db)
	}
}

// MemoryBuilder builds in-memory storage.
func MemoryBuilder() Builder {
	return func(topic string, partition int32) (Storage, error) {
		return NewMemory(), nil
	}
}
