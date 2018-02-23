package storage

import (
	"fmt"
	"path/filepath"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// DefaultStorageBuilder builds a LevelDB storage with default configuration.
// The database will be stored in the given path.
func DefaultStorageBuilder(path string) func(topic string, partition int32) (Storage, error) {
	return func(topic string, partition int32) (Storage, error) {
		fp := filepath.Join(path, fmt.Sprintf("%s.%d", topic, partition))
		db, err := leveldb.OpenFile(fp, nil)
		if err != nil {
			return nil, fmt.Errorf("error opening leveldb: %v", err)
		}
		return New(db)
	}
}

// StorageBuilderWithOptions builds LevelDB storage with the given options and
// in the given path.
func StorageBuilderWithOptions(path string, opts *opt.Options) func(topic string, partition int32) (Storage, error) {
	return func(topic string, partition int32) (Storage, error) {
		fp := filepath.Join(path, fmt.Sprintf("%s.%d", topic, partition))
		db, err := leveldb.OpenFile(fp, nil)
		if err != nil {
			return nil, fmt.Errorf("error opening leveldb: %v", err)
		}
		return New(db)
	}
}
