package bolt

import (
	"errors"
	"fmt"

	"github.com/lovoo/goka/storage"

	"github.com/boltdb/bolt"
)

// Bolt Builder builds bolt storage.
func BoltBuilder(client *bolt.DB, namespace string) storage.Builder {
	return func(topic string, partition int32) (storage.Storage, error) {
		if namespace == "" {
			return nil, errors.New("missing namespace to bolt storage")
		}
		return New(client, fmt.Sprintf("%s:%s:%d", namespace, topic, partition))
	}
}
