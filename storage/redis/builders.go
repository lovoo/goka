package redis

import (
	"errors"
	"fmt"

	"github.com/lovoo/goka/storage"

	redis "gopkg.in/redis.v5"
)

// RedisBuilder builds redis storage.
func RedisBuilder(client *redis.Client, namespace string) storage.Builder {
	return func(topic string, partition int32) (storage.Storage, error) {
		if namespace == "" {
			return nil, errors.New("missing namespace to redis storage")
		}
		return New(client, fmt.Sprintf("%s:%s:%d", namespace, topic, partition))
	}
}
