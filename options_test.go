package goka

import (
	"fmt"
	"testing"

	"github.com/lovoo/goka/storage"
	"github.com/stretchr/testify/require"
)

func nullStorageBuilder() storage.Builder {
	return func(topic string, partition int32) (storage.Storage, error) {
		return &storage.Null{}, nil
	}
}

func newMockOptions(t *testing.T) *poptions {
	opts := new(poptions)
	err := opts.applyOptions(new(GroupGraph))
	require.Error(t, err)

	opts.builders.storage = nullStorageBuilder()
	err = opts.applyOptions(new(GroupGraph))
	require.NoError(t, err)

	fmt.Printf("%+v\n", opts)
	return opts
}
