package goka

import (
	"fmt"
	"testing"

	"github.com/facebookgo/ensure"
)

func newMockOptions(t *testing.T) *poptions {
	opts := new(poptions)
	err := opts.applyOptions("")
	ensure.Nil(t, err)
	opts.storagePath = "/tmp/goka-test"

	fmt.Printf("%+v\n", opts)
	return opts
}

func TestOptions_storagePathForPartition(t *testing.T) {
	topic := "test"
	var id int32
	opts := newMockOptions(t)
	path := opts.storagePathForPartition(topic, id)
	ensure.DeepEqual(t, path, fmt.Sprintf("/tmp/goka-test/processor/%s.%d", topic, id))
}
