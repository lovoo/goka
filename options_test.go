package goka

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/facebookgo/ensure"
)

func newMockOptions(t *testing.T) *poptions {
	opts := new(poptions)
	err := opts.applyOptions(new(GroupGraph))
	ensure.Err(t, err, regexp.MustCompile("StorageBuilder not set$"))

	opts.builders.storage = nullStorageBuilder()
	err = opts.applyOptions(new(GroupGraph))
	ensure.Nil(t, err)

	fmt.Printf("%+v\n", opts)
	return opts
}
