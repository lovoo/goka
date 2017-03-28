package kafka

import (
	"errors"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestPromise_thenBeforeFinish(t *testing.T) {
	p := new(Promise)

	var promiseErr error
	p.Then(func(err error) {
		promiseErr = err
	})

	p.Finish(errors.New("test"))

	ensure.DeepEqual(t, promiseErr.Error(), "test")

	// repeating finish won't change result
	p.Finish(errors.New("test-whatever"))

	ensure.DeepEqual(t, promiseErr.Error(), "test")
}

func TestPromise_thenAfterFinish(t *testing.T) {
	p := new(Promise)

	var promiseErr error
	p.Finish(errors.New("test"))
	p.Then(func(err error) {
		promiseErr = err
	})

	ensure.DeepEqual(t, promiseErr.Error(), "test")
}
