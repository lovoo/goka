package goka

import (
	"errors"
	"testing"

	"github.com/lovoo/goka/internal/test"
)

func TestPromise_thenBeforeFinish(t *testing.T) {
	p := new(Promise)

	var promiseErr error
	p.Then(func(err error) {
		promiseErr = err
	})

	p.Finish(errors.New("test"))

	test.AssertEqual(t, promiseErr.Error(), "test")

	// repeating finish won't change result
	p.Finish(errors.New("test-whatever"))

	test.AssertEqual(t, promiseErr.Error(), "test")
}

func TestPromise_thenAfterFinish(t *testing.T) {
	p := new(Promise)

	var promiseErr error
	p.Finish(errors.New("test"))
	p.Then(func(err error) {
		promiseErr = err
	})

	test.AssertEqual(t, promiseErr.Error(), "test")
}
