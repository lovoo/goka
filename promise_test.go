package goka

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPromise_thenBeforeFinish(t *testing.T) {
	p := new(Promise)

	var promiseErr error
	p.Then(func(err error) {
		promiseErr = err
	})

	p.finish(nil, errors.New("test"))

	require.Equal(t, "test", promiseErr.Error())

	// repeating finish won't change result
	p.finish(nil, errors.New("test-whatever"))

	require.Equal(t, "test", promiseErr.Error())
}

func TestPromise_thenAfterFinish(t *testing.T) {
	p := new(Promise)

	var promiseErr error
	p.finish(nil, errors.New("test"))
	p.Then(func(err error) {
		promiseErr = err
	})

	require.Equal(t, "test", promiseErr.Error())
}
