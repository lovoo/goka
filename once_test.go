package goka

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOnce_Do(t *testing.T) {
	var o once

	err := o.Do(func() error { return errors.New("some error") })
	require.Error(t, err)

	err2 := o.Do(func() error { return nil })
	require.Error(t, err2)
	require.Equal(t, err, err2)
}
