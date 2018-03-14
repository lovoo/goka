package goka

import (
	"errors"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestOnce_Do(t *testing.T) {
	var o once

	err := o.Do(func() error { return errors.New("some error") })
	ensure.NotNil(t, err)

	err2 := o.Do(func() error { return nil })
	ensure.NotNil(t, err2)
	ensure.DeepEqual(t, err, err2)
}
