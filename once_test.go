package goka

import (
	"errors"
	"testing"

	"github.com/lovoo/goka/internal/test"
)

func TestOnce_Do(t *testing.T) {
	var o once

	err := o.Do(func() error { return errors.New("some error") })
	test.AssertNotNil(t, err)

	err2 := o.Do(func() error { return nil })
	test.AssertNotNil(t, err2)
	test.AssertEqual(t, err, err2)
}
