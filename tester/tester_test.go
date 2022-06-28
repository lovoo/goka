package tester

import (
	"testing"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/stretchr/testify/require"
)

type failingT struct {
	failed bool
}

func (ft *failingT) Errorf(format string, args ...interface{}) {
	ft.failed = true
}

func (ft *failingT) Fatalf(format string, args ...interface{}) {
	ft.failed = true
}

func (ft *failingT) Fatal(a ...interface{}) {
	ft.failed = true
}

func TestTester(t *testing.T) {
	var ft failingT
	gkt := New(&ft)

	goka.NewProcessor(nil,
		goka.DefineGroup("", goka.Input("input", new(codec.Int64), func(ctx goka.Context, msg interface{}) {})),
		goka.WithTester(gkt))

	// make sure that an invalid group graph fails the test
	require.True(t, ft.failed)
}
