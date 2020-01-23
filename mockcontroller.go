package goka

import (
	"fmt"

	"github.com/golang/mock/gomock"
)

type gomockPanicker struct {
	reporter gomock.TestReporter
}

func (gm *gomockPanicker) Errorf(format string, args ...interface{}) {
	gm.reporter.Errorf(format, args...)
}
func (gm *gomockPanicker) Fatalf(format string, args ...interface{}) {
	defer panic(fmt.Sprintf(format, args...))
	gm.reporter.Fatalf(format, args...)
}

// NewMockController returns a *gomock.Controller using a wrapped testing.T (or whatever)
// which panics on a Fatalf. This is necessary when using a mock in kafkamock.
// Otherwise it will freeze on an unexpected call.
func NewMockController(t gomock.TestReporter) *gomock.Controller {
	return gomock.NewController(&gomockPanicker{reporter: t})
}
