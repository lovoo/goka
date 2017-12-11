package multierr

import (
	"fmt"
	"sync"
)

// Errors represent a list of errors triggered during the execution of a goka view/processor.
// Normally, the first error leads to stopping the processor/view, but during shutdown, more errors
// might occur.
type Errors struct {
	errs []error
	m    sync.Mutex
}

func (e *Errors) Collect(err error) {
	e.m.Lock()
	e.errs = append(e.errs, err)
	e.m.Unlock()
}

func (e *Errors) HasErrors() bool {
	return len(e.errs) > 0
}

func (e *Errors) Error() string {
	str := "Errors:\n"
	for _, err := range e.errs {
		str += fmt.Sprintf("\t%s\n", err.Error())
	}
	return str
}

func (e *Errors) NilOrError() error {
	if e.HasErrors() {
		return e
	}
	return nil
}
