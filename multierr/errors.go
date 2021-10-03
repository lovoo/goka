package multierr

import (
	"fmt"
	"sync"
)

// Errors represent a list of errors triggered during the execution of a goka view/processor.
// Normally, the first error leads to stopping the processor/view, but during shutdown, more errors
// might occur.
// DEPRECATED. This will be removed one day, we migrated to the implementation in
// github.com/hashicorp/go-multierror
type Errors struct {
	errs []error
	m    sync.Mutex
}

func (e *Errors) Collect(err error) *Errors {
	if err == nil {
		return e
	}
	e.m.Lock()
	e.errs = append(e.errs, err)
	e.m.Unlock()
	return e
}

func (e *Errors) Merge(o *Errors) *Errors {
	if o == nil {
		return e
	}

	// lock base
	e.m.Lock()
	defer e.m.Unlock()
	// lock other
	o.m.Lock()
	defer o.m.Unlock()

	e.errs = append(e.errs, o.errs...)
	return e
}

func (e *Errors) HasErrors() bool {
	return len(e.errs) > 0
}

func (e *Errors) Error() string {
	if !e.HasErrors() {
		return ""
	}
	if len(e.errs) == 1 {
		return e.errs[0].Error()
	}
	str := "Errors:\n"
	for _, err := range e.errs {
		str += fmt.Sprintf("\t* %s\n", err.Error())
	}
	return str
}

func (e *Errors) NilOrError() error {
	if e.HasErrors() {
		return e
	}
	return nil
}
