package multierr

import (
	"context"
	"sync"
)

type MultiWait struct {
	ctx context.Context
	wg  sync.WaitGroup
}

func NewMultiWait(ctx context.Context, num int) *MultiWait {
	mw := &MultiWait{
		ctx: ctx,
	}
	mw.wg.Add(num)

	return mw
}

func (mw *MultiWait) Add(done <-chan struct{}) {
	go func() {
		select {
		case <-mw.ctx.Done():
		case <-done:
			mw.wg.Done()
		}
	}()
}

func (mw *MultiWait) Done() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		mw.wg.Wait()
	}()
	return done
}

func (mw *MultiWait) Wait() bool {
	select {
	case <-mw.Done():
		return true
	case <-mw.ctx.Done():
		return false
	}
}
