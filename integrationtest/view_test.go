package integrationtest

import (
	"context"
	"testing"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/internal/test"
	"github.com/lovoo/goka/tester"
)

func TestView(t *testing.T) {
	t.Run("uninitialized_get", func(t *testing.T) {
		gkt := tester.New(t)

		// create an empty view on table "test"
		view, err := goka.NewView(nil, "test", new(codec.String), goka.WithViewTester(gkt))
		test.AssertNil(t, err)

		// try to get a value
		val, err := view.Get("key")
		// --> must fail, because the view is not running yet
		test.AssertNotNil(t, err)
		test.AssertNil(t, val)

		// start the view
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			if err := view.Run(ctx); err != nil {
				panic(err)
			}
		}()

		// set some value (this will wait internally until the view is running)
		gkt.SetTableValue("test", "key", "value")

		// try to get some non-existent key
		val, err = view.Get("not-existent")
		test.AssertNil(t, val)
		test.AssertNil(t, nil)

		// get the value we set earlier
		val, err = view.Get("key")
		test.AssertEqual(t, val.(string), "value")
		test.AssertNil(t, nil)

		// stop the view and wait for it to finish up
		cancel()
		<-done
	})

}
