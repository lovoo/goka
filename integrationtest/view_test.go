package integrationtest

import (
	"context"
	"testing"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/tester"
	"github.com/stretchr/testify/require"
)

func TestView(t *testing.T) {
	t.Run("uninitialized_get", func(t *testing.T) {
		gkt := tester.New(t)

		// create an empty view on table "test"
		view, err := goka.NewView(nil, "test", new(codec.String), goka.WithViewTester(gkt))
		require.NoError(t, err)

		// try to get a value
		val, err := view.Get("key")
		// --> must fail, because the view is not running yet
		require.Error(t, err)
		require.Nil(t, val)

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
		require.NoError(t, err)
		require.Nil(t, val)
		require.NoError(t, nil)

		// get the value we set earlier
		val, err = view.Get("key")
		require.NoError(t, err)
		require.Equal(t, "value", val.(string))
		require.NoError(t, nil)

		// get all the keys from table "test"
		keys := gkt.GetTableKeys("test")
		// at the moment we only have one key "key"
		require.Equal(t, []string{"key"}, keys)

		// set a second key
		gkt.SetTableValue("test", "key2", "value")
		keys = gkt.GetTableKeys("test")
		require.Equal(t, []string{"key", "key2"}, keys)

		// stop the view and wait for it to finish up
		cancel()
		<-done
	})
}
