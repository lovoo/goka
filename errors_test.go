package goka

import (
	"errors"
	"fmt"
	"testing"

	"github.com/facebookgo/ensure"

	"github.com/lovoo/goka/codec"
)

func consume(ctx Context, msg interface{}) {
	fmt.Println("received", msg)
}

func TestError(t *testing.T) {
	var err error
	km := NewKafkaMock(t, "group")
	proc, err := NewProcessor([]string{"broker"},
		DefineGroup("group",
			Input("upstream", new(codec.String), consume),
			Persist(c),
		),
		km.ProcessorOptions()...,
	)
	ensure.Nil(t, err)
	done := make(chan struct{})
	go func() {
		defer func() {
			close(done)
		}()
		err = proc.Start()
	}()

	km.consumeData("upstream", "test", []byte("test"))
	km.consumeData("upstream", "test", []byte("test"))
	// consume an error
	km.consumeError(errors.New("hallo welt"))
	<-done
	ensure.NotNil(t, err)
}
