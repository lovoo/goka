package goka

import (
	"errors"
	"fmt"
	"testing"

	"github.com/facebookgo/ensure"

	"stash.lvint.de/lab/goka/codec"
)

func consume(ctx Context, msg interface{}) {
	fmt.Println("received", msg)
}

func TestError(t *testing.T) {
	km := NewKafkaMock(t, "group")
	proc, err := NewProcessor([]string{"broker"},
		"group",
		Stream("upstream", new(codec.String), consume),
		km.ProcessorOptions()...,
	)
	ensure.Nil(t, err)
	go func() {
		err := proc.Start()
		ensure.NotNil(t, err)
	}()

	for i := 0; i < 2; i++ {
		km.consumeData("upstream", "test", []byte("test"))
	}

	km.consumeError(errors.New("hallo welt"))

	doTimed(t, proc.Stop)
}
