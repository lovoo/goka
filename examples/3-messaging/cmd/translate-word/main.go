package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/examples/3-messaging/translator"
)

var (
	word   = flag.String("word", "", "word to translate")
	with   = flag.String("with", "", "word translation")
	broker = flag.String("broker", "localhost:9092", "boostrap Kafka broker")
)

func main() {
	flag.Parse()
	if *word == "" {
		fmt.Println("cannot translate word ''")
		os.Exit(1)
	}
	emitter, err := goka.NewEmitter([]string{*broker}, translator.Stream, new(codec.String))
	if err != nil {
		panic(err)
	}
	defer emitter.Finish()

	err = emitter.EmitSync(*word, *with)
	if err != nil {
		panic(err)
	}
}
