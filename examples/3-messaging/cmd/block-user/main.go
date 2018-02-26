package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/examples/3-messaging/blocker"
)

var (
	user    = flag.String("user", "", "user to block")
	unblock = flag.Bool("unblock", false, "unblock user instead of blocking")
	broker  = flag.String("broker", "localhost:9092", "boostrap Kafka broker")
)

func main() {
	flag.Parse()
	if *user == "" {
		fmt.Println("cannot block user ''")
		os.Exit(1)
	}
	emitter, err := goka.NewEmitter([]string{*broker}, blocker.Stream, new(blocker.BlockEventCodec))
	if err != nil {
		panic(err)
	}
	defer emitter.Finish()

	err = emitter.EmitSync(*user, &blocker.BlockEvent{Unblock: *unblock})
	if err != nil {
		panic(err)
	}
}
