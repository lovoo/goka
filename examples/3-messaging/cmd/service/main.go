package main

import (
	"flag"

	"github.com/lovoo/goka/examples/3-messaging"
	"github.com/lovoo/goka/examples/3-messaging/service"
)

var (
	sent   = flag.Bool("sent", false, "emit to SentStream")
	broker = flag.String("broker", "localhost:9092", "boostrap Kafka broker")
)

func main() {
	flag.Parse()
	if *sent {
		service.Run([]string{*broker}, messaging.SentStream)
	} else {
		service.Run([]string{*broker}, messaging.ReceivedStream)
	}
}
