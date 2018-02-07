package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/lovoo/goka/examples/3-messaging/blocker"
	"github.com/lovoo/goka/examples/3-messaging/collector"
	"github.com/lovoo/goka/examples/3-messaging/detector"
	"github.com/lovoo/goka/examples/3-messaging/filter"
	"github.com/lovoo/goka/examples/3-messaging/translator"
)

var (
	brokers       = []string{"localhost:9092"}
	runFilter     = flag.Bool("filter", false, "run filter processor")
	runCollector  = flag.Bool("collector", false, "run collector processor")
	runTranslator = flag.Bool("translator", false, "run translator processor")
	runBlocker    = flag.Bool("blocker", false, "run blocker processor")
	runDetector   = flag.Bool("detector", false, "run detector processor")
	broker        = flag.String("broker", "localhost:9092", "boostrap Kafka broker")
)

func main() {
	flag.Parse()
	if *runCollector {
		log.Println("starting collector")
		go collector.Run(brokers)
	}
	if *runFilter {
		log.Println("starting filter")
		go filter.Run(brokers)
	}
	if *runBlocker {
		log.Println("starting blocker")
		go blocker.Run(brokers)
	}
	if *runDetector {
		log.Println("starting detector")
		go detector.Run(brokers)
	}
	if *runTranslator {
		log.Println("starting translator")
		go translator.Run(brokers)
	}

	// Wait for SIGINT/SIGTERM
	waiter := make(chan os.Signal, 1)
	signal.Notify(waiter, syscall.SIGINT, syscall.SIGTERM)

	select {
	case signal := <-waiter:
		log.Printf("Got interrupted by %v", signal)
	}
}
