package main

import (
	"fmt"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	brokers             = []string{"localhost:9092"}
	topic   goka.Stream = "mini-input"
	group   goka.Group  = "mini-group"
)

// emitter
func runEmitter() {
	emitter, err := goka.NewEmitter(brokers, topic,
		new(codec.String))
	if err != nil {
		panic(err)
	}
	defer emitter.Finish()

	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	i := 0
	for range t.C {
		key := fmt.Sprintf("%d", i%10)
		value := fmt.Sprintf("%s", time.Now())
		emitter.EmitSync(key, value)
		i++
	}
}

// processor
func process(ctx goka.Context, msg interface{}) {
	var counter int64
	if val := ctx.Value(); val != nil {
		counter = val.(int64)
	}
	counter++
	ctx.SetValue(counter)

	fmt.Println("[proc] key:", ctx.Key(),
		"count:", counter, "msg:", msg)
}

func runProcessor() {
	g := goka.DefineGroup(group,
		goka.Input(topic, new(codec.String), process),
		goka.Persist(new(codec.Int64)),
	)
	if p, err := goka.NewProcessor(brokers, g); err != nil {
		panic(err)
	} else if err = p.Start(); err != nil {
		panic(err)
	}
}

// view
func runView() {
	view, err := goka.NewView(brokers,
		goka.GroupTable(group),
		new(codec.Int64),
	)
	if err != nil {
		panic(err)
	}
	go view.Start()
	defer view.Stop()

	t := time.NewTicker(10 * time.Second)
	defer t.Stop()

	for range t.C {
		for i := 0; i < 10; i++ {
			val, _ := view.Get(fmt.Sprintf("%d", i))
			fmt.Printf("[view] %d: %v\n", i, val)
		}
	}
}

func main() {
	go runEmitter()
	go runProcessor()
	runView()
}
