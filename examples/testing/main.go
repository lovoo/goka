package main

import (
	"fmt"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

func ConsumeScalar(ctx goka.Context, msg interface{}) {
	scalar, is := msg.(int64)
	if !is {
		ctx.Fail(fmt.Errorf("Invalid message type. expected int64, was %T", msg))
	} else {
		ctx.Emit("sink", "outgoing", int64(scalar+1))
	}
}

func ConsumeScalarState(ctx goka.Context, msg interface{}) {
	scalar, is := msg.(int64)

	if !is {
		ctx.Fail(fmt.Errorf("Invalid message type. expected int64, was %T", msg))
	} else {
		var value int64
		rawValue := ctx.Value()
		if rawValue != nil {
			value = rawValue.(int64)
		}
		value += scalar
		ctx.SetValue(value)
	}
}

func createProcessor(brokers []string, extraopts ...goka.ProcessorOption) (*goka.Processor, error) {
	return goka.NewProcessor(brokers,
		goka.DefineGroup(
			goka.Group("consume-scalar"),
			goka.Persist(new(codec.Int64)),
			goka.Input(goka.Stream("scalar"), new(codec.Int64), ConsumeScalarState),
		),
		extraopts...,
	)
}

func main() {
	proc, err := createProcessor([]string{"localhost:9092"})
	if err != nil {
		panic(err)
	}

	errs := proc.Start()
	if errs != nil {
		fmt.Printf("Error executing processor: %v", errs)
	}
}
