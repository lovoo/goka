package main

import (
	"context"
	"log"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/typed"
)

func main() {
	graph, err := NewHash_storerProcessorBuilder("group").
		WithPersistState(new(typed.StringCodec[PhotoID]), new(JsonCodec[*PhotoHashed])).
		HandlePhotoHashed("photo_uploaded", new(typed.StringCodec[PhotoID]), new(JsonCodec[*PhotoHashed]),
			func(ctx *hash_storerContext, key PhotoID, msg *PhotoHashed) {
				ctx.SetValue(msg)
			}, typed.AutoCreate(20)).Build()
	if err != nil {
		log.Fatalf("error creating processor: %v", err)
	}

	proc, err := goka.NewProcessor([]string{"localhost:9092"}, graph)
	if err != nil {
		log.Fatalf("error creating goka processor: %v", err)
	}

	proc.Run(context.Background())
}
