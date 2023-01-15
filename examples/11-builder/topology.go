package main

import (
	"encoding/json"

	"github.com/lovoo/goka/typed"
)

// user defined
type (
	UserID    string
	PhotoID   string
	PhotoHash string

	PhotoUploaded struct {
		User UserID
		Id   string
	}

	PhotoHashed struct {
		Id   string
		Hash string
	}

	PhotoCheck struct {
		User UserID
	}

	Profile struct {
		User   string
		Photos []string
		Hashes []string
	}

	ProfileHashesOnly struct {
		User   string
		Hashes []string
	}
)

type JsonCodec[T any] struct{}

func (jc *JsonCodec[T]) Encode(value T) (data []byte, err error) {
	return json.Marshal(value)
}

func (jc *JsonCodec[T]) Decode(data []byte) (T, error) {
	var value T
	return value, json.Unmarshal(data, &value)
}

// current questions:
// how to initialize the topo fields with all codecs, without specifying everything else.

// step 1: define it
// go:generate goka-topology -type topology
type hasher struct {
	PhotoUploaded typed.GInput[UserID, *PhotoUploaded]
	PhotoHashed   typed.GOutput[PhotoID, *PhotoHashed]
}

type hash_storer struct {
	PhotoHashed typed.GInput[PhotoID, *PhotoHashed]
	State       typed.GState[PhotoID, *PhotoHashed]
}

type user_collector struct {
	PhotoCheck typed.GInput[UserID, *PhotoCheck]
	PhotoHash  typed.GLookup[PhotoID, *PhotoHashed]
	Profile    typed.GState[UserID, *Profile]
}

// for the issue:
// Why not the declarative DSL like kafka streams?
// Things like conditional lookups/emits become very hard to write
// one would have to define branch/if/else constructs to do so simple things like
// calling the context.
// Sometimes you want to lookup from different tables depending on each others values.
// Expressing this becomes very complicated and we'd have to create factory functions
// for different number of types like Join2[A, B] and Join3[A, B, C] etc. which feels unintuitive.
