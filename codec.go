package goka

import (
	"io"

	"github.com/lovoo/goka/codec"
)

// Codec decodes and encodes from and to []byte
type Codec interface {
	Encode(value interface{}) (data []byte, err error)
	Decode(data []byte) (value interface{}, err error)
}

type CodecP interface {
	Codec

	DecodeP(data []byte) (value interface{}, closer io.Closer, err error)
}

// CloserFunc implements io.Closer-interface for convenience when wrapping functions
type CloserFunc func() error

func (f CloserFunc) Close() error {
	return f()
}

type codecWrapper struct {
	Codec
}

func (cw *codecWrapper) DecodeP(data []byte) (value interface{}, closer io.Closer, err error) {
	val, err := cw.Codec.Decode(data)
	return val, codec.NoopCloser, err
}

func convertOrFakeCodec(c Codec) CodecP {
	if cp, ok := c.(CodecP); ok {
		return cp
	}
	return &codecWrapper{Codec: c}
}
