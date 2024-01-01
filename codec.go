package goka

import "io"

// Codec decodes and encodes from and to []byte
type Codec interface {
	Encode(value interface{}) (data []byte, err error)
	Decode(data []byte) (value interface{}, err error)
	DecodeP(data []byte) (value interface{}, closer io.Closer, err error)
}

// FuncCloser implements io.Closer-interface for convenience
type FuncCloser func() error

func (f FuncCloser) Close() error {
	return f()
}
