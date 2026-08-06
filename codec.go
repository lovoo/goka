package goka

// Codec decodes and encodes from and to []byte
type Codec interface {
	Encode(value any) (data []byte, err error)
	Decode(data []byte) (value any, err error)
}
