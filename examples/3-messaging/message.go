package messaging

import (
	"encoding/json"
	"io"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	SentStream     goka.Stream = "message_sent"
	ReceivedStream goka.Stream = "message_received"
)

type Message struct {
	From    string
	To      string
	Content string
}

type MessageCodec struct{}

func (c *MessageCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *MessageCodec) Decode(data []byte) (interface{}, error) {
	var m Message
	return &m, json.Unmarshal(data, &m)
}

func (c *MessageCodec) DecodeP(data []byte) (interface{}, io.Closer, error) {
	dec, err := c.Decode(data)
	return dec, codec.NoopCloser, err
}

type MessageListCodec struct{}

func (c *MessageListCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *MessageListCodec) Decode(data []byte) (interface{}, error) {
	var m []Message
	err := json.Unmarshal(data, &m)
	return m, err
}

func (c *MessageListCodec) DecodeP(data []byte) (interface{}, io.Closer, error) {
	dec, err := c.Decode(data)
	return dec, codec.NoopCloser, err
}
