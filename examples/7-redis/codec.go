package main

import "encoding/json"

type Codec struct{}

// Encode encodes a event struct into an array.
func (c *Codec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

// Decode decodes a event from byte encoded array.
func (c *Codec) Decode(data []byte) (interface{}, error) {
	event := new(Event)

	err := json.Unmarshal(data, event)
	return event, err
}
