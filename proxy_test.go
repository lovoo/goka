package goka

import (
	"bytes"
	"github.com/Shopify/sarama"
	"github.com/lovoo/goka/storage"
	"testing"
)

type nullProxy struct{}

func (p *nullProxy) Add(topic string, offset int64) error { return nil }
func (p *nullProxy) Remove(topic string) error            { return nil }
func (p *nullProxy) AddGroup()                            {}
func (p *nullProxy) Stop()                                {}

func TestUpdateWithHeaders(t *testing.T) {
	s := storageProxy{
		update: func(s storage.Storage, partition int32, key string, value []byte, headers ...*sarama.RecordHeader) error {
			if len(headers) == 0 {
				t.Errorf("Missing headers")
				return nil
			}
			if !bytes.Equal(headers[0].Key, []byte("key")) {
				t.Errorf("Key missmatch. Expected %q. Found: %q", "key", headers[0].Key)
			}
			if !bytes.Equal(headers[0].Value, []byte("value")) {
				t.Errorf("Key missmatch. Expected %q. Found: %q", "value", headers[0].Value)
			}
			return nil
		},
	}
	_ = s.Update("", nil, &sarama.RecordHeader{Key: []byte("key"), Value: []byte("value")})
}