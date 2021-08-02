package goka

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka/storage"
)

type nullProxy struct{}

func (p *nullProxy) Add(topic string, offset int64) error { return nil }
func (p *nullProxy) Remove(topic string) error            { return nil }
func (p *nullProxy) AddGroup()                            {}
func (p *nullProxy) Stop()                                {}

func TestUpdateWithHeaders(t *testing.T) {
	s := storageProxy{
		update: func(ctx UpdateContext, s storage.Storage, key string, value []byte) error {
			if len(ctx.Headers()) == 0 {
				t.Errorf("Missing headers")
				return nil
			}
			if string(ctx.Headers()["key"]) != "value" {
				t.Errorf("Key missmatch. Expected %q. Found: %q", "key", string(ctx.Headers()["key"]))
			}
			return nil
		},
	}
	_ = s.Update(&DefaultUpdateContext{headers: []*sarama.RecordHeader{{Key: []byte("key"), Value: []byte("value")}}}, "", nil)
}
