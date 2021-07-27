package goka

import (
	"testing"

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
			if ctx.Headers().Len() == 0 {
				t.Errorf("Missing headers")
				return nil
			}
			if ctx.Headers().StrVal("key") != "value" {
				t.Errorf("Key missmatch. Expected %q. Found: %q", "key", ctx.Headers().StrVal("key"))
			}
			return nil
		},
	}
	headers, err := NewHeaders("key", "value")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_ = s.Update("", nil, 0, headers)
}
