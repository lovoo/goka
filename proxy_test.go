package goka

import (
	"bytes"
	"testing"
)

type nullProxy struct{}

func (p *nullProxy) Add(topic string, offset int64) error { return nil }
func (p *nullProxy) Remove(topic string) error            { return nil }
func (p *nullProxy) AddGroup()                            {}
func (p *nullProxy) Stop()                                {}

func TestUpdateWithHeaders(t *testing.T) {
	s := storageProxy{
		update: func(ctx UpdateContext) error {
			if len(ctx.Headers()) == 0 {
				t.Errorf("Missing headers")
				return nil
			}
			if !bytes.Equal(ctx.Headers()["key"], []byte("value")) {
				t.Errorf("Key missmatch. Expected %q. Found: %q", "key", ctx.Headers()["key"])
			}
			return nil
		},
	}
	_ = s.Update("", nil, 0, Headers{"key": []byte("value")})
}
