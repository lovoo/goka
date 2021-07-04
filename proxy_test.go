package goka

import (
	"bytes"
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
		update: func(s storage.Storage, partition int32, key string, value []byte, headers Headers) error {
			if len(headers) == 0 {
				t.Errorf("Missing headers")
				return nil
			}
			if !bytes.Equal(headers["key"], []byte("value")) {
				t.Errorf("Key missmatch. Expected %q. Found: %q", "key", headers["key"])
			}
			return nil
		},
	}
	_ = s.Update("", nil, Headers{"key": []byte("value")})
}
