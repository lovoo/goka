package goka

import (
	"testing"

	"github.com/Shopify/sarama"
)

func TestHeaders_Merged(t *testing.T) {
	h1, _ := NewHeaders()
	h1.Set("key1", []byte("val1"))
	h2, _ := NewHeaders()
	h2.Set("key1", []byte("val1b"))
	h2.Set("key2", []byte("val2"))
	merged := h1.Merged(h2)

	if h1.Len() != 1 || h1.StrVal("key1") != "val1" {
		t.Errorf("Merged failed: reciver was modified")
	}

	if h2.Len() != 2 || h2.StrVal("key1") != "val1b" || h2.StrVal("key2") != "val2" {
		t.Errorf("Merged failed: argument was modified")
	}

	if merged.Len() != 2 {
		t.Errorf("Merged failed: expected %d keys, but found %d", 2, merged.Len())
	}

	if merged.StrVal("key1") != "val1b" {
		t.Errorf("Merged failed: expected %q for key %q, but found %q",
			"val1b", "key1", merged.StrVal("key1"))
	}

	if merged.StrVal("key2") != "val2" {
		t.Errorf("Merged failed: expected %q for key %q, but found %q",
			"val2", "key2", merged.StrVal("key2"))
	}
}

func TestHeaders_Lazy(t *testing.T) {
	h := HeadersFromSarama([]*sarama.RecordHeader{{Key: []byte("key"), Value: []byte("value")}})
	if len(h.sarama) == 0 {
		t.Fatalf("Sarama headers should be set")
	}
	if len(h.goka) > 0 {
		t.Fatalf("Goka headers should not be set")
	}

	if h.Len() != 1 {
		t.Fatalf("Unexpected header count: expected 1, but found %d", h.Len())
	}

	if len(h.sarama) > 0 {
		t.Fatalf("Sarama headers should not be set")
	}
	if len(h.goka) == 0 {
		t.Fatalf("Goka headers should be set")
	}
}

func TestHeaders_New(t *testing.T) {
	h, err := NewHeaders("k1", "v1", "k2", "v2")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if h.Len() != 2 {
		t.Fatalf("Unexpected header count: expected 2, but found %d", h.Len())
	}

	if h.StrVal("k1") != "v1" {
		t.Fatalf("Unexpected value for k1: %q", h.StrVal("k1"))
	}
	if h.StrVal("k2") != "v2" {
		t.Fatalf("Unexpected value for k2: %q", h.StrVal("k2"))
	}

	h, err = NewHeaders("k")
	if err == nil {
		t.Fatalf("Expected an error for a key without value")
	}
}
