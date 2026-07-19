package goka

import (
	"errors"
	"testing"
)

func TestHeaders_Merged(t *testing.T) {
	h1 := Headers{
		"key1": []byte("val1"),
	}
	h2 := Headers{
		"key1": []byte("val1b"),
		"key2": []byte("val2"),
	}
	merged := h1.Merged(h2)

	if len(h1) != 1 || string(h1["key1"]) != "val1" {
		t.Errorf("Merged failed: receiver was modified")
	}

	if len(h2) != 2 || string(h2["key1"]) != "val1b" || string(h2["key2"]) != "val2" {
		t.Errorf("Merged failed: argument was modified")
	}

	if len(merged) != 2 {
		t.Errorf("Merged failed: expected %d keys, but found %d", 2, len(merged))
	}

	if string(merged["key1"]) != "val1b" {
		t.Errorf("Merged failed: expected %q for key %q, but found %q",
			"val1b", "key1", string(merged["key1"]))
	}

	if string(merged["key2"]) != "val2" {
		t.Errorf("Merged failed: expected %q for key %q, but found %q",
			"val2", "key2", string(merged["key2"]))
	}
}

func TestTextMapHeaders(t *testing.T) {
	h := Headers{"traceparent": []byte("00-abc")}
	carrier := TextMapHeaders(h)

	if got := carrier.Get("traceparent"); got != "00-abc" {
		t.Errorf("Get: got %q, want %q", got, "00-abc")
	}
	if got := carrier.Get("missing"); got != "" {
		t.Errorf("Get missing: got %q, want empty", got)
	}

	carrier.Set("x-datadog-trace-id", "123")
	if string(h["x-datadog-trace-id"]) != "123" {
		t.Errorf("Set did not write through: %q", h["x-datadog-trace-id"])
	}

	keys := carrier.Keys()
	if len(keys) != 2 {
		t.Errorf("Keys: got %d, want 2", len(keys))
	}

	seen := map[string]string{}
	err := carrier.ForeachKey(func(key, val string) error {
		seen[key] = val
		return nil
	})
	if err != nil {
		t.Fatalf("ForeachKey: %v", err)
	}
	if seen["traceparent"] != "00-abc" || seen["x-datadog-trace-id"] != "123" {
		t.Errorf("ForeachKey seen: %#v", seen)
	}

	stop := errors.New("stop")
	err = carrier.ForeachKey(func(key, val string) error {
		return stop
	})
	if !errors.Is(err, stop) {
		t.Errorf("ForeachKey should return handler error, got %v", err)
	}

	var nilCarrier TextMapHeaders
	if nilCarrier.Get("x") != "" {
		t.Errorf("nil Get should return empty")
	}
	nilCarrier.Set("x", "y") // must not panic
	if nilCarrier.Keys() != nil {
		t.Errorf("nil Keys should return nil")
	}
	if err := nilCarrier.ForeachKey(func(key, val string) error {
		t.Errorf("unexpected key %q", key)
		return nil
	}); err != nil {
		t.Errorf("nil ForeachKey: %v", err)
	}
}
