package goka

import (
	"fmt"

	"github.com/Shopify/sarama"
)

// Headers represents custom message headers with a convenient interface.
type Headers struct {
	// Actual headers stored here.
	goka map[string][]byte
	// Temp sotrage for sarama headers to allow lazy evaluation.
	sarama []*sarama.RecordHeader
}

// NewHeaders creates a new headers instance.
func NewHeaders(keyVal ...string) (*Headers, error) {
	h := &Headers{
		goka: make(map[string][]byte),
	}
	key := ""
	for _, kv := range keyVal {
		if key == "" {
			key = kv
			continue
		}
		h.Set(key, []byte(kv))
		key = ""
	}
	if key != "" {
		return h, fmt.Errorf("header key provided without value: %q", key)
	}
	return h, nil
}

// HeadersFromSarama lazily converts sarama headers. Later records override earlier ones.
// Conversion happens when headers are read.
func HeadersFromSarama(saramaHeaders []*sarama.RecordHeader) *Headers {
	headers, _ := NewHeaders()
	headers.sarama = saramaHeaders
	return headers
}

// Set adds/overrides a key-value to the set.
// Pending sarama headers might overwrite this when evaluated.
// To avoid that, call one of the read functions to trigger lazy evaluation.
func (h *Headers) Set(key string, value []byte) {
	h.goka[key] = value
}

// Val returns the header value associated with the provided key.
func (h *Headers) Val(key string) []byte {
	h.eval()
	return h.goka[key]
}

// StrVal returns the header value associated with the provided key as a string.
func (h *Headers) StrVal(key string) string {
	h.eval()
	return string(h.goka[key])
}

// Len returns the number of headers in the set.
// Returns 0 if called on a nil receiver to match the behavior of built-in `len` function.
func (h *Headers) Len() int {
	if h == nil {
		return 0
	}
	h.eval()
	return len(h.goka)
}

// Merged returns new instance with all headers merged. Later keys override earlier ones.
// Handles a nil receiver and nil arguments without panics.
// If all headers are empty, nil is returned to allow using directly in emit functions.
func (h *Headers) Merged(headers ...*Headers) *Headers {
	// optimize for headerless processors.
	if allEmpty(h, headers...) {
		return nil
	}

	merged, _ := NewHeaders()
	if h.Len() > 0 {
		for k, v := range h.goka {
			merged.goka[k] = v
		}
	}

	for _, hs := range headers {
		if hs.Len() == 0 {
			continue
		}
		for k, v := range hs.goka {
			merged.goka[k] = v
		}
	}

	if len(merged.goka) == 0 {
		return nil
	}

	return merged
}

// ToSarama converts the headers to a slice of sarama.RecordHeader.
// If called on a nil receiver returns nil.
func (h *Headers) ToSarama() []sarama.RecordHeader {
	if h == nil {
		return nil
	}

	h.eval()
	recordHeaders := make([]sarama.RecordHeader, 0, len(h.goka))
	for key, value := range h.goka {
		recordHeaders = append(recordHeaders,
			sarama.RecordHeader{
				Key:   []byte(key),
				Value: value,
			})
	}
	return recordHeaders
}

// ToSaramaPtr converts the headers to a slice of pointers to sarama.RecordHeader.
// If called on a nil receiver returns nil.
func (h *Headers) ToSaramaPtr() []*sarama.RecordHeader {
	if h == nil {
		return nil
	}

	h.eval()
	recordHeaders := make([]*sarama.RecordHeader, 0, len(h.goka))
	for key, value := range h.goka {
		recordHeaders = append(recordHeaders,
			&sarama.RecordHeader{
				Key:   []byte(key),
				Value: value,
			})
	}
	return recordHeaders
}

// eval converts any internal sarama headers.
// It must be called from every accessor function.
func (h *Headers) eval() {
	if len(h.sarama) == 0 {
		return
	}
	for _, rec := range h.sarama {
		h.goka[string(rec.Key)] = rec.Value
	}
	h.sarama = nil
}

func allEmpty(first *Headers, rest ...*Headers) bool {
	if first.Len() != 0 {
		return false
	}
	for _, hs := range rest {
		if hs.Len() != 0 {
			return false
		}
	}
	return true
}
