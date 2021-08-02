package goka

import (
	"github.com/Shopify/sarama"
)

// Headers represents custom message headers with a convenient interface.
type Headers map[string][]byte

// HeadersFromSarama converts sarama headers to goka's type.
func HeadersFromSarama(saramaHeaders []*sarama.RecordHeader) Headers {
	headers := Headers{}
	for _, rec := range saramaHeaders {
		headers[string(rec.Key)] = rec.Value
	}
	return headers
}

// Merged returns a new instance with all headers merged. Later keys override earlier ones.
// Handles a nil receiver and nil arguments without panics.
// If all headers are empty, nil is returned to allow using directly in emit functions.
func (h Headers) Merged(headersList ...Headers) Headers {
	// optimize for headerless processors.
	if len(h) == 0 && allEmpty(headersList...) {
		return nil
	}

	merged := Headers{}
	for k, v := range h {
		merged[k] = v
	}

	for _, headers := range headersList {
		for k, v := range headers {
			merged[k] = v
		}
	}

	if len(merged) == 0 {
		return nil
	}

	return merged
}

// ToSarama converts the headers to a slice of sarama.RecordHeader.
// If called on a nil receiver returns nil.
func (h Headers) ToSarama() []sarama.RecordHeader {
	if h == nil {
		return nil
	}

	recordHeaders := make([]sarama.RecordHeader, 0, len(h))
	for key, value := range h {
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
func (h Headers) ToSaramaPtr() []*sarama.RecordHeader {
	if h == nil {
		return nil
	}

	recordHeaders := make([]*sarama.RecordHeader, 0, len(h))
	for key, value := range h {
		recordHeaders = append(recordHeaders,
			&sarama.RecordHeader{
				Key:   []byte(key),
				Value: value,
			})
	}
	return recordHeaders
}

func allEmpty(headersList ...Headers) bool {
	for _, headers := range headersList {
		if len(headers) != 0 {
			return false
		}
	}
	return true
}
