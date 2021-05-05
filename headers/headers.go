package headers

import "github.com/Shopify/sarama"

// Headers represents custom message headers with a convenient interface.
type Headers map[string][]byte

// Merged returns a new map with all headers from all maps. Later keys override earlier ones.
// If all headers are empty, nil is returned to allow using directly in emit functions.
// This is safe to call on a nil map.
func (h Headers) Merged(headers ...Headers) Headers {
	merged := Headers{}
	for k, v := range h {
		merged[k] = v
	}
	for _, hs := range headers {
		for k, v := range hs {
			merged[k] = v
		}
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}

// ToSarama converts the headers to sarama's header type.
func (h Headers) ToSarama() []sarama.RecordHeader {
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

// FromSarama converts sarama's list of headers to Headers.
// Later records override earlier ones.
func FromSarama(saramaHeaders []*sarama.RecordHeader) Headers {
	headers := make(Headers, len(saramaHeaders))
	for _, h := range saramaHeaders {
		headers[string(h.Key)] = h.Value
	}
	return headers
}
