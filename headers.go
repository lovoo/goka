package goka

import "github.com/Shopify/sarama"

// MergeHeaders returns new headers with all elements from both maps. Later keys override earlier ones.
// If all headers are empty, nil is returned to allow using directly in emit functions.
func MergeHeaders(headers ...map[string][]byte) map[string][]byte {
	merged := map[string][]byte{}
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

// ToSaramaHeaders converts headers to sarama's header type.
func ToSaramaHeaders(headers map[string][]byte) []sarama.RecordHeader {
	recordHeaders := make([]sarama.RecordHeader, 0, len(headers))
	for key, value := range headers {
		recordHeaders = append(recordHeaders,
			sarama.RecordHeader{
				Key:   []byte(key),
				Value: value,
			})
	}
	return recordHeaders
}

// FromSaramaHeaders converts sarama's list of headers to Headers. Later records override earlier ones.
func FromSaramaHeaders(saramaHeaders []*sarama.RecordHeader) map[string][]byte {
	headers := make(map[string][]byte, len(saramaHeaders))
	for _, h := range saramaHeaders {
		headers[string(h.Key)] = h.Value
	}
	return headers
}