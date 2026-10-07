package goka

import (
	"fmt"
	"hash"
)

// PartitionForKey calculates the partition ID for a given key using the provided
// hasher factory and partition count. This uses the same algorithm as View.hash()
// and Processor.hash() internally, making it safe to predict partition assignments
// externally without access to a running View or Processor instance.
//
// Use DefaultHasher() as the hasher argument unless a custom hasher was configured
// via WithHasher/WithViewHasher/WithEmitterHasher when creating the View/Processor.
func PartitionForKey(key []byte, numPartitions int32, hasher func() hash.Hash32) (int32, error) {
	if numPartitions <= 0 {
		return 0, fmt.Errorf("invalid partition count: %d", numPartitions)
	}

	h := hasher()
	_, err := h.Write(key)
	if err != nil {
		return -1, fmt.Errorf("failed to hash key: %w", err)
	}

	p := int32(h.Sum32())
	if p < 0 {
		p = -p
	}
	return p % numPartitions, nil
}
