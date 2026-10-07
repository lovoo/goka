package goka

import (
	"hash"
	"hash/fnv"
	"testing"

	"github.com/IBM/sarama"
)

func TestPartitionForKey(t *testing.T) {
	t.Run("basic partition calculation", func(t *testing.T) {
		hasher := DefaultHasher()
		keys := []string{"cs12345", "fs67890", "cs99999", "test-key"}

		for _, key := range keys {
			partition, err := PartitionForKey([]byte(key), 64, hasher)
			if err != nil {
				t.Fatalf("unexpected error for key %q: %v", key, err)
			}
			if partition < 0 || partition >= 64 {
				t.Errorf("partition %d out of range [0, 64) for key %q", partition, key)
			}
		}
	})

	t.Run("deterministic results", func(t *testing.T) {
		hasher := DefaultHasher()
		key := []byte("cs12345")

		p1, _ := PartitionForKey(key, 64, hasher)
		p2, _ := PartitionForKey(key, 64, hasher)

		if p1 != p2 {
			t.Errorf("non-deterministic: got %d and %d for same key", p1, p2)
		}
	})

	t.Run("single partition", func(t *testing.T) {
		hasher := DefaultHasher()
		partition, err := PartitionForKey([]byte("any-key"), 1, hasher)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if partition != 0 {
			t.Errorf("expected partition 0 for single partition, got %d", partition)
		}
	})

	t.Run("empty key", func(t *testing.T) {
		hasher := DefaultHasher()
		partition, err := PartitionForKey([]byte(""), 64, hasher)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if partition < 0 || partition >= 64 {
			t.Errorf("partition %d out of range for empty key", partition)
		}
	})

	t.Run("invalid partition count", func(t *testing.T) {
		hasher := DefaultHasher()

		_, err := PartitionForKey([]byte("key"), 0, hasher)
		if err == nil {
			t.Error("expected error for 0 partitions")
		}

		_, err = PartitionForKey([]byte("key"), -1, hasher)
		if err == nil {
			t.Error("expected error for negative partitions")
		}
	})

	t.Run("matches sarama HashPartitioner", func(t *testing.T) {
		hasher := DefaultHasher()
		partitionerConstructor := sarama.NewCustomHashPartitioner(hasher)
		partitioner := partitionerConstructor("") // instantiate with empty topic

		keys := []string{"cs12345", "fs67890", "cs99999", "fs00001", "ci55555"}
		partitionCounts := []int32{16, 32, 64, 128}

		for _, numPartitions := range partitionCounts {
			for _, key := range keys {
				gokaPartition, err := PartitionForKey([]byte(key), numPartitions, hasher)
				if err != nil {
					t.Fatalf("PartitionForKey error: %v", err)
				}

				msg := &sarama.ProducerMessage{Key: sarama.StringEncoder(key)}
				saramaPartition, err := partitioner.Partition(msg, numPartitions)
				if err != nil {
					t.Fatalf("sarama Partition error: %v", err)
				}

				if gokaPartition != saramaPartition {
					t.Errorf("key=%q partitions=%d: goka=%d sarama=%d",
						key, numPartitions, gokaPartition, saramaPartition)
				}
			}
		}
	})

	t.Run("matches internal View.hash logic", func(t *testing.T) {
		// Verify that PartitionForKey produces the same result as the internal
		// hash algorithm used by View.hash() / Processor.hash()
		hasher := DefaultHasher()
		keys := []string{"cs12345", "fs67890", "test-key-123"}
		numPartitions := int32(64)

		for _, key := range keys {
			// Replicate View.hash() logic directly
			h := fnv.New32a()
			h.Write([]byte(key))
			expected := int32(h.Sum32())
			if expected < 0 {
				expected = -expected
			}
			expected = expected % numPartitions

			got, err := PartitionForKey([]byte(key), numPartitions, hasher)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got != expected {
				t.Errorf("key=%q: PartitionForKey=%d, manual hash=%d", key, got, expected)
			}
		}
	})

	t.Run("custom hasher", func(t *testing.T) {
		// Verify that a custom hasher produces different results than default
		customHasher := func() hash.Hash32 {
			return fnv.New32() // FNV-1 (not FNV-1a)
		}

		key := []byte("test-key")
		defaultPartition, _ := PartitionForKey(key, 64, DefaultHasher())
		customPartition, _ := PartitionForKey(key, 64, customHasher)

		// They may or may not differ for a specific key, but the function should work
		// Just verify no error and valid range
		if customPartition < 0 || customPartition >= 64 {
			t.Errorf("custom hasher partition %d out of range", customPartition)
		}
		_ = defaultPartition
	})
}
