package goka

import (
	"testing"

	"github.com/Shopify/sarama"
)

func TestConfig_DefaultConfig(t *testing.T) {
	t.Run("equal", func(t *testing.T) {
		cfg := DefaultConfig()
		assertTrue(t, cfg.Version == sarama.V2_0_0_0)
		assertTrue(t, cfg.Consumer.Return.Errors == true)
		assertTrue(t, cfg.Consumer.MaxProcessingTime == defaultMaxProcessingTime)
		assertTrue(t, cfg.Consumer.Offsets.Initial == sarama.OffsetNewest)
		assertTrue(t, cfg.Producer.RequiredAcks == sarama.WaitForLocal)
		assertTrue(t, cfg.Producer.Compression == sarama.CompressionSnappy)
		assertTrue(t, cfg.Producer.Flush.Frequency == defaultFlushFrequency)
		assertTrue(t, cfg.Producer.Flush.Bytes == defaultFlushBytes)
		assertTrue(t, cfg.Producer.Return.Successes == true)
		assertTrue(t, cfg.Producer.Return.Errors == true)
		assertTrue(t, cfg.Producer.Retry.Max == defaultProducerMaxRetries)
	})
}

func TestConfig_ReplaceGlobalConfig(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		custom := DefaultConfig()
		custom.Version = sarama.V0_8_2_0
		ReplaceGlobalConfig(custom)
		assertEqual(t, globalConfig.Version, custom.Version)
	})
	t.Run("panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("there was no panic")
			}
		}()
		ReplaceGlobalConfig(nil)
	})
}
