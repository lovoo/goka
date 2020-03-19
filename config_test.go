package goka

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/lovoo/goka/internal/test"
)

func TestConfig_DefaultConfig(t *testing.T) {
	t.Run("equal", func(t *testing.T) {
		cfg := DefaultConfig()
		test.AssertTrue(t, cfg.Version == sarama.V2_0_0_0)
		test.AssertTrue(t, cfg.Consumer.Return.Errors == true)
		test.AssertTrue(t, cfg.Consumer.MaxProcessingTime == defaultMaxProcessingTime)
		test.AssertTrue(t, cfg.Consumer.Offsets.Initial == sarama.OffsetNewest)
		test.AssertTrue(t, cfg.Producer.RequiredAcks == sarama.WaitForLocal)
		test.AssertTrue(t, cfg.Producer.Compression == sarama.CompressionSnappy)
		test.AssertTrue(t, cfg.Producer.Flush.Frequency == defaultFlushFrequency)
		test.AssertTrue(t, cfg.Producer.Flush.Bytes == defaultFlushBytes)
		test.AssertTrue(t, cfg.Producer.Return.Successes == true)
		test.AssertTrue(t, cfg.Producer.Return.Errors == true)
		test.AssertTrue(t, cfg.Producer.Retry.Max == defaultProducerMaxRetries)
	})
}

func TestConfig_ReplaceGlobalConfig(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		custom := DefaultConfig()
		custom.Version = sarama.V0_8_2_0
		ReplaceGlobalConfig(custom)
		test.AssertEqual(t, globalConfig.Version, custom.Version)
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
