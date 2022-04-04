package goka

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
)

func TestConfig_DefaultConfig(t *testing.T) {
	t.Run("equal", func(t *testing.T) {
		cfg := DefaultConfig()
		require.True(t, cfg.Version == sarama.V2_0_0_0)
		require.True(t, cfg.Consumer.Return.Errors == true)
		require.True(t, cfg.Consumer.MaxProcessingTime == defaultMaxProcessingTime)
		require.True(t, cfg.Consumer.Offsets.Initial == sarama.OffsetNewest)
		require.True(t, cfg.Producer.RequiredAcks == sarama.WaitForLocal)
		require.True(t, cfg.Producer.Compression == sarama.CompressionSnappy)
		require.True(t, cfg.Producer.Flush.Frequency == defaultFlushFrequency)
		require.True(t, cfg.Producer.Flush.Bytes == defaultFlushBytes)
		require.True(t, cfg.Producer.Return.Successes == true)
		require.True(t, cfg.Producer.Return.Errors == true)
		require.True(t, cfg.Producer.Retry.Max == defaultProducerMaxRetries)
	})
}

func TestConfig_ReplaceGlobalConfig(t *testing.T) {
	t.Run("succeed", func(t *testing.T) {
		custom := DefaultConfig()
		custom.Version = sarama.V0_8_2_0
		ReplaceGlobalConfig(custom)
		require.Equal(t, globalConfig.Version, custom.Version)
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
