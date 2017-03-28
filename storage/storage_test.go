package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/lovoo/goka/codec"

	"github.com/facebookgo/ensure"
	"github.com/rcrowley/go-metrics"
)

func TestMemStorageDelete(t *testing.T) {
	storage := NewMock(&codec.String{})

	has, err := storage.Has("key-1")
	ensure.Nil(t, err)
	ensure.False(t, has)

	err = storage.Set("key-1", "content-1")
	ensure.Nil(t, err)

	has, err = storage.Has("key-1")
	ensure.Nil(t, err)
	ensure.True(t, has)

	err = storage.Delete("key-1")
	ensure.Nil(t, err)

	has, err = storage.Has("key-1")
	ensure.Nil(t, err)
	ensure.False(t, has)
}

func TestMemIter(t *testing.T) {
	storage := NewMock(&codec.String{})

	kv := map[string]string{
		"key-1": "val-1",
		"key-2": "val-2",
		"key-3": "val-3",
	}

	found := map[string]string{}

	for k, v := range kv {
		storage.Set(k, v)
	}

	// released iterator should be immidiately exhausted
	iter := storage.Iterator()
	iter.Release()
	ensure.False(t, iter.Next(), "released iterator had a next")

	iter = storage.Iterator()
	for iter.Next() {
		raw, err := iter.Value()
		ensure.Nil(t, err)

		key := string(iter.Key())
		val, ok := raw.(string)
		ensure.True(t, ok)

		v, ok := kv[key]
		ensure.True(t, ok, fmt.Sprintf("unexpected key returned from iterator: %s", key))
		ensure.DeepEqual(t, val, v, fmt.Sprintf("iterator returned wrong value: %s, expected: %s", val, v))

		found[key] = val
	}

	key := iter.Key()
	val, err := iter.Value()
	ensure.Nil(t, err, "exhausted iterator should not return error")
	ensure.True(t, key == nil, fmt.Sprintf("exhausted iterator should return nil key, returned: %s", key))
	ensure.True(t, val == nil, "exhausted iterator should return nil value, returned %s", val)

	ensure.DeepEqual(t, found, kv, "found doesn't match kv, iterator probably didn't return all values")
}

func TestGetHas(t *testing.T) {
	storage := NewMock(&codec.String{})

	var (
		err    error
		hasKey bool
	)

	hasKey, err = storage.Has("test-key")
	ensure.Nil(t, err)
	ensure.False(t, hasKey)

	value, err := storage.Get("test-key")
	ensure.True(t, value == nil)
	ensure.Nil(t, err)

	err = storage.Set("test-key", "test")
	ensure.Nil(t, err)

	hasKey, err = storage.Has("test-key")
	ensure.Nil(t, err)
	ensure.True(t, hasKey)

	value, err = storage.Get("test-key")
	ensure.Nil(t, err)
	ensure.DeepEqual(t, value, "test")

	hasKey, err = storage.Has("nil-value")
	ensure.Nil(t, err)
	ensure.False(t, hasKey)

	err = storage.Set("nil-value", nil)
	ensure.NotNil(t, err)
}

type myMessage struct {
	User  string `json:"user"`
	Value bool   `json:"value"`
}

type myCodec struct{}

func (m *myCodec) Encode(key string, value interface{}) ([]byte, error) {
	msg := value.(*myMessage)
	data, unmarshalErr := json.Marshal(msg)
	return data, unmarshalErr
}

func (m *myCodec) Decode(key string, data []byte) (interface{}, error) {
	msg := &myMessage{}
	unmarshalErr := json.Unmarshal(data, msg)
	return msg, unmarshalErr
}

func TestSetGet_json(t *testing.T) {
	var (
		err    error
		hasKey bool
	)
	testDbPath := "/tmp/statemanagertest.go"
	os.RemoveAll(testDbPath)
	storage, err := New(testDbPath, &codec.String{}, metrics.NewRegistry(), DefaultStorageSnapshotInterval)
	ensure.Nil(t, err)

	hasKey, err = storage.Has("example1")
	ensure.Nil(t, err)
	ensure.False(t, hasKey)

	value, err := storage.Get("example1")
	ensure.True(t, value == nil)
	ensure.Nil(t, err)

	exampleMessage := &myMessage{
		User:  "someuser",
		Value: true,
	}

	err = storage.Set("example1", exampleMessage)
	ensure.Nil(t, err)

	hasKey, err = storage.Has("example1")
	ensure.Nil(t, err)
	ensure.True(t, hasKey)

	value, err = storage.Get("example1")
	ensure.Nil(t, err)

	ensure.Nil(t, storage.Delete("example1"))
	hasKey, err = storage.Has("example1")
	ensure.Nil(t, err)
	ensure.False(t, hasKey)

	// test iteration
	ensure.Nil(t, storage.SetEncoded("encoded", []byte("encoded-value")))
	ensure.Nil(t, storage.Set("decoded", "decoded-value"))
	iter := storage.Iterator()
	defer iter.Release()
	messages := map[string]interface{}{}
	for iter.Next() {
		key := string(iter.Key())
		val, err := iter.Value()
		ensure.Nil(t, err)
		messages[key] = val
	}
	ensure.True(t, len(messages) == 2, fmt.Sprintf("expected 2 messages, got: %d", len(messages)))
	ensure.DeepEqual(t, messages["encoded"], "encoded-value")
	ensure.DeepEqual(t, messages["decoded"], "decoded-value")

	recoveredValue, is := value.(*myMessage)
	ensure.True(t, is)
	ensure.DeepEqual(t, recoveredValue, exampleMessage)
}
