package redis

import (
	"runtime/debug"
	"testing"
)

func Test_redisIterator(t *testing.T) {
	assertRedisIterator(t, &redisIterator{
		current: 0,
		keys: []string{
			"key1",
			"val1",
			offsetKey,
			"123",
			"key2",
			"val2",
		},
	})
	assertRedisIterator(t, &redisIterator{
		current: 0,
		keys: []string{
			offsetKey,
			"123",
			"key1",
			"val1",
			"key2",
			"val2",
		},
	})
	assertRedisIterator(t, &redisIterator{
		current: 0,
		keys: []string{
			"key1",
			"val1",
			"key2",
			"val2",
			offsetKey,
			"123",
		},
	})
}

func assertRedisIterator(t *testing.T, it *redisIterator) {
	// the iterator contract implies we must call `Next()` first
	it.Next()
	assertRedisIteratorKey(t, it, "key1", "val1")

	it.Next()
	assertRedisIteratorKey(t, it, "key2", "val2")
	
	it.Next()
	if !it.exhausted() {
		t.Fatalf("Expected iterator to be exhausted in %s", string(debug.Stack()))
	}
}

func assertRedisIteratorKey(t *testing.T, it *redisIterator, expectedKey string, expectedValue string) {
	if it.exhausted() {
		t.Fatalf("Did not expect iterator to be exhausted in %s", string(debug.Stack()))
	}

	actualKey := string(it.Key())
	if actualKey != expectedKey {
		t.Fatalf("Expected iterator key to be '%s', but was '%s' in %s", expectedKey, actualKey, string(debug.Stack()))
	}

	actualValue, _ := it.Value()
	if string(actualValue) != expectedValue {
		t.Fatalf("Expected iterator value to be '%s', but was '%s' in %s", expectedValue, actualValue, string(debug.Stack()))
	}
}
