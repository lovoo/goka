package goka

import "testing"

func TestMergeHeaders(t *testing.T) {
	merged := MergeHeaders(
		map[string][]byte{"key1": []byte("val1")},
		map[string][]byte{"key1": []byte("val1b"), "key2": []byte("val2")},
	)
	if len(merged) != 2 {
		t.Errorf("MergeHeaders failed: expected %d keys, but found %d", 2, len(merged))
	}
	if string(merged["key1"]) != "val1b" {
		t.Errorf("MergeHeaders failed: expected %q for key %q, but found %q",
			"val1b", "key1", merged["key1"])
	}
	if string(merged["key2"]) != "val2" {
		t.Errorf("MergeHeaders failed: expected %q for key %q, but found %q",
			"val2", "key2", merged["key2"])
	}
}