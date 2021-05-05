package headers

import "testing"

func TestHeaders_Merged(t *testing.T) {
	h1 := Headers{"key1": []byte("val1")}
	h2 := Headers{"key1": []byte("val1b"), "key2": []byte("val2")}
	merged := h1.Merged(h2)

	if len(h1) != 1 || string(h1["key1"]) != "val1" {
		t.Errorf("Merged failed: reciver was modified")
	}

	if len(h2) != 2 || string(h2["key1"]) != "val1b" || string(h2["key2"]) != "val2" {
		t.Errorf("Merged failed: argument was modified")
	}

	if len(merged) != 2 {
		t.Errorf("Merged failed: expected %d keys, but found %d", 2, len(merged))
	}

	if string(merged["key1"]) != "val1b" {
		t.Errorf("Merged failed: expected %q for key %q, but found %q",
			"val1b", "key1", merged["key1"])
	}

	if string(merged["key2"]) != "val2" {
		t.Errorf("Merged failed: expected %q for key %q, but found %q",
			"val2", "key2", merged["key2"])
	}
}
