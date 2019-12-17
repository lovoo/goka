package goka

import (
	"reflect"
	"runtime/debug"
	"testing"
)

func assertTrue(t *testing.T, value bool) {
	if !value {
		t.Fatalf("Expected value to be true, but was false in %s", string(debug.Stack()))
	}
}
func assertFalse(t *testing.T, value bool) {
	if value {
		t.Fatalf("Expected value to be false, but was true in %s", string(debug.Stack()))
	}
}

func assertEqual(t *testing.T, actual, expected interface{}) {
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("Expected values were not equal.\nactual=%#v\nexpected=%#v in %s", actual, expected, string(debug.Stack()))
	}
}
