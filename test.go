package goka

import (
	"reflect"
	"runtime/debug"
	"testing"
)

func assertNil(t *testing.T, actual interface{}) {
	value := reflect.ValueOf(actual)
	if value.IsValid() {
		if !value.IsNil() {
			t.Fatalf("Expected value to be nil, but was not nil in %s", string(debug.Stack()))
		}
	}
}

func assertNotNil(t *testing.T, actual interface{}) {
	value := reflect.ValueOf(actual)
	if !value.IsValid() || value.IsNil() {
		t.Fatalf("Expected value to be not nil, but was nil in %s", string(debug.Stack()))
	}
}

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

func panicAssertEqual(t *testing.T, expected interface{}) {
	if expected == nil {
		panic("can't pass nil to panicAssertEqual")
	}
	actual := recover()
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("Expected values were equal.\nactual=%#v\nexpected=%#v in %s", actual, expected, string(debug.Stack()))
	}
}

func assertNotEqual(t *testing.T, actual, expected interface{}) {
	if reflect.DeepEqual(actual, expected) {
		t.Fatalf("Expected values were equal.\nactual=%#v\nexpected=%#v in %s", actual, expected, string(debug.Stack()))
	}
}

func assertFuncEqual(t *testing.T, actual, expected interface{}) {
	if !(reflect.ValueOf(actual).Pointer() == reflect.ValueOf(expected).Pointer()) {
		t.Fatalf("Expected functions were equal.\nactual=%#v\nexpected=%#v in %s", actual, expected, string(debug.Stack()))
	}
}
