package test

import (
	"reflect"
	"regexp"
	"runtime/debug"
	"strings"
)

type Fataler interface {
	Fatalf(string, ...interface{})
}

func AssertNil(t Fataler, actual interface{}) {
	value := reflect.ValueOf(actual)
	if value.IsValid() {
		if !value.IsNil() {

			t.Fatalf("Expected value to be nil, but was not nil (%v) in %s", actual, string(debug.Stack()))
		}
	}
}

func AssertNotNil(t Fataler, actual interface{}) {
	value := reflect.ValueOf(actual)
	if !value.IsValid() || value.IsNil() {
		t.Fatalf("Expected value to be not nil, but was nil in %s", string(debug.Stack()))
	}
}

func AssertTrue(t Fataler, value bool, fields ...interface{}) {
	if !value {
		t.Fatalf("Expected value to be true, but was false in %s: %v", string(debug.Stack()), fields)
	}
}
func AssertFalse(t Fataler, value bool) {
	if value {
		t.Fatalf("Expected value to be false, but was true in %s", string(debug.Stack()))
	}
}

func AssertEqual(t Fataler, actual, expected interface{}) {
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("Expected values were not equal.\nactual=%#v\nexpected=%#v in %s", actual, expected, string(debug.Stack()))
	}
}

func PanicAssertEqual(t Fataler, expected interface{}) {
	if expected == nil {
		panic("can't pass nil to test.PanicAssertEqual")
	}
	if actual := recover(); actual != nil {
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("Expected values were equal.\nactual=%#v\nexpected=%#v in %s", actual, expected, string(debug.Stack()))
		}
	} else {
		t.Fatalf("panic expected")
	}

}

func AssertNotEqual(t Fataler, actual, expected interface{}) {
	if reflect.DeepEqual(actual, expected) {
		t.Fatalf("Expected values were equal.\nactual=%#v\nexpected=%#v in %s", actual, expected, string(debug.Stack()))
	}
}

func AssertFuncEqual(t Fataler, actual, expected interface{}) {
	if !(reflect.ValueOf(actual).Pointer() == reflect.ValueOf(expected).Pointer()) {
		t.Fatalf("Expected functions were equal.\nactual=%#v\nexpected=%#v in %s", actual, expected, string(debug.Stack()))
	}
}

func AssertError(t Fataler, actual error, reg *regexp.Regexp) {
	if actual == nil || reg == nil {
		t.Fatalf("Error or regexp is nil.\nactual=%#v\nregexp=%#v in %s", actual, reg, string(debug.Stack()))
	}
	if !reg.MatchString(actual.(error).Error()) {
		t.Fatalf("Expected but got.\nactual=%#v\nregexp=%#v in %s", actual, reg, string(debug.Stack()))
	}
}

func AssertStringContains(t Fataler, actual string, contains string) {
	if !strings.Contains(actual, contains) {
		t.Fatalf("Expected string to contain substring \nactual=%#v\nexpected=%#v in %s", actual, contains, string(debug.Stack()))
	}
}

func PanicAssertStringContains(t Fataler, s string) {
	if r := recover(); r != nil {
		err := r.(error)
		AssertStringContains(t, err.Error(), s)
	} else {
		t.Fatalf("panic expected")
	}
}
