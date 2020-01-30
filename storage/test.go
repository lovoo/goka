package storage

import (
	"reflect"
	"regexp"
	"runtime/debug"
	"strings"
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
	if actual := recover(); actual != nil {
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("Expected values were equal.\nactual=%#v\nexpected=%#v in %s", actual, expected, string(debug.Stack()))
		}
	} else {
		t.Errorf("panic expected")
		t.FailNow()
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

func assertError(t *testing.T, actual error, reg *regexp.Regexp) {
	if actual == nil || reg == nil {
		t.Fatalf("Error or regexp is nil.\nactual=%#v\nregexp=%#v in %s", actual, reg, string(debug.Stack()))
	}
	if !reg.MatchString(actual.(error).Error()) {
		t.Fatalf("Expected but got.\nactual=%#v\nregexp=%#v in %s", actual, reg, string(debug.Stack()))
	}
}

func assertStringContains(t *testing.T, actual string, contains string) {
	if !strings.Contains(actual, contains) {
		t.Fatalf("Expected string to contain substring \nactual=%#v\nexpected=%#v in %s", actual, contains, string(debug.Stack()))
	}
}

func panicAssertStringContains(t *testing.T, s string) {
	if r := recover(); r != nil {
		err := r.(error)
		assertStringContains(t, err.Error(), s)
	} else {
		t.Errorf("panic expected")
		t.FailNow()
	}
}
