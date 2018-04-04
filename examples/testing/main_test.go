package main

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/tester"
)

func Test_ConsumeScalar(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctxMock := NewMockContext(ctrl)

	// test passing a wrong type (simulating wrong codec or unmarshalling errors)
	ctxMock.EXPECT().Fail(gomock.Any())
	ConsumeScalar(ctxMock, "invalid-type")

	// correct type, expects emitted value
	ctxMock.EXPECT().Emit(goka.Stream("sink"), "outgoing", int64(124))
	ConsumeScalar(ctxMock, int64(123))
}

func Test_ConsumeScalar_Integration(t *testing.T) {
	// ctrl := goka.NewMockController(t)
	// defer ctrl.Finish()
	tester := tester.New(t)
	proc, err := createProcessor(nil, goka.WithTester(tester))

	if err != nil {
		t.Fatalf("Error creating processor: %v", err)
	}
	done := make(chan int, 0)
	go func() {
		errs := proc.Start()
		if errs != nil {
			t.Errorf("Error executing processor: %v", err)
		}
		close(done)
	}()

	// we are creating a "1" as valid encoded message here for convenience
	msg := []byte(strconv.FormatInt(1, 10))

	// there is no initial value for key "foo"
	if val := tester.ValueForKey("foo"); val != nil {
		t.Errorf("state was not initially empty: %v", val)
	}

	// send the message twice
	tester.Consume("scalar-state", "foo", msg)
	tester.Consume("scalar-state", "foo", msg)

	fooByte, isByte := tester.ValueForKey("foo").([]byte)
	if !isByte {
		t.Errorf("state does not exist or is not []byte")
	}
	value := string(fooByte)
	fmt.Printf("%v\n", value)

	if value != "2" {
		t.Errorf("Expected value %s, got %s", "2", value)
	}

	tester.Consume("scalar", "somekey", msg)
	var (
		parsed   int64
		parseErr error
	)
	// expect that a value was emitted
	tester.ExpectEmit("sink", "outgoing", func(value []byte) {
		parsed, parseErr = strconv.ParseInt(string(value), 10, 64)
	})
	if parseErr != nil || parsed != 2 {
		panic(fmt.Errorf("parsing emitted message failed or had a wrong value (%d): %v", parsed, parseErr))
	}
	tester.Finish(true)

	proc.Stop()
	<-done
}
