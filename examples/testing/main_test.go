package main

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/mock"
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
	kafkaMock := mock.NewKafkaMock(t, "consume-scalar")
	proc, err := createProcessor(nil, goka.WithTester(kafkaMock))

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
	if val := kafkaMock.ValueForKey("foo"); val != nil {
		t.Errorf("state was not initially empty: %v", val)
	}

	// send the message twice
	kafkaMock.Consume("scalar", "foo", msg)
	kafkaMock.Consume("scalar", "foo", msg)

	value := string(kafkaMock.ValueForKey("foo").([]byte))
	fmt.Printf("%v\n", value)

	if value != "2" {
		t.Errorf("Expected value %s, got %s", "2", value)
	}

	proc.Stop()
	<-done
}
