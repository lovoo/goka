package main

import (
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/lovoo/goka"
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
	kafkaMock := goka.NewKafkaMock(t, "consume-scalar")
	proc, err := createProcessor(nil, kafkaMock.ProcessorOptions()...)

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
	if kafkaMock.ValueForKey("foo") != nil {
		t.Errorf("state was not initially empty")
	}

	// send the message twice
	kafkaMock.Consume("scalar", "foo", msg)
	kafkaMock.Consume("scalar", "foo", msg)

	value := kafkaMock.ValueForKey("foo").(int64)

	if value != 2 {
		t.Errorf("Expected value %d, got %d", 2, value)
	}

	proc.Stop()
	<-done
}
