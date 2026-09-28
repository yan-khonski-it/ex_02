package main

import (
	"errors"
	"testing"
	"time"

	"github.com/opencareer/interview-excercises/ex_02/lib"
)

func TestProcessTasksContinuesAfterFailure(t *testing.T) {
	tasks := make(chan lib.Task, 3)
	afterFailureRan := false

	tasks <- lib.Task{Do: func() error { return nil }}
	tasks <- lib.Task{Do: func() error { return errors.New("failed") }}
	tasks <- lib.Task{Do: func() error {
		afterFailureRan = true
		return nil
	}}
	close(tasks)

	processed := processTasks(tasks)

	if processed != 2 {
		t.Fatalf("processed = %d, want 2", processed)
	}
	if !afterFailureRan {
		t.Fatal("processing stopped after a failed task")
	}
}

func TestProcessTasksWaitsUntilChannelIsClosed(t *testing.T) {
	tasks := make(chan lib.Task)
	result := make(chan uint64, 1)

	go func() {
		defer close(tasks)

		for i := 0; i < 3; i++ {
			time.Sleep(time.Millisecond)
			tasks <- lib.Task{Do: func() error { return nil }}
		}
	}()

	go func() {
		result <- processTasks(tasks)
	}()

	select {
	case got := <-result:
		if got != 3 {
			t.Fatalf("processed = %d, want 3", got)
		}
	case <-time.After(time.Second):
		t.Fatal("processTasks did not return after the channel was closed")
	}
}
