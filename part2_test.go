package main

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencareer/interview-excercises/ex_02/lib"
)

func TestProcessTasksWithWorkerPoolBoundsConcurrency(t *testing.T) {
	const (
		workerCount = 3
		taskCount   = 12
	)

	tasks := make(chan lib.Task, taskCount)
	started := make(chan struct{}, taskCount)
	release := make(chan struct{})
	releaseAll := sync.OnceFunc(func() { close(release) })
	defer releaseAll()

	var active atomic.Int64
	var maxActive atomic.Int64

	for i := 0; i < taskCount; i++ {
		tasks <- lib.Task{Do: func() error {
			current := active.Add(1)
			defer active.Add(-1)

			for {
				currentMax := maxActive.Load()
				if current <= currentMax ||
					maxActive.CompareAndSwap(currentMax, current) {
					break
				}
			}

			started <- struct{}{}
			<-release
			return nil
		}}
	}
	close(tasks)

	var processed uint64
	var err error
	done := make(chan struct{})
	go func() {
		defer close(done)
		processed, err = processTasksWithWorkerPool(tasks, workerCount)
	}()

	for i := 0; i < workerCount; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("only %d of %d workers started", i, workerCount)
		}
	}

	select {
	case <-started:
		t.Fatalf("more than %d tasks ran concurrently", workerCount)
	case <-time.After(50 * time.Millisecond):
	}

	releaseAll()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker pool did not finish")
	}

	if err != nil {
		t.Fatalf("processTasksWithWorkerPool() error = %v", err)
	}
	if processed != taskCount {
		t.Fatalf("processed = %d, want %d", processed, taskCount)
	}

	if got := maxActive.Load(); got != workerCount {
		t.Fatalf("maximum concurrency = %d, want %d", got, workerCount)
	}
}

func TestProcessTasksWithWorkerPoolRejectsInvalidWorkerCount(t *testing.T) {
	tasks := make(chan lib.Task)
	close(tasks)

	for _, workerCount := range []int{0, -1} {
		if _, err := processTasksWithWorkerPool(tasks, workerCount); err == nil {
			t.Errorf(
				"workerCount = %d: error = nil, want an error",
				workerCount,
			)
		}
	}
}

func TestProcessTasksWithWorkerPoolAllowsMoreWorkersThanTasks(t *testing.T) {
	const (
		workerCount = 5
		taskCount   = 2
	)

	tasks := make(chan lib.Task, taskCount)
	for i := 0; i < taskCount; i++ {
		tasks <- lib.Task{Do: func() error { return nil }}
	}
	close(tasks)

	type result struct {
		processed uint64
		err       error
	}

	done := make(chan result, 1)
	go func() {
		processed, err := processTasksWithWorkerPool(tasks, workerCount)
		done <- result{processed: processed, err: err}
	}()

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("processTasksWithWorkerPool() error = %v", got.err)
		}
		if got.processed != taskCount {
			t.Fatalf("processed = %d, want %d", got.processed, taskCount)
		}
	case <-time.After(time.Second):
		t.Fatal("worker pool did not finish")
	}
}
