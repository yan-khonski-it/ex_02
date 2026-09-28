package main

import (
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

	var active int64
	var maxActive int64

	for i := 0; i < taskCount; i++ {
		tasks <- lib.Task{Do: func() error {
			current := atomic.AddInt64(&active, 1)
			for {
				currentMax := atomic.LoadInt64(&maxActive)
				if current <= currentMax ||
					atomic.CompareAndSwapInt64(&maxActive, currentMax, current) {
					break
				}
			}

			started <- struct{}{}
			<-release
			atomic.AddInt64(&active, -1)
			return nil
		}}
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

	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()

	for i := 0; i < workerCount; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("workers did not start in time")
		}
	}

	select {
	case <-started:
		t.Fatalf("more than %d tasks started concurrently", workerCount)
	case <-time.After(10 * time.Millisecond):
	}

	close(release)
	released = true

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("processTasksWithWorkerPool() error = %v", got.err)
		}
		if got.processed != taskCount {
			t.Fatalf(
				"processed = %d, want %d",
				got.processed,
				taskCount,
			)
		}
	case <-time.After(time.Second):
		t.Fatal("worker pool did not finish in time")
	}

	if got := atomic.LoadInt64(&maxActive); got != workerCount {
		t.Fatalf("maximum concurrency = %d, want %d", got, workerCount)
	}
}

func TestProcessTasksWithWorkerPoolRejectsInvalidWorkerCount(t *testing.T) {
	tasks := make(chan lib.Task)
	close(tasks)

	if _, err := processTasksWithWorkerPool(tasks, 0); err == nil {
		t.Fatal("processTasksWithWorkerPool() error = nil, want an error")
	}
}
