package main

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencareer/interview-excercises/ex_02/lib"
)

// fakeTaskQueue models the public contract that Shutdown eventually closes
// the channel returned by Listen.
type fakeTaskQueue struct {
	tasks                     chan lib.Task
	dropped                   uint64
	closeTasks                func()
	signalShutdown            func()
	shutdownCalled            chan struct{}
	listenCalls               atomic.Int32
	shutdownCalls             atomic.Int32
	shutdownBeforeListen      atomic.Bool
	droppedReadBeforeShutdown atomic.Bool
	shutdownReturned          atomic.Bool
	onShutdown                func()
}

func newFakeTaskQueue(capacity int, dropped uint64) *fakeTaskQueue {
	queue := &fakeTaskQueue{
		tasks:          make(chan lib.Task, capacity),
		dropped:        dropped,
		shutdownCalled: make(chan struct{}),
	}
	queue.closeTasks = sync.OnceFunc(func() { close(queue.tasks) })
	queue.signalShutdown = sync.OnceFunc(func() {
		close(queue.shutdownCalled)
	})
	return queue
}

func (q *fakeTaskQueue) Listen() <-chan lib.Task {
	q.listenCalls.Add(1)
	return q.tasks
}

func (q *fakeTaskQueue) Shutdown() {
	if q.listenCalls.Load() == 0 {
		q.shutdownBeforeListen.Store(true)
	}

	q.shutdownCalls.Add(1)
	q.signalShutdown()
	if q.onShutdown != nil {
		q.onShutdown()
	}
	q.closeTasks()
	q.shutdownReturned.Store(true)
}

func (q *fakeTaskQueue) TasksDropped() uint64 {
	if !q.shutdownReturned.Load() {
		q.droppedReadBeforeShutdown.Store(true)
	}
	return q.dropped
}

func (q *fakeTaskQueue) finish() {
	q.closeTasks()
}

type part3TestOutcome struct {
	result processingResult
	err    error
}

func processPart3Async(
	queue taskQueue,
	workerCount int,
	cancel <-chan bool,
) <-chan part3TestOutcome {
	done := make(chan part3TestOutcome, 1)
	go func() {
		result, err := processPart3(queue, workerCount, cancel)
		done <- part3TestOutcome{result: result, err: err}
	}()
	return done
}

func awaitPart3(
	t *testing.T,
	done <-chan part3TestOutcome,
) part3TestOutcome {
	t.Helper()

	select {
	case outcome := <-done:
		return outcome
	case <-time.After(time.Second):
		t.Fatal("Part 3 processing did not finish")
		return part3TestOutcome{}
	}
}

func assertPart3QueueLifecycle(t *testing.T, queue *fakeTaskQueue) {
	t.Helper()

	if got := queue.listenCalls.Load(); got != 1 {
		t.Errorf("Listen() calls = %d, want 1", got)
	}
	if got := queue.shutdownCalls.Load(); got != 1 {
		t.Errorf("Shutdown() calls = %d, want 1", got)
	}
	if queue.shutdownBeforeListen.Load() {
		t.Error("Shutdown() was called before Listen()")
	}
	if queue.droppedReadBeforeShutdown.Load() {
		t.Error("TasksDropped() was read before Shutdown()")
	}
}

func TestProcessPart3NaturalCompletion(t *testing.T) {
	queue := newFakeTaskQueue(2, 3)
	queue.tasks <- lib.Task{Do: func() error { return nil }}
	queue.tasks <- lib.Task{Do: func() error { return nil }}
	queue.finish()

	outcome := awaitPart3(
		t,
		processPart3Async(queue, 2, nil),
	)

	if outcome.err != nil {
		t.Fatalf("processPart3() error = %v", outcome.err)
	}
	if outcome.result.cancelled {
		t.Fatal("cancelled = true, want false")
	}
	if outcome.result.processed != 2 {
		t.Fatalf("processed = %d, want 2", outcome.result.processed)
	}
	if outcome.result.dropped != 3 {
		t.Fatalf("dropped = %d, want 3", outcome.result.dropped)
	}
	assertPart3QueueLifecycle(t, queue)
}

func TestProcessPart3CancellationWaitsAndDrainsAcceptedTasks(
	t *testing.T,
) {
	queue := newFakeTaskQueue(2, 4)
	taskStarted := make(chan struct{})
	release := make(chan struct{})
	releaseTask := sync.OnceFunc(func() { close(release) })
	defer releaseTask()

	var queuedTaskRan atomic.Bool
	queue.tasks <- lib.Task{Do: func() error {
		close(taskStarted)
		<-release
		return nil
	}}
	queue.tasks <- lib.Task{Do: func() error {
		queuedTaskRan.Store(true)
		return nil
	}}

	cancel := make(chan bool)
	cancelRun := sync.OnceFunc(func() { close(cancel) })
	defer cancelRun()

	done := processPart3Async(queue, 1, cancel)

	select {
	case <-taskStarted:
	case <-time.After(time.Second):
		t.Fatal("task did not start")
	}

	cancelRun()

	select {
	case <-queue.shutdownCalled:
	case <-time.After(time.Second):
		t.Fatal("Shutdown() was not called")
	}

	select {
	case <-done:
		t.Fatal("processPart3() returned while a task was still running")
	case <-time.After(25 * time.Millisecond):
	}

	releaseTask()
	outcome := awaitPart3(t, done)

	if outcome.err != nil {
		t.Fatalf("processPart3() error = %v", outcome.err)
	}
	if !outcome.result.cancelled {
		t.Fatal("cancelled = false, want true")
	}
	if outcome.result.processed != 2 {
		t.Fatalf("processed = %d, want 2", outcome.result.processed)
	}
	if outcome.result.dropped != 4 {
		t.Fatalf("dropped = %d, want 4", outcome.result.dropped)
	}
	if !queuedTaskRan.Load() {
		t.Fatal("task accepted before shutdown was not processed")
	}
	assertPart3QueueLifecycle(t, queue)
}

func TestProcessPart3ListensBeforeImmediateCancellation(t *testing.T) {
	queue := newFakeTaskQueue(0, 0)
	cancel := make(chan bool)
	close(cancel)

	outcome := awaitPart3(
		t,
		processPart3Async(queue, 1, cancel),
	)

	if outcome.err != nil {
		t.Fatalf("processPart3() error = %v", outcome.err)
	}
	if !outcome.result.cancelled {
		t.Fatal("cancelled = false, want true")
	}
	assertPart3QueueLifecycle(t, queue)
}

func TestProcessPart3WaitsForShutdownAndProcessesFinalAcceptedTask(
	t *testing.T,
) {
	queue := newFakeTaskQueue(0, 5)
	shutdownRelease := make(chan struct{})
	finalTaskRan := make(chan struct{})

	queue.onShutdown = func() {
		<-shutdownRelease
		queue.tasks <- lib.Task{Do: func() error {
			close(finalTaskRan)
			return nil
		}}
	}

	cancel := make(chan bool)
	close(cancel)
	done := processPart3Async(queue, 1, cancel)

	select {
	case <-queue.shutdownCalled:
	case <-time.After(time.Second):
		t.Fatal("Shutdown() was not called")
	}

	select {
	case <-done:
		t.Fatal("processPart3() returned before Shutdown() finished")
	case <-time.After(25 * time.Millisecond):
	}

	close(shutdownRelease)
	outcome := awaitPart3(t, done)

	if outcome.err != nil {
		t.Fatalf("processPart3() error = %v", outcome.err)
	}
	if !outcome.result.cancelled {
		t.Fatal("cancelled = false, want true")
	}
	if outcome.result.processed != 1 {
		t.Fatalf("processed = %d, want 1", outcome.result.processed)
	}
	if outcome.result.dropped != 5 {
		t.Fatalf("dropped = %d, want 5", outcome.result.dropped)
	}

	select {
	case <-finalTaskRan:
	default:
		t.Fatal("task delivered during shutdown was not processed")
	}

	assertPart3QueueLifecycle(t, queue)
}

func TestProcessPart3InvalidWorkerCountShutsDownQueue(t *testing.T) {
	queue := newFakeTaskQueue(0, 0)

	if _, err := processPart3(queue, 0, nil); err == nil {
		t.Fatal("processPart3() error = nil, want an error")
	}
	if got := queue.shutdownCalls.Load(); got != 1 {
		t.Fatalf("Shutdown() calls = %d, want 1", got)
	}
}
