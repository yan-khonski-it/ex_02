package main

import (
	"fmt"
	"sync"

	"github.com/opencareer/interview-excercises/ex_02/lib"
)

type taskQueue interface {
	Listen() <-chan lib.Task
	Shutdown()
	TasksDropped() uint64
}

// processingResult is the result of processing tasks with graceful cancellation.
type processingResult struct {
	processed uint64
	dropped   uint64
	cancelled bool
}

// workerPoolResult is the result of processing tasks with a bounded worker pool.
// Did the worker pool finish, how many tasks succeeded, and did processing return an error?
type workerPoolResult struct {
	processed uint64
	err       error
}

// runPart3 connects to the queue and reports graceful cancellation results.
func runPart3() error {
	fmt.Println("Running part 3: graceful cancellation of task processing.")

	queue, err := lib.NewTaskQueueConn()
	if err != nil {
		return fmt.Errorf("connect to task queue: %w", err)
	}

	result, err := processPart3(
		&queue,
		workersCount,
		listenCancellation(),
	)
	if err != nil {
		return fmt.Errorf("process tasks: %w", err)
	}

	status := "completed"
	if result.cancelled {
		status = "cancelled"
	}

	fmt.Printf(
		"status: %s, processed: %d, dropped: %d\n",
		status,
		result.processed,
		result.dropped,
	)

	return nil
}

// processPart3 coordinates queue shutdown and waits for accepted tasks to finish.
func processPart3(
	queue taskQueue,
	workerCount int,
	cancel <-chan bool,
) (processingResult, error) {
	shutdown := sync.OnceFunc(queue.Shutdown)
	defer shutdown()

	if workerCount <= 0 {
		return processingResult{}, fmt.Errorf(
			"worker count must be positive: %d",
			workerCount,
		)
	}

	tasks := queue.Listen()
	processingDone := make(chan workerPoolResult, 1)

	go func() {
		processed, err := processTasksWithWorkerPool(tasks, workerCount)
		processingDone <- workerPoolResult{
			processed: processed,
			err:       err,
		}
	}()

	result := processingResult{}
	var workerResult workerPoolResult
	processingFinished := false

	select {
	case workerResult = <-processingDone:
		processingFinished = true
		select {
		case <-cancel:
			result.cancelled = true
		default:
		}
	case <-cancel:
		result.cancelled = true
	}

	shutdown()
	if !processingFinished {
		workerResult = <-processingDone
	}

	result.processed = workerResult.processed
	result.dropped = queue.TasksDropped()

	if workerResult.err != nil {
		return result, workerResult.err
	}

	return result, nil
}
