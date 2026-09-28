package main

import (
	"fmt"

	"github.com/opencareer/interview-excercises/ex_02/lib"
)

// workersCount limits concurrency; 20 workers can keep up with the simulated task rate.
const workersCount = 20

// runPart2 processes queued tasks with a bounded worker pool.
func runPart2() error {
	fmt.Println("Running part 2: bounded worker pool processing of tasks.")

	taskQueue, err := lib.NewTaskQueueConn()
	if err != nil {
		return fmt.Errorf("connect to task queue: %w", err)
	}
	defer taskQueue.Shutdown()

	processed, err := processTasksWithWorkerPool(
		taskQueue.Listen(),
		workersCount,
	)
	if err != nil {
		return fmt.Errorf("process tasks: %w", err)
	}

	fmt.Printf(
		"processed: %d, dropped: %d\n",
		processed,
		taskQueue.TasksDropped(),
	)

	return nil
}

// processTasksWithWorkerPool executes tasks with at most workerCount workers.
func processTasksWithWorkerPool(
	tasks <-chan lib.Task,
	workerCount int,
) (uint64, error) {
	if workerCount <= 0 {
		return 0, fmt.Errorf("worker count must be positive: %d", workerCount)
	}

	workerResults := make(chan uint64, workerCount)

	// Workers share the task channel, so each task is handled once by whichever worker is ready.
	for i := 0; i < workerCount; i++ {
		go func() {
			workerResults <- processTasks(tasks)
		}()
	}

	var processed uint64
	for i := 0; i < workerCount; i++ {
		processed += <-workerResults
	}

	return processed, nil
}
