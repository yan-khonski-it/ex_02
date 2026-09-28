package main

import (
	"fmt"
	"log"

	"github.com/opencareer/interview-excercises/ex_02/lib"
)

// runPart1 processes queued tasks and reports processed and dropped totals.
func runPart1() error {
	taskQueue, err := lib.NewTaskQueueConn()
	if err != nil {
		return fmt.Errorf("connect to task queue: %w", err)
	}
	defer taskQueue.Shutdown()

	processed := processTasks(taskQueue.Listen())

	fmt.Printf(
		"processed: %d, dropped: %d\n",
		processed,
		taskQueue.TasksDropped(),
	)

	return nil
}

// processTasks executes tasks sequentially until the channel is closed and
// drained, and returns the number of tasks that completed successfully.
func processTasks(tasks <-chan lib.Task) uint64 {
	var processed uint64

	for task := range tasks {
		if err := task.Do(); err != nil {
			log.Printf("execute task: %v", err)
			continue
		}

		processed++
	}

	return processed
}
