# ex_02

Run the application with:

```shell
go run .
```

Run the tests with:

```shell
go test ./...
```

## Part 1 — Single-threaded task processing

The first implementation processes tasks sequentially:

1. Create a connection to the external task queue.
2. Call `Listen()` to receive tasks through a channel.
3. Execute each task by calling `Task.Do()`.
4. Count tasks that complete successfully.
5. Continue processing if an individual task fails.
6. Report the number of successfully processed and dropped tasks.

The task-processing loop is extracted into `processTasks`, which accepts a receive-only task channel. Task execution remains single-threaded: the next task is not started until the current task finishes.

The queue connection is shut down with a deferred `Shutdown()` call. Processing ends after the library closes the task channel and all buffered tasks have been consumed.

### Expected behavior

A typical run processes approximately 117 tasks and drops approximately
883, taking about 44 seconds. Exact counts may vary slightly depending on
scheduling, but the processed and dropped counts total 1,000.

The queue receives one task every 25 ms (40 per second), while each task
takes 375 ms to execute. The single-threaded consumer can therefore process
only about 2.67 tasks per second. Because the queue holds only 50 waiting
tasks, its buffer fills quickly and subsequent tasks are dropped.