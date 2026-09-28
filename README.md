# ex_02

Coding exercise. Backpressure example.

Clone the repository:
```shell
git clone https://github.com/yan-khonski-it/ex_02.git
```

Run the tests with:

```shell
go test ./...
```

Run the application with:

```shell
go run .
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

### Output

A typical run processes approximately 117 tasks and drops approximately
883, taking about 44 seconds.