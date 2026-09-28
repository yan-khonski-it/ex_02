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
go run . -part=1
go run . -part=2
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

## Part 2 — Bounded worker pool

Part 2 uses N workers reading from the library's existing buffered task
channel. This provides enough aggregate throughput for the simulated arrival
rate while bounding both active work and queued tasks.

The fixed worker count prevents unbounded goroutine and resource growth, while
the existing 50-task buffer absorbs short bursts. Task completion order is no
longer guaranteed, and sustained input above the worker pool's capacity can
still fill the buffer and cause drops.