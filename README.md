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
go run . -part=3
```

## Part 1 — Single-threaded task processing

The first implementation processes tasks sequentially:

1. Create a connection to the external task queue.
2. Call `Listen()` to receive tasks through a channel.
3. Execute each task by calling `Task.Do()`.
4. Count tasks that complete successfully.
5. Continue processing if an individual task fails.
6. Report the number of successfully processed and dropped tasks.

The task-processing loop is extracted into `processTasks`, which accepts a receive-only task channel. Task execution
remains single-threaded: the next task is not started until the current task finishes.

The queue connection is shut down with a deferred `Shutdown()` call. Processing ends after the library closes the task
channel and all buffered tasks have been consumed.

### Output

A typical run processes approximately 117 tasks and drops approximately
883, taking about 44 seconds.

## Part 2 — Bounded worker pool

Part 2 uses 20 workers reading from the channel returned by `Listen()`. The
channel provides bounded buffering for short bursts while the worker pool
bounds concurrent task execution. The fixed pool was selected because it is
the simplest approach that handles the simulated load without unbounded
resource growth.

### Output

A typical Part 2 run processes all 1,000 tasks with no drops in about 25.4
seconds:

```text
processed: 1000, dropped: 0
```

### Alternatives

The results below are illustrative measurements from separate prototypes. The
slow-task scenario doubles execution time from 375 ms to 750 ms while keeping
the 25 ms arrival interval unchanged.

| Approach                                                              |      Normal tasks (375 ms) |                          Slower tasks (750 ms) |                  Concurrency limit |
|-----------------------------------------------------------------------|---------------------------:|-----------------------------------------------:|-----------------------------------:|
| Single loop (Part 1)                                                  | 117 processed, 883 dropped |                                   Not measured |                                  1 |
| **Fixed pool of 20 workers**                                          | 1,000 processed, 0 dropped |                     718 processed, 282 dropped |                                 20 |
| Semaphore, with a permit acquired before starting each task goroutine | 1,000 processed, 0 dropped |                     719 processed, 281 dropped |                                 20 |
| One goroutine per task                                                | 1,000 processed, 0 dropped |   1,000 processed, 0 dropped; up to 31 running |                          Unbounded |
| Unbounded application queue with 20 workers                           | 1,000 processed, 0 dropped |  1,000 processed, 0 dropped; up to 332 waiting | 20 running; waiting work unbounded |
| Pool growing from 20 to at most 60 workers                            | 1,000 processed, 0 dropped | 1,000 processed, 0 dropped; grew to 35 workers |                                 60 |

At normal speed, even the unbounded prototype needed only about 16 concurrent
tasks, so all approaches that kept up produced similar results. Their
differences became visible when task execution slowed.

#### Tradeoffs

- **Fixed pool:** Simple and bounded, but cannot adapt when tasks slow down.
- **Semaphore:** Enforces the same limit without idle workers, but requires a
  goroutine per task and separate completion tracking. Acquire the permit before
  starting the goroutine to avoid unbounded blocked goroutines.
- **Unbounded goroutines:** Drains input quickly but converts backlog into
  unlimited goroutines and downstream load, violating the concurrency limit.
- **Extra application queue:** Moves backlog into memory. A bounded queue only
  delays overload; an unbounded queue risks growing memory use and latency.
- **Growing pool:** Adapts to slower tasks up to a limit, but adds scaling
  complexity and can overload downstream services.

Batching could reduce per-task cost, but `Task.Do()` is a black box.
Horizontal scaling is not modeled by this exercise. Increasing a finite buffer
still only delays overload; a real no-loss guarantee requires producer
back-pressure or a durable queue with acknowledgements and retries.

### Real-world limitations

The fixed pool is good enough for the simulated load and short bursts, but not
for sustained overload, slower or stuck dependencies, process crashes, or
guaranteed task delivery.

- **Slower or stuck tasks:** In the representative 750 ms scenario, 282 of
  1,000 tasks (28.2%) were dropped. A bounded growing pool could help if the
  downstream service can safely handle the additional concurrency.
- **Failed tasks:** An error returned by `Task.Do()` is logged, but the task is
  not retried and is not included in the library's dropped count.
- **Delivery guarantees:** When available buffering is exhausted, the queue
  discards new arrivals. The application has no acknowledgement or recovery
  mechanism, so a crash can lose delivered work or leave running tasks
  partially completed. Reliable delivery requires producer back-pressure or
  retries, durable storage with acknowledgements, and idempotent task handlers
  because unfinished tasks may be delivered more than once. Expiration and
  dead-letter policies are also needed for tasks that cannot be completed.
- **Panics:** An unrecovered task panic terminates the process. Recovering at
  the task boundary would require an explicit safety policy, stack-trace
  logging, and handling for retries, dead-lettering, and partial side effects.
- **Ordering and downstream limits:** Concurrent execution does not preserve
  completion order, and the worker count must respect database, API, and other
  downstream capacity limits.

## Part 3 — Graceful cancellation

Part 3 reuses the bounded worker pool and listens for the cancellation signal
provided by `listenCancellation()`. The queue listener is started before
cancellation can trigger shutdown, and `Shutdown()` is guarded so the
connection is closed exactly once on cancellation, natural completion, or an
error.

On cancellation, the service stops fetching new tasks and waits for the worker
pool to finish. Workers intentionally drain tasks already delivered by the
queue because the library provides no acknowledgement or requeue operation.
This avoids abandoning accepted work, at the cost of potentially increasing
shutdown time.

### Output

With the provided six-second cancellation signal, a typical run reports:

```text
status: cancelled, processed: 240, dropped: 0
```

The cancelled status is significant: `dropped: 0` only means no fetched task
was rejected before shutdown. It does not mean that all tasks for the day were
fetched.

Part 3 can stop fetching and wait for running tasks, but it cannot interrupt a
stuck `Task.Do()` call because tasks do not accept a cancellation context.
Production task handlers would need cancellation or timeout support to place a
bound on graceful-shutdown duration.
