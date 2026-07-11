# TaskRunner

TaskRunner is a high-performance Go library for distributed, reliable task
processing built on Redis Streams. It provides horizontal scalability, leader
election, delayed scheduling, unique jobs, and rich timing metrics with a simple
API.

## Highlights

- **Distributed & Scalable**: Add instances to scale throughput horizontally.
- **Reliable**: Visibility timeout with heartbeats prevents double-processing;
  pending reclaims; retries.
- **Simple API**: Register tasks and dispatch payloads with minimal boilerplate.
- **Delayed Scheduling**: Schedule jobs for future execution via Redis ZSET +
  Streams.
- **Unique Jobs**: Enforce de-duplication across a time window via distributed
  locks.
- **Leader Election**: Cooperative leadership for scheduling and maintenance
  loops.
- **Observability**: Per-task timing metrics and queue statistics.

## Installation

```bash
go get github.com/soroosh-tanzadeh/taskrunner
```

Requires Go 1.24.9+ and Redis 6+. For local development and tests, the project
uses `miniredis` to simulate Redis in-memory.

## Quickstart (Task Queue)

```go
package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/soroosh-tanzadeh/taskrunner/redisstream"
	"github.com/soroosh-tanzadeh/taskrunner/runner"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", DB: 0, PoolSize: 50})
	queue := redisstream.NewRedisStreamMessageQueueWithOptions(
		rdb,
		redisstream.WithPrefix("example"),
		redisstream.WithQueue("tasks"),
		redisstream.WithReClaimDelay(30*time.Second),
		redisstream.WithDeleteOnAck(true),
	)

	tr := runner.NewTaskRunner(runner.TaskRunnerConfig{
		BatchSize:       10,
		ConsumerGroup:   "example",
		ConsumersPrefix: "default",
		NumWorkers:      8,
		NumFetchers:     4,
		LongQueueHook: func(s runner.Stats) { fmt.Printf("%+v\n", s) },
		LongQueueThreshold: 30 * time.Second,
	}, rdb, queue)

	tr.RegisterTask(&runner.Task{
		Name:     "exampletask",
		MaxRetry: 5,
		Action: func(ctx context.Context, payload any) error {
			fmt.Printf("Hello from example task %v\n", payload)
			return nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() { defer wg.Done(); _ = tr.Start(ctx) }()

	for i := 0; i < 100; i++ {
		_ = tr.Dispatch(context.Background(), "exampletask", strconv.Itoa(i))
	}

	time.Sleep(2 * time.Second)
	cancel()
	wg.Wait()
}
```

## Delayed Task Scheduler

Run a cooperative scheduler that enqueues due jobs from a ZSET into the stream.
The scheduler ticks every 5 seconds; delays shorter than this will be rounded up
to the next tick.

```go
ctx, cancel := context.WithCancel(context.Background())
// Start workers
go tr.Start(ctx)
// Start scheduler with a batch size for due jobs
go tr.StartDelayedSchedule(ctx, 1000)

// Dispatch delayed jobs
_ = tr.DispatchDelayed(context.Background(), "exampletask", "I run in ~5s", 5*time.Second)
_ = tr.DispatchDelayed(context.Background(), "exampletask", "I run in ~10s", 10*time.Second)

// ... later
cancel()
```

Notes:

- The scheduler respects leader election; only the leader instance enqueues due
  jobs.
- Use `ScheduleFor(ctx, taskName, payload, time.Time)` to schedule for an
  absolute time.

## Unique Jobs

Prevent duplicate enqueues of the same job (optionally scoped by a custom key)
for a specified window.

```go
tr.RegisterTask(&runner.Task{
	Name:     "sendEmail",
	MaxRetry: 3,
	Unique:   true,
	UniqueFor: 60, // seconds
	UniqueKey: func(payload any) string { return payload.(string) }, // e.g., email ID
	Action: func(ctx context.Context, payload any) error { return nil },
})

// First dispatch succeeds
_ = tr.Dispatch(context.Background(), "sendEmail", "order-123")
// Second dispatch within 60s will fail with runner.ErrTaskAlreadyDispatched
err := tr.Dispatch(context.Background(), "sendEmail", "order-123")
```

## Configuration

TaskRunner is configured via `runner.TaskRunnerConfig`:

- **Host**: Optional, defaults to hostname; used in metrics and identity.
- **BatchSize**: Number of messages fetched per read per fetcher.
- **ConsumerGroup**: Redis Streams consumer group name.
- **ConsumersPrefix**: Prefix for consumer names.
- **NumWorkers**: Concurrent workers processing messages.
- **Worker tuning (optional)**: Automatically adjust worker pool size to keep
  the estimated queue wait time (from `PredictedWaitTime`) within a target band.
  Enable it by setting:
  - **MinWorkers**: Lower bound for automatic tuning.
  - **MaxWorkers**: Upper bound for automatic tuning.
  - **DesiredWaitTime**: Target predicted wait time (as a `time.Duration`).
  - **DesiredWaitTimeTolerance**: Allowed deviation around the target.
- **NumFetchers**: Concurrent fetchers reading from the stream (each reads
  `BatchSize`).
- **FailedTaskHandler**: Callback when a task exhausts retries.
- **LongQueueHook**: Periodic timing/queue stats callback; frequency set by
  `LongQueueThreshold`.
- **LongQueueThreshold**: Duration that influences the cadence of timing
  aggregation.
- **BlockDuration**: Stream read block duration (defaults to 5s).
- **MetricsResetInterval**: Interval to reset timing metrics (default 24h; set 0
  to disable).
- ~~ReplicationFactor~~: Deprecated; maintained for backward compatibility only.

Redis Stream queue configuration via options on
`NewRedisStreamMessageQueueWithOptions`:

- `WithPrefix(prefix string)`
- `WithQueue(queue string)`
- `WithReClaimDelay(d time.Duration)` – reclaim pending messages after `d`.
- `WithDeleteOnAck(enabled bool)`
- `WithRedisVersion(version string)` – override auto-detected version if needed.

## Worker tuning tips (PredictedWaitTime)

`PredictedWaitTime` is an estimate (not actual measured latency) computed from:
average task execution time, current queue length, and the current worker pool
capacity.

When worker tuning is enabled, the leader periodically calls into the timing
aggregator and adjusts the worker pool capacity (`MinWorkers`..`MaxWorkers`) so
that:

`DesiredWaitTime ± DesiredWaitTimeTolerance`

To tune in practice:

1. Start with a realistic wait target:
   - Set `DesiredWaitTime` to the maximum queueing delay you can tolerate in
     normal operation.
2. Add tolerance to avoid oscillation:
   - Use a non-zero `DesiredWaitTimeTolerance` (for example 20% to 50% of
     `DesiredWaitTime`).
3. Clamp with min/max:
   - `MinWorkers` prevents under-provisioning during low load.
   - `MaxWorkers` protects your CPU/memory budget during spikes.
4. Control how fast the system reacts:
   - Tuning runs on the same schedule as the timing aggregator loop.
   - That loop runs every `LongQueueThreshold/2` (when
     `LongQueueThreshold > 0`), otherwise every 1 minute.
5. Watch `LongQueueHook`:
   - If you already use `LongQueueHook`, it can help you validate whether
     predicted wait time is staying in band.

Example configuration:

```go
tr := runner.NewTaskRunner(runner.TaskRunnerConfig{
    NumWorkers: 8, // used only when tuning is disabled
    MinWorkers: 2,
    MaxWorkers: 32,
    DesiredWaitTime: 500 * time.Millisecond,
    DesiredWaitTimeTolerance: 200 * time.Millisecond,
    LongQueueThreshold: 30 * time.Second, // controls tuning frequency
}, rdb, queue)
```

## Examples

See runnable examples and tests under `examples/`:

- `examples/simple`: Basic queue usage
- `examples/scheduler`: Delayed tasks scheduler
- `examples/unique`: Unique jobs

## Testing

Run the full test suite:

```bash
go test ./...
```

The examples are covered by tests using `miniredis` so they run without a real
Redis server.

## Contributing

Contributions are welcome! Please:

- Open an issue to discuss substantial changes.
- Write tests for new features and ensure `go test ./...` passes.
- Feature branches name must be in this format: `feature/{feature-name}`
- Follow idiomatic Go style and **keep APIs small and focused**.

## License

GPL-3.0. See `LICENSE` for details.
