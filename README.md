<div align="center">

# TaskRunner

**A high-performance Go library for distributed, reliable task processing on
Redis Streams.**

Horizontal scaling · Leader election · Delayed scheduling · Unique jobs ·
Automatic worker tuning · Per-task timing metrics

[![Go Reference](https://pkg.go.dev/badge/github.com/soroosh-tanzadeh/taskrunner.svg)](https://pkg.go.dev/github.com/soroosh-tanzadeh/taskrunner)
[![Go](https://github.com/soroosh-tanzadeh/taskrunner/actions/workflows/go.yml/badge.svg)](https://github.com/soroosh-tanzadeh/taskrunner/actions/workflows/go.yml)
[![License: GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-blue.svg)](LICENSE)

📚 **[Documentation](https://soroosh-tanzadeh.github.io/taskrunner-documents/)**
· 🚀
**[Getting Started](https://soroosh-tanzadeh.github.io/taskrunner-documents/getting-started/)**
· 🔧
**[API Reference](https://soroosh-tanzadeh.github.io/taskrunner-documents/api-reference/)**
· 📊
**[Benchmarks](https://soroosh-tanzadeh.github.io/taskrunner-documents/benchmarks/)**

</div>

---

> **TaskRunner** is a Go library for **distributed task processing** and
> **background job queues** built on **Redis Streams**. It provides **horizontal
> scalability**, **cooperative leader election**, **delayed task scheduling**,
> **unique (de-duplicated) jobs**, **automatic worker-pool tuning**, and
> **per-task timing metrics** through a simple, idiomatic API. It is the Redis
> Streams equivalent of Celery / Sidekiq / Hangfire -- but embedded in your Go
> application as a library, with no separate broker daemon to deploy.

## Why TaskRunner?

- **Distributed & Scalable** -- add instances to scale throughput horizontally;
  Redis Streams consumer groups spread the load automatically.
- **Reliable** -- visibility timeouts with heartbeats prevent double-processing;
  the pending-entries reclaimer and retries recover crashed workers.
- **Simple API** -- register a task and dispatch payloads with minimal
  boilerplate.
- **Delayed Scheduling** -- schedule jobs for future execution via a Redis ZSET
  polled by the leader.
- **Unique Jobs** -- enforce de-duplication across a time window with
  distributed locks (redsync).
- **Leader Election** -- cooperative, Lua-scripted atomic leadership for
  scheduling and maintenance loops.
- **Automatic Worker Tuning** -- the leader recomputes pool capacity to keep
  predicted wait time within a target band.
- **Observability** -- per-task timing metrics, predicted wait-time, TPS, and
  queue statistics.

## Installation

```bash
go get github.com/soroosh-tanzadeh/taskrunner
```

Requires **Go 1.24.9+** and **Redis 6+** (Redis 6.2+ recommended). For local
development and tests, the project uses `miniredis` to simulate Redis in-memory
-- no real Redis required.

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
		BatchSize:          10,
		ConsumerGroup:      "example",
		ConsumersPrefix:    "default",
		NumWorkers:         8,
		NumFetchers:        4,
		LongQueueHook:      func(s runner.Stats) { fmt.Printf("%+v\n", s) },
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

➡️
**[Full Getting Started guide →](https://soroosh-tanzadeh.github.io/taskrunner-documents/getting-started/)**

## Delayed Task Scheduler

Run a cooperative scheduler that enqueues due jobs from a ZSET into the stream.
The scheduler ticks every 5 seconds; delays shorter than this will be rounded up
to the next tick.

```go
ctx, cancel := context.WithCancel(context.Background())
go tr.Start(ctx)                    // workers + election
go tr.StartDelayedSchedule(ctx, 1000) // scheduler (batch size 1000)

_ = tr.DispatchDelayed(ctx, "exampletask", "I run in ~5s", 5*time.Second)
_ = tr.DispatchDelayed(ctx, "exampletask", "I run in ~10s", 10*time.Second)

// ... later
cancel()
```

Notes:

- The scheduler respects **leader election**; only the leader instance enqueues
  due jobs.
- Use `ScheduleFor(ctx, taskName, payload, time.Time)` to schedule for an
  absolute time.

➡️
**[Delayed Scheduling concepts →](https://soroosh-tanzadeh.github.io/taskrunner-documents/concepts/delayed-scheduling/)**

## Unique Jobs

Prevent duplicate enqueues of the same job (optionally scoped by a custom key)
for a specified window.

```go
tr.RegisterTask(&runner.Task{
	Name:      "sendEmail",
	MaxRetry:  3,
	Unique:    true,
	UniqueFor: 60, // seconds
	UniqueKey: func(payload any) string { return payload.(string) }, // e.g. email ID
	Action:    func(ctx context.Context, payload any) error { return nil },
})

// First dispatch succeeds
_ = tr.Dispatch(context.Background(), "sendEmail", "order-123")
// Second dispatch within 60s will fail with runner.ErrTaskAlreadyDispatched
err := tr.Dispatch(context.Background(), "sendEmail", "order-123")
```

➡️
**[Unique Jobs concepts →](https://soroosh-tanzadeh.github.io/taskrunner-documents/concepts/unique-jobs/)**

## Configuration

TaskRunner is configured via `runner.TaskRunnerConfig`:

- **Host** -- Optional; defaults to hostname; used in metrics and identity.
- **BatchSize** -- Number of messages fetched per read per fetcher.
- **ConsumerGroup** -- Redis Streams consumer group name. All instances that
  share a workload MUST use the same value.
- **ConsumersPrefix** -- Prefix for consumer names.
- **NumWorkers** -- Concurrent workers processing messages.
- **Worker tuning (optional)** -- automatically adjust worker pool size to keep
  the estimated queue wait time within a target band. Enable by setting:
  - **MinWorkers** -- lower bound for automatic tuning.
  - **MaxWorkers** -- upper bound for automatic tuning.
  - **DesiredWaitTime** -- target predicted wait time (`time.Duration`).
  - **DesiredWaitTimeTolerance** -- allowed deviation around the target.
- **NumFetchers** -- Concurrent fetchers reading from the stream (each reads
  `BatchSize`).
- **FailedTaskHandler** -- callback when a task exhausts retries.
- **LongQueueHook** -- periodic timing/queue stats callback; frequency set by
  `LongQueueThreshold`.
- **LongQueueThreshold** -- duration that influences the cadence of timing
  aggregation (every `threshold/2`).
- **BlockDuration** -- stream read block duration (defaults to 5s).
- **MetricsResetInterval** -- interval to reset timing metrics (default 24h; set
  0 to disable).
- ~~**ReplicationFactor**~~ -- Deprecated; auto-detected. Maintained for
  backward compatibility only.

Redis Stream queue configuration via options on
`NewRedisStreamMessageQueueWithOptions`:

- `WithPrefix(prefix string)`
- `WithQueue(queue string)`
- `WithReClaimDelay(d time.Duration)` -- reclaim pending messages after `d`.
- `WithDeleteOnAck(enabled bool)`
- `WithRedisVersion(version string)` -- override auto-detected version.

➡️
**[Full Configuration Reference →](https://soroosh-tanzadeh.github.io/taskrunner-documents/guides/configuration/)**

## Worker tuning tips (PredictedWaitTime)

`PredictedWaitTime` is an estimate (not actual measured latency) computed from:
average task execution time, current queue length, and the current worker pool
capacity.

When worker tuning is enabled, the leader periodically calls into the timing
aggregator and adjusts the worker pool capacity (`MinWorkers`..`MaxWorkers`) so
that predicted wait stays within `DesiredWaitTime ± DesiredWaitTimeTolerance`.

To tune in practice:

1. **Start with a realistic wait target** -- set `DesiredWaitTime` to the
   maximum queueing delay you can tolerate in normal operation.
2. **Add tolerance to avoid oscillation** -- use a non-zero
   `DesiredWaitTimeTolerance` (20% to 50% of `DesiredWaitTime`).
3. **Clamp with min/max** -- `MinWorkers` prevents under-provisioning during low
   load; `MaxWorkers` protects your CPU/memory budget during spikes.
4. **Control reaction speed** -- tuning runs on the timing aggregator loop
   (every `LongQueueThreshold/2`, or every 1 minute if unset).
5. **Watch `LongQueueHook`** -- validate that predicted wait time stays in band.

```go
tr := runner.NewTaskRunner(runner.TaskRunnerConfig{
	NumWorkers:               8, // used only when tuning is disabled
	MinWorkers:               2,
	MaxWorkers:               32,
	DesiredWaitTime:          500 * time.Millisecond,
	DesiredWaitTimeTolerance: 200 * time.Millisecond,
	LongQueueThreshold:       30 * time.Second, // controls tuning frequency
}, rdb, queue)
```

➡️
**[Worker Tuning concepts →](https://soroosh-tanzadeh.github.io/taskrunner-documents/concepts/worker-tuning/)**

## Examples

See runnable examples and tests under `examples/`:

- [`examples/simple`](examples/simple) -- Basic queue usage
- [`examples/scheduler`](examples/scheduler) -- Delayed tasks scheduler
- [`examples/unique`](examples/unique) -- Unique jobs
- [`cmd/worker`](cmd/worker) -- Local benchmark harness (needs a real Redis)

## Testing

Run the full test suite:

```bash
go test ./...
```

The examples and engine are covered by tests using `miniredis`, so they run
without a real Redis server. The GitHub Actions CI additionally runs against a
real `redis:7.4` container.

For the micro-benchmark suite:

```bash
go test -run '^$' -bench '.' ./runner/... ./internal/ring/...
```

➡️
**[Benchmarks →](https://soroosh-tanzadeh.github.io/taskrunner-documents/benchmarks/)**

## Documentation

The full documentation is hosted at
**<https://soroosh-tanzadeh.github.io/taskrunner-documents/>** and is built with
MkDocs Material. It covers:

- **Concepts** --
  [Architecture](https://soroosh-tanzadeh.github.io/taskrunner-documents/concepts/architecture/),
  [Leader Election](https://soroosh-tanzadeh.github.io/taskrunner-documents/concepts/leader-election/),
  [Fetchers & Workers](https://soroosh-tanzadeh.github.io/taskrunner-documents/concepts/fetchers-workers/),
  [Unique Jobs](https://soroosh-tanzadeh.github.io/taskrunner-documents/concepts/unique-jobs/),
  [Delayed Scheduling](https://soroosh-tanzadeh.github.io/taskrunner-documents/concepts/delayed-scheduling/),
  [Worker Tuning](https://soroosh-tanzadeh.github.io/taskrunner-documents/concepts/worker-tuning/),
  [Timing Metrics](https://soroosh-tanzadeh.github.io/taskrunner-documents/concepts/metrics/).
- **Guides** --
  [Configuration](https://soroosh-tanzadeh.github.io/taskrunner-documents/guides/configuration/),
  [Error Handling](https://soroosh-tanzadeh.github.io/taskrunner-documents/guides/error-handling/),
  [Observability](https://soroosh-tanzadeh.github.io/taskrunner-documents/guides/observability/),
  [Deployment](https://soroosh-tanzadeh.github.io/taskrunner-documents/guides/deployment/),
  [Redis Key Layout](https://soroosh-tanzadeh.github.io/taskrunner-documents/guides/redis-layout/).
- **Reference** --
  [API](https://soroosh-tanzadeh.github.io/taskrunner-documents/api-reference/),
  [Benchmarks](https://soroosh-tanzadeh.github.io/taskrunner-documents/benchmarks/).

To preview the docs locally:

```bash
pip install -r requirements-docs.txt
mkdocs serve
```

## Contributing

Contributions are welcome! Please:

- Open an issue to discuss substantial changes.
- Write tests for new features and ensure `go test ./...` passes.
- Feature branches must be named `feature/{feature-name}`.
- Follow idiomatic Go style and **keep APIs small and focused**.

➡️
**[Contributing guide →](https://soroosh-tanzadeh.github.io/taskrunner-documents/contributing/)**
· [`AGENTS.md`](AGENTS.md) for AI coding agents.

## License

GPL-3.0. See [`LICENSE`](LICENSE) for details.
