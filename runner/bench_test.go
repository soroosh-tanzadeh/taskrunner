package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/soroosh-tanzadeh/taskrunner/internal/safemap"
	"github.com/soroosh-tanzadeh/taskrunner/redisstream"
)

// -----------------------------------------------------------------------------
// TimingBulkWriter
// -----------------------------------------------------------------------------

// BenchmarkTimingBulkWriter_Write measures the per-write cost of recording a
// task's execution timing, which is the hot path invoked by every worker after
// processing a message (see worker.go -> storeTiming). The writer batches
// writes on a background goroutine; this benchmark drains with a tight flush
// interval so the channel never blocks, isolating the cost of the write call
// itself.
func BenchmarkTimingBulkWriter_Write(b *testing.B) {
	w := NewBulkWriter(time.Microsecond, func(data []timingDto) error {
		return nil
	})
	defer w.close()
	dto := timingDto{taskName: "bench-task", timing: time.Millisecond}

	for b.Loop() {
		if err := w.write(dto); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTimingBulkWriter_WriteConcurrent measures write throughput under
// concurrent producers, mirroring how multiple workers push timings at once.
func BenchmarkTimingBulkWriter_WriteConcurrent(b *testing.B) {
	w := NewBulkWriter(time.Microsecond, func(data []timingDto) error {
		return nil
	})
	defer w.close()
	dto := timingDto{taskName: "bench-task", timing: time.Millisecond}
	workers := runtime.GOMAXPROCS(0)
	var wg sync.WaitGroup
	wg.Add(workers)
	b.ResetTimer()
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < b.N/workers; j++ {
				_ = w.write(dto)
			}
		}()
	}
	wg.Wait()
}

// -----------------------------------------------------------------------------
// Task message serialization (per-message hot path in Dispatch and worker)
// -----------------------------------------------------------------------------

func benchTaskMessage() TaskMessage {
	return TaskMessage{
		TaskName:           "bench-task",
		Unique:             true,
		UniqueFor:          60,
		UniqueKey:          "bench-task:user-123",
		UniqueLockValue:    "lock-token-abc",
		ReservationTimeout: int64(10 * time.Second),
		Payload:            map[string]any{"user_id": 123, "action": "send_email", "ts": 1700000000},
	}
}

// BenchmarkTaskMessage_Marshal covers json.Marshal(TaskMessage), called on
// every Dispatch (task_runner.go).
func BenchmarkTaskMessage_Marshal(b *testing.B) {
	msg := benchTaskMessage()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := json.Marshal(msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTaskMessage_Unmarshal covers the json.Unmarshal a worker performs on
// every fetched message before locating the task action (worker.go).
func BenchmarkTaskMessage_Unmarshal(b *testing.B) {
	msg := benchTaskMessage()
	raw, err := json.Marshal(msg)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		var dst TaskMessage
		if err := json.Unmarshal(raw, &dst); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTaskMessage_MarshalUnmarshal_RoundTrip measures the combined
// Dispatch+worker serialization cost for a single message.
func BenchmarkTaskMessage_MarshalUnmarshal_RoundTrip(b *testing.B) {
	msg := benchTaskMessage()
	b.ReportAllocs()
	for b.Loop() {
		raw, _ := json.Marshal(msg)
		var dst TaskMessage
		_ = json.Unmarshal(raw, &dst)
	}
}

// -----------------------------------------------------------------------------
// Task.CreateMessage + lockKey (called on every Dispatch)
// -----------------------------------------------------------------------------

func benchTask() *Task {
	return &Task{
		Name:               "bench-task",
		MaxRetry:           3,
		ReservationTimeout: 10 * time.Second,
		Unique:             true,
		UniqueFor:          60,
		UniqueKey: func(payload any) string {
			if m, ok := payload.(map[string]any); ok {
				if v, ok := m["user_id"]; ok {
					return fmt.Sprintf("user-%v", v)
				}
			}
			return ""
		},
	}
}

// BenchmarkTask_CreateMessage covers Task.CreateMessage, the first allocation
// on the Dispatch path (task_runner.go -> Dispatch).
func BenchmarkTask_CreateMessage(b *testing.B) {
	task := benchTask()
	payload := map[string]any{"user_id": 123, "action": "send_email"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = task.CreateMessage(payload)
	}
}

// -----------------------------------------------------------------------------
// SafeMap (the task registry -- read on every Dispatch and every worker invocation)
// -----------------------------------------------------------------------------

func populatedSafeMap(n int) *safemap.SafeMap[string, *Task] {
	m := safemap.NewSafeMap[string, *Task]()
	for i := 0; i < n; i++ {
		m.Set(fmt.Sprintf("task-%d", i), &Task{Name: fmt.Sprintf("task-%d", i)})
	}
	return m
}

// BenchmarkSafeMap_Get measures the read cost on the task registry, the hottest
// SafeMap operation (hit on every Dispatch and every worker lookup).
func BenchmarkSafeMap_Get(b *testing.B) {
	m := populatedSafeMap(100)

	for b.Loop() {
		_, _ = m.Get("task-50")
	}
}

// BenchmarkSafeMap_Set measures task registration cost.
func BenchmarkSafeMap_Set(b *testing.B) {
	m := populatedSafeMap(100)
	task := &Task{Name: "new-task"}

	for b.Loop() {
		m.Set("new-task", task)
	}
}

// BenchmarkSafeMap_Snapshot measures the cost of obtaining the task snapshot
// used by timing aggregation and metrics reset.
func BenchmarkSafeMap_Snapshot(b *testing.B) {
	m := populatedSafeMap(100)
	for b.Loop() {
		_ = m.Snapshot()
	}
}

// BenchmarkSafeMap_GetConcurrent measures concurrent read throughput on the
// task registry, modelling multiple fetchers/workers doing lookups in parallel.
func BenchmarkSafeMap_GetConcurrent(b *testing.B) {
	m := populatedSafeMap(100)
	workers := runtime.GOMAXPROCS(0)
	var wg sync.WaitGroup
	wg.Add(workers)
	b.ResetTimer()
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < b.N/workers; j++ {
				_, _ = m.Get("task-50")
			}
		}()
	}
	wg.Wait()
}

// -----------------------------------------------------------------------------
// Dispatch end-to-end (miniredis-backed, real queue + serialization path)
// -----------------------------------------------------------------------------

// setupBenchRunner builds a TaskRunner wired to an in-process miniredis, with a
// registered task. It returns the runner, the miniredis handle, and a cleanup
// func. Dispatch does not require Start(), so we skip the worker pool to keep
// the benchmark focused on the dispatch hot path.
func setupBenchRunner(b *testing.B, taskUnique bool) (*TaskRunner, *miniredis.Miniredis, func()) {
	b.Helper()
	s, err := miniredis.Run()
	if err != nil {
		b.Fatalf("failed to start miniredis: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr(), PoolSize: 30})
	queue := redisstream.NewRedisStreamMessageQueue(rdb, "test", "queue", time.Second*10, true)
	tr := NewTaskRunner(TaskRunnerConfig{
		BatchSize:       5,
		ConsumerGroup:   "bench_group",
		ConsumersPrefix: "taskrunner",
		NumWorkers:      10,
	}, rdb, queue)
	task := &Task{
		Name:               "bench-task",
		MaxRetry:           3,
		ReservationTimeout: 10 * time.Second,
		Unique:             taskUnique,
		UniqueFor:          60,
	}
	if taskUnique {
		// Distinct payloads must map to distinct lock keys, otherwise every
		// dispatch collides on the same lock and returns ErrTaskAlreadyDispatched.
		task.UniqueKey = func(payload any) string {
			return fmt.Sprintf("job-%v", payload)
		}
	}
	tr.RegisterTask(task)
	// The error channel is buffered with capacity 1; drain it in the background
	// so a non-fatal captureError (e.g. an Ack race) cannot block Dispatch.
	go func() {
		for range tr.ErrorChannel() {
		}
	}()
	cleanup := func() {
		s.Close()
		_ = rdb.Close()
	}
	return tr, s, cleanup
}

// BenchmarkDispatch_NonUnique measures the full non-unique Dispatch path:
// task lookup, message creation, JSON marshal, and a Redis Stream XADD against
// miniredis. This is the producer-side hot path.
func BenchmarkDispatch_NonUnique(b *testing.B) {
	tr, _, cleanup := setupBenchRunner(b, false)
	defer cleanup()
	ctx := context.Background()
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		if err := tr.Dispatch(ctx, "bench-task", i); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDispatch_Unique adds the distributed-lock acquisition overhead
// (redsync SET NX) on top of the non-unique path, modelling unique-task
// dispatch which is the other major Dispatch branch.
func BenchmarkDispatch_Unique(b *testing.B) {
	tr, _, cleanup := setupBenchRunner(b, true)
	defer cleanup()
	ctx := context.Background()
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		// Unique tasks with the same payload would collide on the lock, so vary
		// the payload to model dispatching distinct unique jobs.
		if err := tr.Dispatch(ctx, "bench-task", i); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDispatch_Concurrent measures producer throughput under concurrent
// dispatchers, the realistic deployment shape (multiple producers feeding one
// queue).
func BenchmarkDispatch_Concurrent(b *testing.B) {
	tr, _, cleanup := setupBenchRunner(b, false)
	defer cleanup()
	ctx := context.Background()
	workers := runtime.GOMAXPROCS(0)
	var wg sync.WaitGroup
	wg.Add(workers)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			each := b.N / workers
			for j := 0; j < each; j++ {
				if err := tr.Dispatch(ctx, "bench-task", id*1000+j); err != nil {
					b.Error(err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}
