package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/soroosh-tanzadeh/taskrunner/redisstream"
	"github.com/soroosh-tanzadeh/taskrunner/runner"
)

func TestSchedulerExample(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})

	queue := redisstream.NewRedisStreamMessageQueueWithOptions(
		rdb,
		redisstream.WithPrefix("example"),
		redisstream.WithQueue("tasks"),
	)

	tr := runner.NewTaskRunner(runner.TaskRunnerConfig{
		BatchSize:       5,
		ConsumerGroup:   "example-scheduler",
		ConsumersPrefix: "default",
		NumWorkers:      2,
		NumFetchers:     1,
	}, rdb, queue)

	consumed := make(chan string, 1)
	tr.RegisterTask(&runner.Task{
		Name:     "runLater",
		MaxRetry: 1,
		Action: func(ctx context.Context, payload any) error {
			consumed <- payload.(string)
			return nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() { defer wg.Done(); _ = tr.Start(ctx) }()
	go func() { defer wg.Done(); _ = tr.StartDelayedSchedule(ctx, 1000) }()

	_ = tr.DispatchDelayed(context.Background(), "runLater", "A", 5*time.Second)

	// Wait up to the scheduler tick + processing
	select {
	case <-consumed:
	case <-time.After(12 * time.Second):
		t.Fatal("scheduler did not dispatch delayed job in time")
	}

	cancel()
	wg.Wait()
}
