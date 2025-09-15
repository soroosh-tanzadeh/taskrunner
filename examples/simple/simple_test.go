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

func TestSimpleExample(t *testing.T) {
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
		ConsumerGroup:   "example",
		ConsumersPrefix: "default",
		NumWorkers:      2,
		NumFetchers:     1,
	}, rdb, queue)

	consumed := make(chan struct{}, 3)
	tr.RegisterTask(&runner.Task{
		Name:     "exampletask",
		MaxRetry: 1,
		Action: func(ctx context.Context, payload any) error {
			consumed <- struct{}{}
			return nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() { defer wg.Done(); _ = tr.Start(ctx) }()

	_ = tr.Dispatch(context.Background(), "exampletask", "a")
	_ = tr.Dispatch(context.Background(), "exampletask", "b")
	_ = tr.Dispatch(context.Background(), "exampletask", "c")

	select {
	case <-consumed:
	case <-time.After(2 * time.Second):
		t.Fatal("expected at least one task to be consumed")
	}

	cancel()
	wg.Wait()
}
