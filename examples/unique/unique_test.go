package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/soroosh-tanzadeh/taskrunner/redisstream"
	"github.com/soroosh-tanzadeh/taskrunner/runner"
)

func TestUniqueExample(t *testing.T) {
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
		BatchSize:       2,
		ConsumerGroup:   "example-unique",
		ConsumersPrefix: "default",
		NumWorkers:      1,
		NumFetchers:     1,
	}, rdb, queue)

	tr.RegisterTask(&runner.Task{
		Name:      "sendEmail",
		MaxRetry:  1,
		Unique:    true,
		UniqueFor: 60,
		UniqueKey: func(payload any) string { return payload.(string) },
		Action:    func(ctx context.Context, payload any) error { return nil },
	})

	// Do not start workers; keep the unique lock held by dispatch
	if err := tr.Dispatch(context.Background(), "sendEmail", "order-123"); err != nil {
		t.Fatalf("first dispatch failed: %v", err)
	}
	if err := tr.Dispatch(context.Background(), "sendEmail", "order-123"); !errors.Is(err, runner.ErrTaskAlreadyDispatched) {
		t.Fatalf("expected ErrTaskAlreadyDispatched, got: %v", err)
	}

	// After window passes, it should allow again
	srv.FastForward(61 * time.Second)
	if err := tr.Dispatch(context.Background(), "sendEmail", "order-123"); err != nil {
		t.Fatalf("dispatch after window failed: %v", err)
	}
}
