package main

import (
	"context"
	"fmt"
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
		BatchSize:       5,
		ConsumerGroup:   "example-unique",
		ConsumersPrefix: "default",
		NumWorkers:      2,
		NumFetchers:     1,
	}, rdb, queue)

	tr.RegisterTask(&runner.Task{
		Name:      "sendEmail",
		MaxRetry:  1,
		Unique:    true,
		UniqueFor: 60,
		UniqueKey: func(payload any) string { return payload.(string) },
		Action: func(ctx context.Context, payload any) error {
			fmt.Printf("sending email: %s\n", payload)
			return nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tr.Start(ctx)

	_ = tr.Dispatch(context.Background(), "sendEmail", "order-123")
	if err := tr.Dispatch(context.Background(), "sendEmail", "order-123"); err != nil {
		fmt.Printf("duplicate blocked: %v\n", err)
	}

	time.Sleep(2 * time.Second)
	cancel()
}
