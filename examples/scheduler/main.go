package main

import (
	"context"
	"fmt"
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
		NumFetchers:     2,
	}, rdb, queue)

	tr.RegisterTask(&runner.Task{
		Name:     "runLater",
		MaxRetry: 3,
		Action: func(ctx context.Context, payload any) error {
			fmt.Printf("executed: %v\n", payload)
			return nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() { defer wg.Done(); _ = tr.Start(ctx) }()
	go func() { defer wg.Done(); _ = tr.StartDelayedSchedule(ctx, 1000) }()

	_ = tr.DispatchDelayed(context.Background(), "runLater", "I run in ~5s", 5*time.Second)
	_ = tr.DispatchDelayed(context.Background(), "runLater", "I run in ~10s", 10*time.Second)

	time.Sleep(12 * time.Second)
	cancel()
	wg.Wait()
}
