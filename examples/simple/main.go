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

	for i := 0; i < 10; i++ {
		_ = tr.Dispatch(context.Background(), "exampletask", strconv.Itoa(i))
	}

	time.Sleep(2 * time.Second)
	cancel()
	wg.Wait()
}
