# TaskRunner 
TaskRunner is a robust and efficient Go library designed to leverage the power of **Redis Streams** for distributed task execution.

## Key Features
- **Asynchronous task Processing:** TaskRunner enables the scheduling and execution of tasks asynchronously, allowing your application to remain responsive and performant under heavy load.
- **Scalable and Distributed:** Designed to scale horizontally, this library can expand its capacity by adding more workers without impacting the existing infrastructure. It supports seamless distribution of tasks across a cluster of servers, optimizing resource usage and balancing the load.
- **Fault Tolerance:** Includes features for automatic task retries and error handling, ensuring that system failures do not lead to task loss or inconsistencies.
- **Simple API:** Offers a straightforward and intuitive API that makes it easy to integrate and use within your existing Go applications.
- **Real-time Monitoring and Logging:** Integrates monitoring capabilities to track task status and performance metrics in real-time, alongside comprehensive logging for debugging and audit trails.

## How to use

### Simple Example
```go
package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"git.arvaninternal.ir/cdn-go-kit/taskrunner/redisstream"
	"git.arvaninternal.ir/cdn-go-kit/taskrunner/runner"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		DB:       5,
		PoolSize: 100,
	})
	wg := sync.WaitGroup{}

	queue := redisstream.NewRedisStreamMessageQueue(rdb, "example_tasks", "default", time.Second*30, true)
	taskRunner := runner.NewTaskRunner(runner.TaskRunnerConfig{
		BatchSize:         10,
		ConsumerGroup:     "example",
		ConsumersPrefix:   "default",
		NumWorkers:        10,
		ReplicationFactor: 1,
		LongQueueHook: func(s runner.Stats) {
			fmt.Printf("%v \n", s)
		},
		LongQueueThreshold: time.Second * 30,
	}, rdb, queue)

	taskRunner.RegisterTask(runner.Task{
		Name:     "exampletask",
		MaxRetry: 10,
		Action: func(ctx context.Context, payload any) error {
			fmt.Printf("Hello from example task %s\n", payload)
			return nil
		},
		Unique: false,
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		taskRunner.Start(context.Background())
	}()

	for i := 0; i < 100; i++ {
		taskRunner.Dispatch(context.Background(), "exampletask", strconv.Itoa(i))
	}

	wg.Wait()

}
```

### TaskRunnerConfig Parameters

Below is a detailed description of each parameter in the `TaskRunnerConfig` struct.

| Parameter            | Type                 | Description                                                                                                        |
|----------------------|----------------------|--------------------------------------------------------------------------------------------------------------------|
| `BatchSize`          | `int`                | The number of tasks to be processed in a single batch by the consumer.                                             |
| `ConsumerGroup`      | `string`             | The name of the consumer group used in Redis Streams to differentiate between different sets of consumers.         |
| `ConsumersPrefix`    | `string`             | A prefix used for naming consumers within the group, aiding in identification and management.                       |
| `NumWorkers`         | `int`                | The number of consumer workers that will concurrently process tasks from the queue.                                 |
| `ReplicationFactor`  | `int`                | ReplicationFactor Number of pod replicas configured. Let T_avg be the average execution time of task, Q_len be the length of the queue, and W_num be the number of workers. The total execution time for the queue is estimated as **(T_avg * Q_len) / (W_num * ReplicationFactor)**.|
| `FailedTaskHandler`  | `FailedTaskHandler`  | When task can not be retried any more, this function will be called|
| `LongQueueHook`      | `LongQueueHook`      | A hook function or callback that is triggered when the task queue length exceeds a specified threshold.             |
| `LongQueueThreshold` | `time.Duration`      | The duration or length of the queue that triggers the `LongQueueHook` when exceeded.                                |

This configuration is critical for efficiently managing and operating asynchronous task processing systems, especially in environments where task distribution and fault tolerance are key.

**Recommendation**: 
For optimal performance with long-running tasks, it is recommended to use a small BatchSize coupled with a large number of NumWorkers. This configuration ensures that tasks are distributed more evenly across workers, reducing bottlenecks and improving overall efficiency.

### Task Paramters

Below is a table detailing the fields of the `Task` struct:

| Field               | Type            | Description                                                                                                                                                      |
|---------------------|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Name`              | `string`        | The name of the task.                                                                                                                                            |
| `MaxRetry`          | `int`           | The maximum number of times the task is retried if it fails initially.                                                                                           |
| `ReservationTimeout`| `time.Duration` | The duration after which a worker must send a heartbeat if the task is still running. This prevents the task from being reassigned or reclaimed by another worker.|
| `Action`            | `TaskAction`    | The action associated with the task.                                                                                                                             |
| `Unique`            | `bool`          | If set to `true`, the task will not be dispatched if an identical task (by name and UniqueKey) is already in the queue and has not been completed.                             |
| `UniqueKey`         | `UniqueKeyFunc` | Specifies a function to generate a unique key for the task. If `Unique` is `true`, tasks with the same unique key must not be in the queue simultaneously.       |
| `UniqueFor`         | `int64`         | The time in seconds that must elapse since a similarly unique task was last enqueued before a new task with the same name and unique key can be dispatched.      |