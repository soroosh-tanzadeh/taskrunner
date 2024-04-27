package runner

import (
	"context"
	"time"
)

type LongQueueHook func(Stats)
type FailedTaskHandler func(ctx context.Context, task TaskMessage, err error) error

type TaskRunnerConfig struct {
	BatchSize       int
	ConsumerGroup   string
	ConsumersPrefix string

	NumWorkers int

	// ReplicationFactor Number of pod replicas configured, affecting metric calculations
	// Let T_avg be the average execution time of task, Q_len be the length of the queue, and W_num be the number of workers
	// The total execution time for the queue is estimated as (T_avg * Q_len) / (W_num * ReplicationFactor).
	ReplicationFactor int

	FailedTaskHandler FailedTaskHandler

	LongQueueHook      LongQueueHook
	LongQueueThreshold time.Duration
}
