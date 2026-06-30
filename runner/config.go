package runner

import (
	"context"
	"time"
)

type LongQueueHook func(Stats)
type FailedTaskHandler func(ctx context.Context, task TaskMessage, err error) error

type TaskRunnerConfig struct {
	// Optional
	Host string

	BatchSize       int
	ConsumerGroup   string
	ConsumersPrefix string

	NumWorkers  int
	NumFetchers int

	// Worker tuning (optional).
	//
	// When enabled, the runner will automatically adjust the worker pool capacity to
	// keep PredictedWaitTime within:
	//   DesiredWaitTime +- DesiredWaitTimeTolerance
	//
	// All fields are expected in the same time unit (Duration). PredictedWaitTime is
	// computed in milliseconds.
	MinWorkers int
	MaxWorkers int

	DesiredWaitTime            time.Duration
	DesiredWaitTimeTolerance  time.Duration

	// TuningCooldownSeconds controls how frequently worker capacity can be
	// recalculated after an actual capacity change.
	//
	// Default: 10 seconds.
	// If set <= 0, default is applied.
	TuningCooldownSeconds int

	// ReplicationFactor Number of pod replicas configured, affecting metric calculations
	// Let T_avg be the average execution time of task, Q_len be the length of the queue, and W_num be the number of workers
	// The total execution time for the queue is estimated as (T_avg * Q_len) / (W_num * ReplicationFactor).
	// Deprecated: taskrunner automaticaly handles
	ReplicationFactor int

	FailedTaskHandler FailedTaskHandler

	LongQueueHook      LongQueueHook
	LongQueueThreshold time.Duration

	BlockDuration time.Duration

	// MetricsResetInterval defines how often timing metrics (sum and count) are reset.
	// If zero or negative, a default of 24 hours is used.
	MetricsResetInterval time.Duration
}
