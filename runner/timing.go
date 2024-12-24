package runner

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

type timingDto struct {
	timing   time.Duration
	taskName string
}

func (t *TaskRunner) storeTiming(taskName string, x time.Duration) {
	t.tasksTimingBulkWriter.write(timingDto{taskName: taskName, timing: x})
}

// timingAggregator captures a snapshot of the currently registered tasks and calculates the average execution time for each task.
// It then determines the total execution time for the queue by averaging the average execution times of tasks.
// Let T_avg be the average execution time of task, Q_len be the length of the queue, and W_num be the number of workers, and R_factor be the Replication Factor entered in configuration.
// The total execution time for the queue is estimated as (T_avg * Q_len) / (W_num * R_factor).
// If the estimated time exceeds the LongQueueThreshold, a Hook is triggered to notify the User.
func (t *TaskRunner) timingAggregator() {
	if !t.IsLeader() {
		return
	}
	stats, err := t.GetTimingStatistics()
	if err != nil {
		t.captureError(err)
		return
	}

	if time.Duration(stats.PredictedWaitTime*float64(time.Millisecond)) > t.cfg.LongQueueThreshold {
		// LongQueueThreshold exceed, notify the developer
		if t.cfg.LongQueueHook != nil {
			t.cfg.LongQueueHook(stats)
		}
	}
	// Reset metrics for all tasks
	t.resetTimingMetrics()
}

// GetTimingStatistics return PerTaskTiming and other estimated statistics of the queue
func (t *TaskRunner) GetTimingStatistics() (Stats, error) {
	replicationFactor, err := t.GetNumberOfReplications()
	if err != nil {
		replicationFactor = 1
		t.captureError(err)
	}

	// captures a snapshot of the currently registered tasks
	tasks := t.tasks.Snapshot()
	if len(tasks) == 0 {
		return Stats{}, nil
	}

	var sumTasksTiming int64 = 0
	var countTasks int64 = 0
	perTaskTiming := make(map[string]int64)

	ctx := context.Background()
	// iterate over tasks
	for taskName := range tasks {
		totalTiming, err := t.redisClient.HGet(ctx, t.metricsHash, taskName+"_sum").Int64()
		if err != nil && !errors.Is(err, redis.Nil) {
			t.captureError(err)
			continue
		}
		sumTasksTiming += totalTiming

		count, err := t.redisClient.HGet(ctx, t.metricsHash, taskName+"_count").Int64()
		if err != nil && !errors.Is(err, redis.Nil) {
			t.captureError(err)
			continue
		}
		countTasks += count

		if count > 0 {
			// calculate average execution of task
			avg := int64(totalTiming / count)
			perTaskTiming[taskName] = avg
		}
	}

	// calculate total average (T_avg)
	var totalExecutionAverage int64 = 0
	if countTasks > 0 {
		totalExecutionAverage = int64(sumTasksTiming / countTasks)
	}

	var scheduleTiming int64 = 0
	// calculate schedule timing
	totalScheduleTiming, err := t.redisClient.HGet(ctx, t.metricsHash, t.getDelayedTimingTasksKey()+"_sum").Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.captureError(err)
	}
	countSchedules, err := t.redisClient.HGet(ctx, t.metricsHash, t.getDelayedTimingTasksKey()+"_count").Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.captureError(err)
	}

	if countSchedules > 0 {
		scheduleTiming = int64(totalScheduleTiming / countSchedules)
	}

	// calculate total average (T_avg)
	// totalExecutionAverage = totalExecutionAverage / int64(len(tasks))
	avgTiming := totalExecutionAverage
	queueLen, err := t.queue.Len()
	if err != nil {
		t.captureError(err)
		return Stats{}, nil
	}
	predictedWaitTime := ((float64(avgTiming) * float64(queueLen)) / (float64(t.cfg.NumWorkers)) * float64(replicationFactor))
	tps := 0.0
	if avgTiming != 0 {
		tps = (float64(1000.0) / float64(avgTiming)) * float64(t.activeWorkers.Load()) * float64(replicationFactor)
	}
	return Stats{
		PerTaskTiming:     perTaskTiming,
		PredictedWaitTime: float64(predictedWaitTime),
		AvgTiming:         time.Duration(avgTiming * int64(time.Millisecond)),
		AvgScheduleTiming: scheduleTiming,
		TPS:               math.Round(tps),
	}, nil
}

func (t *TaskRunner) resetTimingMetrics() {
	ctx := context.Background()
	taskKeys := t.tasks.Snapshot()

	_, err := t.redisClient.Pipelined(ctx, func(p redis.Pipeliner) error {
		for taskName := range taskKeys {
			p.HSet(ctx, t.metricsHash, taskName+"_sum", 0)
			p.HSet(ctx, t.metricsHash, taskName+"_count", 0)
		}
		p.HSet(ctx, t.metricsHash, t.getDelayedTimingTasksKey()+"_sum", 0)
		p.HSet(ctx, t.metricsHash, t.getDelayedTimingTasksKey()+"_count", 0)
		return nil
	})

	if err != nil {
		t.captureError(err)
	}
}

func (t *TaskRunner) timingFlush(buf []timingDto) error {
	ctx := context.Background()
	_, err := t.redisClient.Pipelined(ctx, func(p redis.Pipeliner) error {
		for _, timing := range buf {
			p.HIncrBy(ctx, t.metricsHash, timing.taskName+"_sum", timing.timing.Milliseconds())
			p.HIncrBy(ctx, t.metricsHash, timing.taskName+"_count", 1)
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
