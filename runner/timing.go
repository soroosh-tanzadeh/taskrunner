package runner

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
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
	// If worker tuning is enabled, followers should keep up with any changes
	// published by the current leader.
	if t.isWorkerTuningEnabled() {
		t.applyRemoteWorkerTuning()
	}

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

	t.tuneWorkers(stats)

	// Check if MetricsResetInterval is configured and if the interval has passed
	if t.cfg.MetricsResetInterval > 0 { // Ensure interval is positive
		if time.Since(t.lastMetricsResetTime) > t.cfg.MetricsResetInterval {
			t.resetTimingMetrics()
			t.lastMetricsResetTime = time.Now()
		}
	}
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
			perTaskTiming[taskName] = int64(totalTiming / count)
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
	avgTiming := totalExecutionAverage
	queueLen, err := t.queue.Len()
	if err != nil {
		t.captureError(err)
		return Stats{}, nil
	}

	// Use the current worker pool capacity for the estimate. If the pool is nil
	// (e.g. called before Start()), fall back to the configured NumWorkers.
	workers := 0
	if t.workerPool != nil {
		workers = t.workerPool.Cap()
	}
	if workers <= 0 {
		workers = t.cfg.NumWorkers
	}
	if workers <= 0 {
		workers = 1
	}
	// Estimated queue waiting time:
	// (T_avg * Q_len) / (W_num * replicationFactor)
	// where W_num is the per-instance worker pool capacity.
	predictedWaitTime := ((float64(avgTiming) * float64(queueLen)) / (float64(workers) * float64(replicationFactor)))
	tps := 0.0
	if avgTiming != 0 {
		tpsWorkers := workers
		if t.workerPool != nil {
			tpsWorkers = t.workerPool.Cap()
		}
		tps = (float64(1000.0) / float64(avgTiming)) * float64(tpsWorkers) * float64(replicationFactor)
	}
	return Stats{
		PerTaskTiming:     perTaskTiming,
		PredictedWaitTime: float64(predictedWaitTime),
		AvgTiming:         time.Duration(avgTiming * int64(time.Millisecond)),
		AvgScheduleTiming: scheduleTiming,
		TPS:               math.Round(tps),
	}, nil
}

func (t *TaskRunner) tuneWorkers(stats Stats) {
	if t.workerPool == nil {
		return
	}

	if !t.isWorkerTuningEnabled() {
		return
	}

	current := t.workerPool.Cap()
	if current < t.cfg.MinWorkers {
		current = t.cfg.MinWorkers
	}
	desiredMs := float64(t.cfg.DesiredWaitTime) / float64(time.Millisecond)
	toleranceMs := float64(t.cfg.DesiredWaitTimeTolerance) / float64(time.Millisecond)
	if desiredMs <= 0 {
		return
	}

	replicationFactor, err := t.GetNumberOfReplications()
	if err != nil || replicationFactor <= 0 {
		replicationFactor = 1
	}

	next := computeTunedWorkers(
		current,
		replicationFactor,
		t.cfg.MinWorkers,
		t.cfg.MaxWorkers,
		stats.PredictedWaitTime,
		desiredMs,
		toleranceMs,
	)
	if next != current {
		t.workerPool.Tune(next)
		t.publishWorkerCapacity(next)
	}
}

const workerTuningTTL = time.Second * 20

func (t *TaskRunner) tuningWorkersKey() string {
	return fmt.Sprintf("taskrunner:%s:worker_tuning_capacity", t.cfg.ConsumerGroup)
}

func (t *TaskRunner) isWorkerTuningEnabled() bool {
	return t.cfg.MinWorkers > 0 &&
		t.cfg.MaxWorkers > 0 &&
		t.cfg.MinWorkers <= t.cfg.MaxWorkers &&
		t.cfg.DesiredWaitTime > 0 &&
		t.cfg.DesiredWaitTimeTolerance >= 0
}

func (t *TaskRunner) publishWorkerCapacity(workers int) {
	if t.redisClient == nil {
		return
	}
	_ = t.redisClient.Set(context.Background(), t.tuningWorkersKey(), workers, workerTuningTTL).Err()
}

func (t *TaskRunner) applyRemoteWorkerTuning() {
	if t.workerPool == nil || !t.isWorkerTuningEnabled() {
		return
	}

	v, err := t.redisClient.Get(context.Background(), t.tuningWorkersKey()).Result()
	if err != nil {
		// Ignore missing/expired value.
		return
	}
	remote, err := strconv.Atoi(v)
	if err != nil {
		return
	}
	if remote <= 0 {
		return
	}

	// Clamp to configured bounds before applying.
	if remote < t.cfg.MinWorkers {
		remote = t.cfg.MinWorkers
	}
	if remote > t.cfg.MaxWorkers {
		remote = t.cfg.MaxWorkers
	}

	current := t.workerPool.Cap()
	if current != remote {
		t.workerPool.Tune(remote)
	}
}

func computeTunedWorkers(currentWorkers, replicationFactor, minWorkers, maxWorkers int, predictedWaitMs, desiredWaitMs, toleranceMs float64) int {
	if currentWorkers <= 0 || minWorkers <= 0 || maxWorkers <= 0 {
		return currentWorkers
	}
	if replicationFactor <= 0 {
		replicationFactor = 1
	}
	if desiredWaitMs <= 0 || toleranceMs < 0 {
		return currentWorkers
	}

	lower := desiredWaitMs - toleranceMs
	if lower < 0 {
		lower = 0
	}
	upper := desiredWaitMs + toleranceMs

	// Already in band.
	if predictedWaitMs >= lower && predictedWaitMs <= upper {
		return currentWorkers
	}

	// Predicted wait is inversely proportional to the total worker capacity:
	// totalWorkers = currentWorkers * replicationFactor
	// We compute the target total workers that would bring predicted wait
	// back to the closest boundary (lower/upper), then translate it into a
	// per-instance capacity by dividing by replicationFactor.
	targetWaitMs := desiredWaitMs
	increase := predictedWaitMs > upper
	if increase {
		targetWaitMs = upper
	} else {
		targetWaitMs = lower
	}

	if targetWaitMs <= 0 {
		return currentWorkers
	}

	currentTotalWorkers := float64(currentWorkers * replicationFactor)
	desiredTotalWorkers := currentTotalWorkers * (predictedWaitMs / targetWaitMs)

	var nextTotal int
	if increase {
		nextTotal = int(math.Ceil(desiredTotalWorkers))
	} else {
		nextTotal = int(math.Floor(desiredTotalWorkers))
	}
	if nextTotal <= 0 {
		nextTotal = 1
	}

	// Convert total workers back into per-instance worker capacity.
	var next int
	if increase {
		next = int(math.Ceil(float64(nextTotal) / float64(replicationFactor)))
	} else {
		next = int(math.Floor(float64(nextTotal) / float64(replicationFactor)))
	}

	if next < minWorkers {
		next = minWorkers
	}
	if next > maxWorkers {
		next = maxWorkers
	}
	if next <= 0 {
		next = minWorkers
	}

	return next
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
