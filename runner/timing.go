package runner

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// Task Timing key taskrunner:metrics:*
// e.g. TaskSLLSetup will be taskrunner:metrics:sslsetup
const taskMetricStreamPrefix = "taskrunner:metrics:"

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
}

// GetTimingStatistics return PerTaskTiming and other estimated statistics of the queue
func (t *TaskRunner) GetTimingStatistics() (Stats, error) {
	// captures a snapshot of the currently registered tasks
	tasks := t.tasks.Snapshot()
	if len(tasks) == 0 {
		return Stats{}, nil
	}

	var totalExecutionAverage int64 = 0
	perTaskTiming := make(map[string]int64)
	// iterate over tasks
	for taskName := range tasks {
		stream := taskMetricStreamPrefix + taskName
		// get average execution of task
		avg, err := t.avgOfStream(t.metricsHash, taskName+"_avg", stream, "-", "+", 1000, "timing")
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}

			t.captureError(err)
			continue
		}

		// add the average to totalExecutionAverage (T_avg function comments)
		totalExecutionAverage += avg
		perTaskTiming[taskName] = avg
	}

	// Schedule timing
	scheduleTiming, err := t.avgOfStream(t.metricsHash, "schedule"+"_avg", taskMetricStreamPrefix+":"+delayedTasksTimingKey, "-", "+", 1000, "timing")
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			t.captureError(err)
		}
	}

	// calculate total average (T_avg)
	totalExecutionAverage = totalExecutionAverage / int64(len(tasks))
	avgTiming := totalExecutionAverage
	queueLen, err := t.queue.Len()
	if err != nil {
		t.captureError(err)
		return Stats{}, nil
	}
	predictedWaitTime := ((float64(avgTiming) * float64(queueLen)) / (float64(t.cfg.NumWorkers)) * float64(t.cfg.ReplicationFactor))
	return Stats{
		PerTaskTiming:     perTaskTiming,
		PredictedWaitTime: float64(predictedWaitTime),
		AvgTiming:         time.Duration(avgTiming * int64(time.Millisecond)),
		AvgScheduleTiming: scheduleTiming,
		TPS:               float64(1000.0) / float64(avgTiming),
	}, nil
}

func (t *TaskRunner) timingFlush(buf []timingDto) error {
	ctx := context.Background()
	_, err := t.redisClient.Pipelined(ctx, func(p redis.Pipeliner) error {
		for _, timing := range buf {
			p.XAdd(ctx, &redis.XAddArgs{
				Stream: taskMetricStreamPrefix + timing.taskName,
				MaxLen: 1000,
				Approx: false,
				Values: map[string]any{
					"timing": timing.timing.Milliseconds(),
				},
			})
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
