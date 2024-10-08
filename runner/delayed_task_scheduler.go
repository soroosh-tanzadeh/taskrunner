package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/google/uuid"
	ants "github.com/panjf2000/ants/v2"
	"github.com/redis/go-redis/v9"
)

const delayedTasksKey = "taskrunner:%s:delayed_tasks"
const delayedTasksTimingKey = "taskrunner:%s:delayed_tasks_schedule"

func (t *TaskRunner) getDelayedTasksKey() string {
	return fmt.Sprintf(delayedTasksKey, t.ConsumerGroup())
}

func (t *TaskRunner) getDelayedTimingTasksKey() string {
	return fmt.Sprintf(delayedTasksTimingKey, t.ConsumerGroup())
}

func (t *TaskRunner) StartDelayedSchedule(ctx context.Context, batchSize int) error {
	workerPool, err := ants.NewPoolWithFunc(batchSize, func(arg interface{}) {
		args := arg.(map[string]interface{})
		task := args["task"].(DelayedTask)
		payload := args["payload"].(string)

		if t.status.Load() == stateStopped {
			return
		}

		if err := t.Dispatch(context.Background(), task.Task, task.Payload); err != nil {
			if !errors.Is(err, ErrTaskAlreadyDispatched) {
				t.captureError(err)
			}
			return
		}

		if _, err := t.redisClient.ZRem(context.Background(), t.getDelayedTasksKey(), payload).Result(); err != nil {
			t.captureError(err)
		}

	}, ants.WithNonblocking(false), ants.WithExpiryDuration(time.Second*2))

	if err != nil {
		return err
	}
	ticker := time.NewTicker(time.Second * 5)
	for {
		select {
		case nowTime := <-ticker.C:
			now := strconv.Itoa(int(nowTime.Unix()))

			if !t.IsLeader() {
				continue
			}

			count, err := t.redisClient.ZCount(ctx, t.getDelayedTasksKey(), "0", now).Result()
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}

				t.captureError(err)
				continue
			}
			if count == 0 {
				continue
			}

			pages := math.Ceil(float64(count) / float64(batchSize))

			startDispatcing := time.Now()
			for page := 0; page < int(pages); page++ {
				tasks, err := t.redisClient.ZRangeByScoreWithScores(context.Background(), t.getDelayedTasksKey(), &redis.ZRangeBy{
					Min:    "-inf",
					Max:    now,
					Count:  int64(batchSize),
					Offset: int64((page) * batchSize),
				}).Result()

				if err != nil {
					t.captureError(err)
					continue
				}

				for _, payload := range tasks {
					var task DelayedTask
					payload := payload.Member.(string)

					if err := json.Unmarshal([]byte(payload), &task); err != nil {
						t.captureError(err)
						continue
					}

					err := workerPool.Invoke(map[string]interface{}{"task": task, "payload": payload})
					if err != nil {
						return err
					}
				}
			}
			dispatchDuration := time.Since(startDispatcing)
			t.storeTiming(t.getDelayedTimingTasksKey(), dispatchDuration)

		case <-ctx.Done():
			workerPool.Release()
			return nil
		}
	}
}

// DispatchDelayed dispatches the task with delay
// Note: delay is not exact and may have 1-5 seconds error
func (t *TaskRunner) DispatchDelayed(ctx context.Context, taskName string, payload any, d time.Duration) error {
	_, ok := t.tasks.Get(taskName)
	if !ok {
		return ErrTaskNotFound
	}

	if d.Seconds() < 5 {
		return ErrDurationIsSmallerThanCheckCycle
	}

	delayedTask := DelayedTask{
		Id:      uuid.NewString(),
		Time:    time.Now().Add(d).Unix(),
		Payload: payload,
		Task:    taskName,
	}
	delayedTaskJson, err := json.Marshal(delayedTask)
	if err != nil {
		return err
	}

	return t.redisClient.ZAdd(ctx, t.getDelayedTasksKey(), redis.Z{Score: float64(delayedTask.Time), Member: delayedTaskJson}).Err()
}

func (t *TaskRunner) ScheduleFor(ctx context.Context, taskName string, payload any, executionTime time.Time) error {
	_, ok := t.tasks.Get(taskName)
	if !ok {
		return ErrTaskNotFound
	}

	if -time.Since(executionTime).Seconds() < 5 {
		return ErrDurationIsSmallerThanCheckCycle
	}

	delayedTask := DelayedTask{
		Id:      uuid.NewString(),
		Time:    executionTime.Unix(),
		Payload: payload,
		Task:    taskName,
	}
	delayedTaskJson, err := json.Marshal(delayedTask)
	if err != nil {
		return err
	}

	return t.redisClient.ZAdd(ctx, t.getDelayedTasksKey(), redis.Z{Score: float64(delayedTask.Time), Member: delayedTaskJson}).Err()
}
