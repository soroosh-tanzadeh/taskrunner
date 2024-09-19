package runner

import (
	"context"
	"encoding/json"
	"math"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	election "github.com/soroosh-tanzadeh/taskrunner/election"
)

const delayedTasksKey = "taskrunner:delayed_tasks"

func (t *TaskRunner) StartDelayedSchedule(ctx context.Context, batchSize int, elector *election.Elector) error {
	ticker := time.NewTicker(time.Second * 5)
	for {
		select {
		case nowTime := <-ticker.C:
			now := strconv.Itoa(int(nowTime.UnixMilli()))

			isLeader := elector.IsLeader(context.Background())
			if isLeader != nil {
				continue
			}

			count, err := t.redisClient.ZCount(ctx, delayedTasksKey, "0", now).Result()
			if err != nil {
				t.captureError(err)
				continue
			}

			pages := math.Ceil(float64(count) / float64(batchSize))

			for page := 0; page < int(pages); page++ {
				tasks, err := t.redisClient.ZRangeByScoreWithScores(context.Background(), delayedTasksKey, &redis.ZRangeBy{
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
					}

					if err := t.Dispatch(context.Background(), task.Task, task.Payload); err != nil {
						t.captureError(err)
					}
					t.redisClient.ZRem(context.Background(), delayedTasksKey, payload)
				}
			}

		case <-ctx.Done():
			return nil
		}
	}
}

func (t *TaskRunner) DispatchDelayed(ctx context.Context, taskName string, payload any, d time.Duration) error {
	_, ok := t.tasks.Get(taskName)
	if !ok {
		return ErrTaskNotFound
	}

	delayedTask := DelayedTask{
		Id:      uuid.NewString(),
		Time:    time.Now().Add(d).UnixMilli(),
		Payload: payload,
		Task:    taskName,
	}
	delayedTaskJson, err := json.Marshal(delayedTask)
	if err != nil {
		return err
	}

	return t.redisClient.ZAdd(ctx, delayedTasksKey, redis.Z{Score: float64(delayedTask.Time), Member: delayedTaskJson}).Err()
}

func (t *TaskRunner) ScheduleFor(ctx context.Context, taskName string, payload any, executionTime time.Time) error {
	_, ok := t.tasks.Get(taskName)
	if !ok {
		return ErrTaskNotFound
	}

	delayedTask := DelayedTask{
		Id:      uuid.NewString(),
		Time:    executionTime.UnixMilli(),
		Payload: payload,
		Task:    taskName,
	}
	delayedTaskJson, err := json.Marshal(delayedTask)
	if err != nil {
		return err
	}

	return t.redisClient.ZAdd(ctx, delayedTasksKey, redis.Z{Score: float64(delayedTask.Time), Member: delayedTaskJson}).Err()
}
