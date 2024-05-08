package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"git.arvaninternal.ir/cdn-go-kit/taskrunner/contracts"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

func (t *TaskRunner) addWorker(ctx context.Context, workerID int) {
	t.activeWorkers.Add(1)
	defer func() {
		r := recover()
		if r != nil {
			err, ok := r.(error)
			logEntry := log.WithField("worker_id", workerID).WithField("cause", r)
			if ok {
				logEntry = logEntry.WithError(err)
			}
			logEntry.Error("Woker Panic")

			// Add new process to keep number of workers constant
			t.wg.Add(1)
			go t.addWorker(ctx, int(t.activeWorkers.Add(1)))
		}
	}()
	defer t.wg.Done()
	defer t.activeWorkers.Add(-1)
	t.process(ctx, workerID)
}

func (t *TaskRunner) process(ctx context.Context, workerID int) {
	batchSize := t.cfg.BatchSize
	consumerName := t.consumerName() + "_" + strconv.Itoa(workerID)

	executeTask := func(ctx context.Context, task *Task, payload any, resultChannel chan any) {
		// Close channel to prevent infinit for-loop
		defer close(resultChannel)
		// Handle Panic
		defer func() {
			if r := recover(); r != nil {
				err, ok := r.(error)
				logEntry := log.WithField("worker_id", workerID).WithField("cause", r)
				if ok {
					logEntry = logEntry.WithError(err)
					resultChannel <- err
				} else {
					resultChannel <- TaskRunnerError(fmt.Sprintf("Task %s execution failed", task.Name))
				}

				logEntry.Error("Task Panic")

			}
		}()
		// Note: Deferred function calls are pushed onto a stack.
		if err := task.Action(ctx, payload); err != nil {
			resultChannel <- err
		}
		resultChannel <- true
	}

	t.queue.Consume(ctx, batchSize, time.Second*5, t.cfg.ConsumerGroup, consumerName, t.errorChannel, func(ctx context.Context, m contracts.Message, hbf contracts.HeartBeatFunc) error {
		t.inFlight.Add(1)
		defer func() {
			t.inFlight.Add(-1)
		}()
		failed := func() {
			t.fails.Add(1)
		}

		// Start Time Tracking
		timeStart := time.Now()

		messagePayload := m.Payload

		taskMessage := TaskMessage{}
		if err := json.Unmarshal([]byte(messagePayload), &taskMessage); err != nil {
			log.WithError(err).WithField("payload", m.Payload).Error("Can not parse message payload")
			failed()
			// When message payload is invalid retrying is meaningless
			return t.cfg.FailedTaskHandler(ctx, TaskMessage{Payload: m.Payload}, ErrInvalidTaskPayload)
		}

		task, ok := t.tasks.Get(taskMessage.TaskName)
		if !ok {
			log.WithError(ErrTaskNotFound).Errorf("task %s not not fond", taskMessage.TaskName)
			failed()
			// Handle failed task (Store in database, Reschedule or etc.)
			// If failed task handler fails, it will be retried in next cycle
			return t.cfg.FailedTaskHandler(ctx, taskMessage, ErrTaskNotFound)
		}

		// Set lock value to unlock
		if taskMessage.Unique {
			task.lockValue = taskMessage.UniqueLockValue
		}

		// Handle Max retry
		if task.MaxRetry == 0 {
			task.MaxRetry = 1
		}

		if task.MaxRetry < int(m.GetReceiveCount()) {
			log.WithError(ErrTaskMaxRetryExceed).Errorf("task %s max retry exceed", taskMessage.TaskName)
			failed()
			// Handle failed task (Store in database, Reschedule or etc.)
			// If failed task handler fails, it will be retried in next cycle
			return t.cfg.FailedTaskHandler(ctx, taskMessage, ErrTaskMaxRetryExceed)
		}

		// defer update timing to be update timing metrics
		defer func() { t.storeTiming(task.Name, time.Since(timeStart)) }()

		// Release Lock and etc...
		defer t.afterProcess(task, taskMessage.Payload)

		// Execute Task
		resultChannel := make(chan any)

		go executeTask(ctx, task, taskMessage.Payload, resultChannel)

		// Wait for execution result
		for {
			select {
			// Task Execution Finished
			case result := <-resultChannel:
				if _, ok := result.(bool); !ok {
					failed()
					return result.(error)
				}
				t.processed.Add(1)
				return nil

			// Task execution is taking time, send heartbeat to prevent reClaim
			case <-time.After(task.ReservationTimeout):
				if err := hbf(ctx); err != nil {
					t.errorChannel <- err
				}
			}
		}
	})
}

func (t *TaskRunner) afterProcess(task *Task, payload any) {
	if task.Unique {
		err := t.releaseLock(task.lockKey(payload), task.lockValue)
		if err != nil {
			t.errorChannel <- err
		}
	}
}

func (t *TaskRunner) consumerName() string {
	consumerName := t.cfg.ConsumerGroup + "_"
	if host, err := os.Hostname(); err == nil {
		consumerName = consumerName + host
	} else {
		consumerName = consumerName + uuid.NewString()
	}
	return consumerName
}
