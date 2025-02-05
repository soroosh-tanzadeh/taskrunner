package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

func (t *TaskRunner) workerPanicHandler(r interface{}) {
	err, ok := r.(error)
	if ok {
		log.Errorf("Worker panic: %s", err.Error())
	} else {
		log.Errorf("Worker panic: %v", err)
	}
}

func executeTask(ctx context.Context, task *Task, payload any, resultChannel chan any) {
	defer close(resultChannel)

	// Handle Panic
	defer func() {
		if r := recover(); r != nil {
			err, ok := r.(error)
			logEntry := log.WithField("cause", r)
			if ok {
				logEntry = logEntry.WithError(err)
				resultChannel <- NewTaskExecutionError(task.Name, err)
			} else {
				resultChannel <- NewTaskExecutionError(task.Name, TaskRunnerError(fmt.Sprintf("Task %s Panic: %v", task.Name, err)))
			}

			logEntry.Errorf("Task %s Panic", task.Name)
		}
	}()
	// Note: Deferred function calls are pushed onto a stack.
	if err := task.Action(ctx, payload); err != nil {
		resultChannel <- NewTaskExecutionError(task.Name, err)
	}
	resultChannel <- true
}

func (t *TaskRunner) worker(i interface{}) {
	ctx := context.Background()
	arg := i.(fetchedMessage)

	m := arg.message
	consumerName := arg.consumer

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

	handleFailedTask := func(ctx context.Context, taskMessage TaskMessage, err error) {
		if err := t.cfg.FailedTaskHandler(ctx, taskMessage, err); err != nil {
			log.WithError(err).Error()
		} else {
			t.queue.Ack(ctx, t.ConsumerGroup(), m.GetId())
		}
	}

	taskMessage := TaskMessage{}
	// Set task id to message id
	taskMessage.ID = m.GetId()

	if err := json.Unmarshal([]byte(messagePayload), &taskMessage); err != nil {
		log.WithError(err).WithField("payload", m.Payload).Error("Can not parse message payload")
		failed()
		// When message payload is invalid retrying is meaningless
		taskMessage.Payload = m.Payload
		handleFailedTask(ctx, taskMessage, ErrInvalidTaskPayload)
		return
	}

	task, ok := t.tasks.Get(taskMessage.TaskName)
	if !ok {
		log.WithError(ErrTaskNotFound).Errorf("task %s not not fond", taskMessage.TaskName)
		failed()
		// Handle failed task (Store in database, Reschedule or etc.)
		// If failed task handler fails, it will be retried in next cycle
		handleFailedTask(ctx, taskMessage, ErrTaskNotFound)
		return
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
		handleFailedTask(ctx, taskMessage, ErrTaskMaxRetryExceed)
		return
	}

	// defer update timing to be update timing metrics
	defer func() {
		t.storeTiming(task.Name, time.Since(timeStart))
	}()

	// Release Lock and etc...
	defer t.afterProcess(taskMessage)

	// Execute Task
	resultChannel := make(chan any, 1)

	go executeTask(ctx, task, taskMessage.Payload, resultChannel)

	// Wait for execution result
	for {
		select {
		// Task Execution Finished
		case result := <-resultChannel:
			if _, ok := result.(bool); !ok {
				failed()
				t.captureError(result.(TaskExecutionError))
			} else {
				t.queue.Ack(ctx, t.ConsumerGroup(), m.GetId())
			}
			t.processed.Add(1)
			return
		// Task execution is taking time, send heartbeat to prevent reClaim
		case <-time.After(task.ReservationTimeout):
			if t.queue.RequireHeartHeartBeat() {
				if err := t.queue.HeartBeat(ctx, t.ConsumerGroup(), consumerName, m.GetId()); err != nil {
					t.captureError(err)
				}
			}
		}
	}
}

func (t *TaskRunner) afterProcess(message TaskMessage) {
	// Release Unique lock
	if message.Unique {
		err := t.releaseLock(message.UniqueKey, message.UniqueLockValue)
		if err != nil {
			t.captureError(err)
		}
	}
}

func (t *TaskRunner) consumerName() string {
	return t.cfg.ConsumerGroup + "_" + t.host
}
