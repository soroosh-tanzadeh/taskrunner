package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/soroosh-tanzadeh/taskrunner/contracts"
	"github.com/soroosh-tanzadeh/taskrunner/redisstream"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing context
type TaskRunnerTestSuit struct {
	suite.Suite
	redisServer *miniredis.Miniredis
}

func TestTaskRunnerTestSuit(t *testing.T) {
	suite.Run(t, new(TaskRunnerTestSuit))
}

func (t *TaskRunnerTestSuit) setupRedis() *redis.Client {
	redisServer, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	t.redisServer = redisServer

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisServer.Addr(),
		PoolSize: 30,
	})

	return redisClient
}

func (t *TaskRunnerTestSuit) setupTaskRunner(redisClient *redis.Client) (contracts.MessageQueue, *TaskRunner) {
	queue := redisstream.NewRedisStreamMessageQueue(redisClient, "test", "queue", time.Second*10, true)
	taskRunner := NewTaskRunner(TaskRunnerConfig{
		BatchSize:          5,
		ConsumerGroup:      "test_group",
		ConsumersPrefix:    "taskrunner",
		NumWorkers:         10,
		LongQueueThreshold: time.Second * 10,
		ReplicationFactor:  1,
		FailedTaskHandler: func(_ context.Context, _ TaskMessage, err error) error {
			return nil
		},
	}, redisClient, queue)

	return queue, taskRunner
}

func (t *TaskRunnerTestSuit) Test_ShouldExecuteTask() {
	callChannel := make(chan bool)
	expectedPayload := "Test Payload"
	_, taskRunner := t.setupTaskRunner(t.setupRedis())

	taskRunner.RegisterTask(&Task{
		Name:               "task",
		MaxRetry:           10,
		ReservationTimeout: time.Second,
		Action: func(ctx context.Context, payload any) error {
			t.Assert().Equal(expectedPayload, payload)
			fmt.Println("Hello From TaskRunner")
			callChannel <- true
			return nil
		},
		Unique: false,
	})
	go func() {
		taskRunner.Start(context.Background())
	}()

	err := taskRunner.Dispatch(context.Background(), "task", expectedPayload)
	t.Assert().NoError(err)

	select {
	case <-callChannel:
		break
	case err := <-taskRunner.ErrorChannel():
		t.Fail(err.Error())
	case <-time.After(time.Second):
		t.FailNow("Task was not excuted")
	}
}

func (t *TaskRunnerTestSuit) Test_ShouldRetryTaskBaseOnMaxRetry() {
	callChannel := make(chan bool)
	counter := atomic.Int64{}
	expectedPayload := "Test Payload"
	expectedError := errors.New("I'm Panic Error")
	expectedErrorWrap := NewTaskExecutionError("task", expectedError)
	_, taskRunner := t.setupTaskRunner(t.setupRedis())

	taskRunner.RegisterTask(&Task{
		Name:               "task",
		MaxRetry:           10,
		ReservationTimeout: time.Second,
		Action: func(ctx context.Context, payload any) error {
			t.Assert().Equal(payload, expectedPayload)
			fmt.Println("Hello From TaskRunner")
			counter.Add(1)
			if counter.Load() == 10 {
				callChannel <- true
				return nil
			}
			return expectedError
		},
		Unique: false,
	})
	go func() {
		taskRunner.Start(context.Background())
	}()

	err := taskRunner.Dispatch(context.Background(), "task", expectedPayload)
	t.Assert().NoError(err)

	select {
	case <-callChannel:
		t.Assert().Equal(10, counter.Load())
		break
	case err := <-taskRunner.ErrorChannel():
		if err.(TaskExecutionError).GetError().Error() != expectedErrorWrap.GetError().Error() {
			t.FailNow(err.Error())
		}
	case <-time.After(time.Second):
		t.FailNow("Task was not excuted")
	}
}

func (t *TaskRunnerTestSuit) Test_ShouldRetryTaskWhenPaniced() {
	callChannel := make(chan bool)
	counter := atomic.Int64{}
	expectedPayload := "Test Payload"
	expectedError := errors.New("I'm Panic Error")
	expectedErrorWrap := NewTaskExecutionError("task", expectedError)
	_, taskRunner := t.setupTaskRunner(t.setupRedis())

	taskRunner.RegisterTask(&Task{
		Name:               "task",
		MaxRetry:           10,
		ReservationTimeout: time.Second,
		Action: func(ctx context.Context, payload any) error {
			t.Assert().Equal(payload, expectedPayload)
			fmt.Println("Hello From TaskRunner")
			counter.Add(1)
			if counter.Load() == 10 {
				callChannel <- true
				return nil
			}
			panic(expectedError)
		},
		Unique: false,
	})
	go func() {
		taskRunner.Start(context.Background())
	}()

	err := taskRunner.Dispatch(context.Background(), "task", expectedPayload)
	t.Assert().NoError(err)

	select {
	case <-callChannel:
		t.Assert().Equal(10, counter.Load())
		break
	case err := <-taskRunner.ErrorChannel():
		if err.(TaskExecutionError).GetError().Error() != expectedErrorWrap.GetError().Error() {
			t.FailNow(err.Error())
		}
	case <-time.After(time.Second):
		t.FailNow("Task was not excuted")
	}
}

func (t *TaskRunnerTestSuit) Test_ShouldNotDispatchDuplicatedTasks() {
	callChannel := make(chan bool)
	counter := atomic.Int64{}
	expectedPayload := "Test Payload"
	redisClient := t.setupRedis()
	_, taskRunner := t.setupTaskRunner(redisClient)

	taskRunner.RegisterTask(&Task{
		Name:               "task",
		MaxRetry:           1,
		ReservationTimeout: time.Second,
		Action: func(ctx context.Context, payload any) error {
			t.Assert().Equal(payload, expectedPayload)
			fmt.Println("Hello From TaskRunner")
			counter.Add(1)
			callChannel <- true
			return nil
		},
		Unique:    true,
		UniqueFor: 300,
		UniqueKey: func(payload any) string {
			return "hello"
		},
	})
	go func() {
		taskRunner.Start(context.Background())
	}()

	err := taskRunner.Dispatch(context.Background(), "task", expectedPayload)
	t.Assert().NoError(err)

	err = taskRunner.Dispatch(context.Background(), "task", expectedPayload)
	t.Assert().ErrorIs(err, ErrTaskAlreadyDispatched)

	val, err := redisClient.Get(context.Background(), "taskrunner:unique:task:hello").Result()
	t.Assert().NoError(err)

	t.Assert().NotEmpty(val)
	select {
	case <-callChannel:
		t.Assert().Equal(int64(1), counter.Load())
		<-time.After(time.Millisecond * 200)
		_, err := redisClient.Get(context.Background(), "taskrunner:unique:task:hello").Result()
		t.Assert().ErrorIs(err, redis.Nil)
		break
	case err := <-taskRunner.ErrorChannel():
		t.FailNow(err.Error())
	case <-time.After(time.Second):
		t.FailNow("Task was not excuted")
	}
}

func (t *TaskRunnerTestSuit) Test_ShouldSendHeartbeatForLongRunnintTasks() {
	callChannel := make(chan bool)
	hbfCounter := atomic.Int64{}
	expectedPayload := "Test Payload"
	redisClient := t.setupRedis()

	taskOptions := &Task{
		Name:               "task",
		MaxRetry:           1,
		ReservationTimeout: time.Millisecond * 10,
		Action: func(ctx context.Context, payload any) error {
			<-time.After(time.Millisecond * 100)
			callChannel <- true
			return nil
		},
	}
	mockQueue := NewMockMessageQueue(t.T())
	mockQueue.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		taskmessageJson, err := json.Marshal(taskOptions.CreateMessage(expectedPayload))
		t.Require().NoError(err)
		wokerFunc := args[6].(contracts.StreamConsumeFunc)
		wokerFunc(context.Background(), contracts.NewMessage(uuid.NewString(), string(taskmessageJson), 1), func(ctx context.Context) error {
			hbfCounter.Add(1)
			return nil
		})
	}).Return(nil)
	mockQueue.On("Add", mock.Anything, mock.Anything).Return(nil)
	taskRunner := NewTaskRunner(TaskRunnerConfig{
		BatchSize:          100,
		ConsumerGroup:      "test",
		ConsumersPrefix:    "test",
		LongQueueThreshold: time.Second * 5,
		NumWorkers:         1,
	}, redisClient, mockQueue)

	taskRunner.RegisterTask(taskOptions)
	go func() {
		taskRunner.Start(context.Background())
	}()

	err := taskRunner.Dispatch(context.Background(), "task", expectedPayload)
	t.Assert().NoError(err)
	select {
	case <-callChannel:
		t.Assert().Equal(int64(9), hbfCounter.Load())
		break
	case err := <-taskRunner.ErrorChannel():
		t.FailNow(err.Error())
	case <-time.After(time.Second):
		t.FailNow("Task was not excuted")
	}
}

func (t *TaskRunnerTestSuit) Test_ShouldHandleWorkerPanic() {
	callChannel := make(chan bool)
	consumeCallCounter := atomic.Int64{}
	expectedPayload := "Test Payload"
	redisClient := t.setupRedis()
	taskOptions := &Task{
		Name:               "task",
		MaxRetry:           1,
		ReservationTimeout: time.Millisecond * 10,
		Action: func(ctx context.Context, payload any) error {
			<-time.After(time.Millisecond * 100)
			callChannel <- true
			return nil
		},
	}
	mockQueue := NewMockMessageQueue(t.T())
	mockQueue.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		consumeCallCounter.Add(1)
		if consumeCallCounter.Load() == 1 {
			panic(errors.New("I'm Panic"))
		}

		taskmessageJson, err := json.Marshal(taskOptions.CreateMessage(expectedPayload))
		t.Require().NoError(err)
		wokerFunc := args[6].(contracts.StreamConsumeFunc)
		wokerFunc(context.Background(), contracts.NewMessage(uuid.NewString(), string(taskmessageJson), 1), func(ctx context.Context) error {
			return nil
		})
	}).Return(nil)
	mockQueue.On("Add", mock.Anything, mock.Anything).Return(nil)
	taskRunner := NewTaskRunner(TaskRunnerConfig{
		BatchSize:          100,
		ConsumerGroup:      "test",
		ConsumersPrefix:    "test",
		LongQueueThreshold: time.Second * 10,
		NumWorkers:         2,
	}, redisClient, mockQueue)

	taskRunner.RegisterTask(taskOptions)
	go func() {
		taskRunner.Start(context.Background())
	}()

	err := taskRunner.Dispatch(context.Background(), "task", expectedPayload)
	t.Assert().NoError(err)
	select {
	case <-callChannel:
		time.Sleep(time.Millisecond * 30)
		t.Assert().Equal(int64(2), taskRunner.activeWorkers.Load())
		break
	case err := <-taskRunner.ErrorChannel():
		t.FailNow(err.Error())
	case <-time.After(time.Second):
		t.FailNow("Task was not excuted")
	}
}

func (t *TaskRunnerTestSuit) Test_ShouldCallFailedTaskHandler_WhenMaxRtryExceed() {
	callChannel := make(chan bool)
	expectedPayload := "Test Payload"
	expectedError := errors.New("I'm Panic Error")
	redisClient := t.setupRedis()
	queue := redisstream.NewRedisStreamMessageQueue(redisClient, "test", "queue", time.Millisecond*100, true)
	taskRunner := NewTaskRunner(TaskRunnerConfig{
		BatchSize:          100,
		ConsumerGroup:      "test_group",
		ConsumersPrefix:    "taskrunner",
		NumWorkers:         10,
		LongQueueThreshold: time.Second * 10,
		FailedTaskHandler: func(_ context.Context, taskMessage TaskMessage, err error) error {
			t.Assert().Equal(taskMessage.Payload, expectedPayload)
			t.Assert().ErrorIs(err, ErrTaskMaxRetryExceed)

			callChannel <- true
			return nil
		},
	}, redisClient, queue)
	taskRunner.RegisterTask(&Task{
		Name:               "task",
		MaxRetry:           3,
		ReservationTimeout: time.Millisecond,
		Action: func(ctx context.Context, payload any) error {
			return expectedError
		},
		Unique: false,
	})
	go func() {
		taskRunner.Start(context.Background())
	}()

	err := taskRunner.Dispatch(context.Background(), "task", expectedPayload)
	t.Assert().NoError(err)

	select {
	case <-callChannel:
		break
	case <-time.After(time.Second):
		t.FailNow("Task was not excuted")
	}
}

func (t *TaskRunnerTestSuit) Test_ShouldCallFailedTaskHandler_WhenTaskNotExist() {
	callChannel := make(chan bool)
	expectedPayload := "Test Payload"
	expectedError := errors.New("I'm Panic Error")
	redisClient := t.setupRedis()
	queue := redisstream.NewRedisStreamMessageQueue(redisClient, "test", "queue", time.Millisecond*100, true)
	taskRunner := NewTaskRunner(TaskRunnerConfig{
		BatchSize:          100,
		ConsumerGroup:      "test_group",
		ConsumersPrefix:    "taskrunner",
		LongQueueThreshold: time.Second * 10,
		NumWorkers:         10,
		FailedTaskHandler: func(_ context.Context, taskMessage TaskMessage, err error) error {
			t.Assert().Equal(taskMessage.Payload, expectedPayload)
			t.Assert().ErrorIs(err, ErrTaskNotFound)
			callChannel <- true
			return nil
		},
	}, redisClient, queue)
	taskRunner.RegisterTask(&Task{
		Name:               "task",
		MaxRetry:           3,
		ReservationTimeout: time.Millisecond,
		Action: func(ctx context.Context, payload any) error {
			return expectedError
		},
		Unique: false,
	})
	err := taskRunner.Dispatch(context.Background(), "task", expectedPayload)
	taskRunner.tasks.Delete("task")

	go func() {
		taskRunner.Start(context.Background())
	}()

	t.Assert().NoError(err)
	select {
	case <-callChannel:
		break
	case <-time.After(time.Second):
		t.FailNow("Task was not excuted")
	}
}

func (t *TaskRunnerTestSuit) Test_ShouldCallFailedTaskHandler_WhenPayloadIsInvalid() {
	callChannel := make(chan bool)
	expectedPayload := "INVALIDJSONPAYLOAD"
	expectedError := errors.New("I'm Panic Error")
	redisClient := t.setupRedis()
	queue := redisstream.NewRedisStreamMessageQueue(redisClient, "test", "queue", time.Millisecond*100, true)
	taskRunner := NewTaskRunner(TaskRunnerConfig{
		BatchSize:          100,
		ConsumerGroup:      "test_group",
		ConsumersPrefix:    "taskrunner",
		NumWorkers:         10,
		LongQueueThreshold: time.Second * 10,
		FailedTaskHandler: func(_ context.Context, taskMessage TaskMessage, err error) error {
			t.Assert().Equal(taskMessage.Payload, expectedPayload)
			t.Assert().ErrorIs(err, ErrInvalidTaskPayload)
			callChannel <- true
			return nil
		},
	}, redisClient, queue)
	taskRunner.RegisterTask(&Task{
		Name:               "task",
		MaxRetry:           3,
		ReservationTimeout: time.Millisecond,
		Action: func(ctx context.Context, payload any) error {
			return expectedError
		},
		Unique: false,
	})
	go func() {
		taskRunner.Start(context.Background())
	}()

	err := queue.Add(context.Background(), &contracts.Message{Payload: expectedPayload})
	t.Assert().NoError(err)

	select {
	case <-callChannel:
		break
	case <-time.After(time.Second):
		t.FailNow("Task was not excuted")
	}
}

func (t *TaskRunnerTestSuit) Test_ShouldNotDispatch_WhenTaskIsUniqueButDoesntHaveUniqueFor() {
	_, taskRunner := t.setupTaskRunner(t.setupRedis())

	taskRunner.RegisterTask(&Task{
		Name:               "task",
		MaxRetry:           3,
		ReservationTimeout: time.Millisecond,
		Action: func(ctx context.Context, payload any) error {
			return nil
		},
		Unique: true,
	})
	go func() {
		taskRunner.Start(context.Background())
	}()

	err := taskRunner.Dispatch(context.Background(), "task", "")
	t.Assert().ErrorIs(err, ErrUniqueForIsRequired)
}

func (t *TaskRunnerTestSuit) Test_ShouldUseTaskNameForUnique_WhenUniqueKeyIsNil() {
	redisClient := t.setupRedis()
	_, taskRunner := t.setupTaskRunner(redisClient)

	taskRunner.RegisterTask(&Task{
		Name:               "task",
		MaxRetry:           3,
		ReservationTimeout: time.Millisecond,
		Action: func(ctx context.Context, payload any) error {
			return nil
		},
		Unique:    true,
		UniqueFor: 5,
	})

	err := taskRunner.Dispatch(context.Background(), "task", "")
	t.Assert().NoError(err)

	v, err := redisClient.Get(context.Background(), uniqueLockPrefix+"task").Result()
	t.Assert().NoError(err)
	t.Assert().NotEmpty(v)
}

func (t *TaskRunnerTestSuit) Test_timingAggregator_ShouldAggregateAndStoreTimingAverage() {
	redisClient := t.setupRedis()
	queue := redisstream.NewRedisStreamMessageQueue(redisClient, "test", "queue", time.Millisecond*100, true)
	taskRunner := NewTaskRunner(TaskRunnerConfig{
		BatchSize:          1,
		ConsumerGroup:      "test_group",
		ConsumersPrefix:    "taskrunner",
		NumWorkers:         1,
		LongQueueThreshold: time.Millisecond,
		ReplicationFactor:  1,
		FailedTaskHandler: func(_ context.Context, _ TaskMessage, err error) error {
			return nil
		},
	}, redisClient, queue)
	taskRunner.RegisterTask(&Task{
		Name:               "task",
		MaxRetry:           3,
		ReservationTimeout: time.Millisecond,
		Action: func(ctx context.Context, payload any) error {
			<-time.After(time.Millisecond * 10)
			return nil
		},
	})
	for i := 0; i < 5; i++ {
		err := taskRunner.Dispatch(context.Background(), "task", "test")
		t.Assert().NoError(err)
	}

	go func() {
		taskRunner.Start(context.Background())
	}()

	<-time.After(time.Second * 5)

	value, err := redisClient.HGet(context.Background(), taskRunner.metricsHash, "task_avg").Float64()
	t.Assert().NoError(err)
	t.Assert().LessOrEqual(math.Floor(value), 12.0)
}

func (t *TaskRunnerTestSuit) Test_timingAggregator_ShouldCallLongQueueWhenLongQueueIsHappening() {
	callChannel := make(chan Stats)
	redisClient := t.setupRedis()
	queue := redisstream.NewRedisStreamMessageQueue(redisClient, "test", "queue", time.Second*2, true)
	taskRunner := NewTaskRunner(TaskRunnerConfig{
		BatchSize:          1,
		ConsumerGroup:      "test_group",
		ConsumersPrefix:    "taskrunner",
		NumWorkers:         4,
		LongQueueThreshold: time.Millisecond * 100,
		ReplicationFactor:  1,
		FailedTaskHandler: func(_ context.Context, _ TaskMessage, err error) error {
			return nil
		},
	}, redisClient, queue)
	taskRunner.cfg.LongQueueHook = func(s Stats) {
		callChannel <- s
	}

	taskRunner.RegisterTask(&Task{
		Name:               "task",
		MaxRetry:           1,
		ReservationTimeout: time.Second * 2,
		Action: func(ctx context.Context, payload any) error {
			<-time.After(time.Millisecond * 500)
			return nil
		},
	})
	for i := 0; i < 16; i++ {
		err := taskRunner.Dispatch(context.Background(), "task", "test-task")
		t.Assert().NoError(err)
	}

	go func() {
		taskRunner.Start(context.Background())
	}()

	select {
	case s := <-callChannel:
		t.Assert().GreaterOrEqual(math.Floor(s.PredictedWaitTime), 100.0)
	case <-time.After(time.Second * 5):
		t.FailNow("LongQueueHook not called")
	}
}

func (t *TaskRunnerTestSuit) Test_timingAggregator_ShouldNotCallLongQueueWhenThereAreEnoughWorkersToProcessLongQueue() {
	// The queue is expected to be empty in approximately 350 milliseconds
	callChannel := make(chan Stats)
	redisClient := t.setupRedis()
	queue := redisstream.NewRedisStreamMessageQueue(redisClient, "test", "queue", time.Second*2, true)
	taskRunner := NewTaskRunner(TaskRunnerConfig{
		BatchSize:          5,
		ConsumerGroup:      "test_group",
		ConsumersPrefix:    "taskrunner",
		NumWorkers:         30,
		LongQueueThreshold: time.Millisecond * 600,
		ReplicationFactor:  1,
		FailedTaskHandler: func(_ context.Context, _ TaskMessage, err error) error {
			return nil
		},
	}, redisClient, queue)
	taskRunner.cfg.LongQueueHook = func(s Stats) {
		callChannel <- s
	}

	taskRunner.RegisterTask(&Task{
		Name:               "task",
		MaxRetry:           1,
		ReservationTimeout: time.Second * 2,
		Action: func(ctx context.Context, payload any) error {
			<-time.After(time.Millisecond * 500)
			return nil
		},
	})
	for i := 0; i < 20; i++ {
		err := taskRunner.Dispatch(context.Background(), "task", "test-task")
		t.Assert().NoError(err)
	}

	go func() {
		taskRunner.Start(context.Background())
	}()

	select {
	case <-callChannel:
		t.FailNow("LongQueueHook called")
	case <-time.After(time.Second * 5):
	}
}

func (t *TaskRunnerTestSuit) Test_DispatchDelayed_ShouldStoreTaskForGivenTime() {
	redisClient := t.setupRedis()
	queue := redisstream.NewRedisStreamMessageQueue(redisClient, "test", "queue", time.Second*2, true)
	taskRunner := NewTaskRunner(TaskRunnerConfig{
		BatchSize:          1,
		ConsumerGroup:      "test_group",
		ConsumersPrefix:    "taskrunner",
		NumWorkers:         4,
		LongQueueThreshold: time.Millisecond * 100,
		ReplicationFactor:  1,
		FailedTaskHandler: func(_ context.Context, _ TaskMessage, err error) error {
			return nil
		},
	}, redisClient, queue)
	taskRunner.RegisterTask(&Task{
		Name:               "task",
		MaxRetry:           1,
		ReservationTimeout: time.Second * 2,
		Action: func(ctx context.Context, payload any) error {
			return nil
		},
	})

	taskRunner.DispatchDelayed(context.Background(), "task", "Hello world", time.Minute)

	entries, err := t.redisServer.ZMembers(delayedTasksKey)
	t.Assert().Nil(err)
	t.Len(entries, 1)

	var delayedTask DelayedTask
	err = json.Unmarshal([]byte(entries[0]), &delayedTask)
	t.Assert().Nil(err)

	t.Assert().Equal("task", delayedTask.Task)
	t.Assert().Equal("Hello world", delayedTask.Payload)
}

func (t *TaskRunnerTestSuit) Test_ScheduleFor_ShouldStoreTaskForGivenTime() {
	redisClient := t.setupRedis()
	queue := redisstream.NewRedisStreamMessageQueue(redisClient, "test", "queue", time.Second*2, true)
	taskRunner := NewTaskRunner(TaskRunnerConfig{
		BatchSize:          1,
		ConsumerGroup:      "test_group",
		ConsumersPrefix:    "taskrunner",
		NumWorkers:         4,
		LongQueueThreshold: time.Millisecond * 100,
		ReplicationFactor:  1,
		FailedTaskHandler: func(_ context.Context, _ TaskMessage, err error) error {
			return nil
		},
	}, redisClient, queue)
	taskRunner.RegisterTask(&Task{
		Name:               "task",
		MaxRetry:           1,
		ReservationTimeout: time.Second * 2,
		Action: func(ctx context.Context, payload any) error {
			return nil
		},
	})
	expectedTime := time.Now().Add(time.Minute)
	taskRunner.ScheduleFor(context.Background(), "task", "Hello world", expectedTime)

	entries, err := t.redisServer.ZMembers(delayedTasksKey)
	t.Assert().Nil(err)
	t.Len(entries, 1)

	score, err := t.redisServer.ZScore(delayedTasksKey, entries[0])
	t.Assert().Nil(err)
	t.Equal(expectedTime.Unix(), int64(score))
	var delayedTask DelayedTask
	err = json.Unmarshal([]byte(entries[0]), &delayedTask)
	t.Assert().Nil(err)

	t.Assert().Equal("task", delayedTask.Task)
	t.Assert().Equal("Hello world", delayedTask.Payload)
}

func (t *TaskRunnerTestSuit) Test_ShouldStartTaskAtExpectedTime() {
	callChannel := make(chan time.Time)

	redisClient := t.setupRedis()
	queue := redisstream.NewRedisStreamMessageQueue(redisClient, "test", "queue", time.Second*2, true)
	taskRunner := NewTaskRunner(TaskRunnerConfig{
		BatchSize:          1,
		ConsumerGroup:      "test_group",
		ConsumersPrefix:    "taskrunner",
		NumWorkers:         4,
		LongQueueThreshold: time.Millisecond * 100,
		ReplicationFactor:  1,
		FailedTaskHandler: func(_ context.Context, _ TaskMessage, err error) error {
			return nil
		},
	}, redisClient, queue)
	taskRunner.RegisterTask(&Task{
		Name:               "task",
		MaxRetry:           1,
		ReservationTimeout: time.Second * 2,
		Action: func(ctx context.Context, payload any) error {
			callChannel <- time.Now()
			return nil
		},
	})
	go taskRunner.Start(context.Background())
	go taskRunner.StartDelayedSchedule(context.Background(), 100)

	dispatchTime := time.Now()
	taskRunner.DispatchDelayed(context.Background(), "task", "Hello world", time.Second*5)

	select {
	case execTime := <-callChannel:
		t.Assert().WithinRange(execTime, dispatchTime.Add(time.Second*5), dispatchTime.Add(time.Second*6))
	case <-time.After(time.Second * 10):
		t.FailNow("it should execute task")
	}
}
