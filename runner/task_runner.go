package runner

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"git.arvaninternal.ir/cdn-go-kit/taskrunner/contracts"
	"git.arvaninternal.ir/cdn-go-kit/taskrunner/internal/locker"
	"git.arvaninternal.ir/cdn-go-kit/taskrunner/internal/safemap"
	"github.com/redis/go-redis/v9"
)

const (
	ErrTaskRunnerAlreadyStarted = TaskRunnerError("ErrTaskRunnerAlreadyStarted")
	ErrRaceOccuredOnStart       = TaskRunnerError("ErrRaceOccuredOnStart")
	ErrTaskNotFound             = TaskRunnerError("TaskNotFound")
	ErrInvalidTaskPayload       = TaskRunnerError("ErrInvalidTaskPayload")

	ErrTaskMaxRetryExceed  = TaskRunnerError("ErrTaskMaxRetryExceed")
	ErrUniqueForIsRequired = TaskRunnerError("ErrUniqueForIsRequired")

	// It will happen when task is setted to be Unique and another task with same name and unique key dispached
	ErrTaskAlreadyDispatched = TaskRunnerError("ErrTaskAlreadyDispatched")
)

const (
	stateInit = iota
	stateStarting
	stateStarted
)
const metricsKeyPrefix = "taskrunner:"

type TaskRunner struct {
	status atomic.Uint64

	tasks *safemap.SafeMap[string, *Task]

	cfg TaskRunnerConfig

	activeWorkers atomic.Int64

	inFlight  atomic.Int64
	processed atomic.Int64
	fails     atomic.Int64

	queue contracts.MessageQueue

	wg sync.WaitGroup

	metricsHash string

	redisClient *redis.Client

	errorChannel chan error

	timingBulkWriter TimingBulkWriter

	locker contracts.DistributedLocker
}

func NewTaskRunner(cfg TaskRunnerConfig, client *redis.Client, queue contracts.MessageQueue) *TaskRunner {
	taskRunner := &TaskRunner{
		cfg:          cfg,
		queue:        queue,
		tasks:        safemap.NewSafeMap[string, *Task](),
		wg:           sync.WaitGroup{},
		metricsHash:  metricsKeyPrefix + cfg.ConsumerGroup + ":metrics",
		redisClient:  client,
		errorChannel: make(chan error),
	}
	if taskRunner.cfg.ReplicationFactor == 0 {
		taskRunner.cfg.ReplicationFactor = 1
	}
	taskRunner.timingBulkWriter = *NewBulkWriter(time.Second, taskRunner.timingFlush)
	taskRunner.locker = locker.NewRedisMutexLocker(taskRunner.redisClient)
	return taskRunner
}

func (t *TaskRunner) ErrorChannel() chan error {
	return t.errorChannel
}

func (t *TaskRunner) Start(ctx context.Context) error {
	if t.status.Load() != stateInit {
		return ErrTaskRunnerAlreadyStarted
	}
	if !t.status.CompareAndSwap(stateInit, stateStarting) {
		return ErrRaceOccuredOnStart
	}

	// Span n workers to start consuming messages
	for workerID := 1; workerID <= t.cfg.NumWorkers; workerID++ {
		t.wg.Add(1)
		go t.addWorker(ctx, workerID)
	}

	t.wg.Add(1)
	go func() {
		ticker := time.NewTicker(t.cfg.LongQueueThreshold / 2)
		defer t.wg.Done()
		for {
			select {
			case <-ticker.C:
				t.timingAggregator()
			case <-ctx.Done():
				return
			}
		}
	}()

	if !t.status.CompareAndSwap(stateStarting, stateStarted) {
		panic(ErrRaceOccuredOnStart)
	}

	t.wg.Wait()

	t.shutdown()
	return nil
}

func (t *TaskRunner) shutdown() {
	t.timingBulkWriter.close()
}

func (t *TaskRunner) Dispatch(ctx context.Context, taskName string, payload any) error {
	task, ok := t.tasks.Get(taskName)
	if !ok {
		return ErrTaskNotFound
	}

	var lock contracts.Lock

	onFail := func(err error) error {
		if task.Unique {
			if unlockResult, unlockErr := lock.Unlock(); unlockErr != nil || !unlockResult {
				return errors.Join(unlockErr, ErrTaskUnlockFailed, unlockErr)
			}
		}
		return err
	}

	taskMessage := task.CreateMessage(payload)

	// Unique Lock
	if task.Unique {
		lockKey := task.lockKey(payload)

		if task.UniqueFor == 0 {
			return ErrUniqueForIsRequired
		}
		lock, err := t.acquireUniqueLock(lockKey, time.Duration(task.UniqueFor)*time.Second)
		if err != nil {
			return ErrTaskAlreadyDispatched
		}
		taskMessage.UniqueLockValue = lock.Value()
	}

	jobMessageJson, err := json.Marshal(taskMessage)
	if err != nil {
		// Unlock Task immediately on error
		return onFail(err)
	}

	message := contracts.Message{Payload: string(jobMessageJson)}
	if err := t.queue.Add(ctx, &message); err != nil {
		// Unlock Task immediately on error
		return onFail(err)
	}
	return nil
}

func (t *TaskRunner) RegisterTask(task *Task) {
	t.tasks.Set(task.Name, task)
}
