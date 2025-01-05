package runner

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/panjf2000/ants/v2"
	"github.com/redis/go-redis/v9"
	"github.com/soroosh-tanzadeh/taskrunner/contracts"
	"github.com/soroosh-tanzadeh/taskrunner/election"
	"github.com/soroosh-tanzadeh/taskrunner/internal/cache"
	"github.com/soroosh-tanzadeh/taskrunner/internal/locker"
	"github.com/soroosh-tanzadeh/taskrunner/internal/safemap"
)

const (
	ErrTaskRunnerAlreadyStarted = TaskRunnerError("ErrTaskRunnerAlreadyStarted")
	ErrRaceOccuredOnStart       = TaskRunnerError("ErrRaceOccuredOnStart")
	ErrTaskNotFound             = TaskRunnerError("TaskNotFound")
	ErrInvalidTaskPayload       = TaskRunnerError("ErrInvalidTaskPayload")

	ErrTaskMaxRetryExceed              = TaskRunnerError("ErrTaskMaxRetryExceed")
	ErrUniqueForIsRequired             = TaskRunnerError("ErrUniqueForIsRequired")
	ErrDurationIsSmallerThanCheckCycle = TaskRunnerError("ErrDurationIsSmallerThanCheckCycle")

	// It will happen when task is setted to be Unique and another task with same name and unique key dispached
	ErrTaskAlreadyDispatched = TaskRunnerError("ErrTaskAlreadyDispatched")
)

const (
	stateInit = iota
	stateStarting
	stateStarted
	stateStopped
)
const metricsKeyPrefix = "taskrunner:"

type TaskRunner struct {
	id string

	elector *election.Elector

	cache *cache.Cache[string, int]

	isLeader *atomic.Bool

	status atomic.Uint64

	tasks *safemap.SafeMap[string, *Task]

	cfg TaskRunnerConfig

	inFlight  atomic.Int64
	processed atomic.Int64
	fails     atomic.Int64

	workerPool *ants.PoolWithFunc

	queue contracts.MessageQueue

	wg sync.WaitGroup

	metricsHash string

	redisClient *redis.Client

	errorChannel chan error

	tasksTimingBulkWriter *TimingBulkWriter

	locker contracts.DistributedLocker

	host string
}

func NewTaskRunner(cfg TaskRunnerConfig, client *redis.Client, queue contracts.MessageQueue) *TaskRunner {
	taskRunner := &TaskRunner{
		cfg:          cfg,
		queue:        queue,
		tasks:        safemap.NewSafeMap[string, *Task](),
		wg:           sync.WaitGroup{},
		metricsHash:  metricsKeyPrefix + cfg.ConsumerGroup + ":metrics",
		redisClient:  client,
		isLeader:     &atomic.Bool{},
		cache:        cache.New[string, int](time.Minute, time.Minute),
		errorChannel: make(chan error),
	}
	if taskRunner.cfg.ReplicationFactor == 0 {
		taskRunner.cfg.ReplicationFactor = 1
	}
	if len(taskRunner.cfg.Host) == 0 {
		hostName, err := os.Hostname()
		if err != nil {
			hostName = uuid.NewString()
		}
		taskRunner.cfg.Host = hostName
	}
	taskRunner.host = taskRunner.cfg.Host

	// For Backward compatibility
	if taskRunner.cfg.NumFetchers == 0 {
		taskRunner.cfg.NumFetchers = cfg.BatchSize
	}

	taskRunner.tasksTimingBulkWriter = NewBulkWriter(time.Second, taskRunner.timingFlush)

	taskRunner.locker = locker.NewRedisMutexLocker(taskRunner.redisClient)
	return taskRunner
}

func (t *TaskRunner) ConsumerGroup() string {
	return t.cfg.ConsumerGroup
}

func (t *TaskRunner) GetQueue() contracts.MessageQueue {
	return t.queue
}

func (t *TaskRunner) ErrorChannel() chan error {
	return t.errorChannel
}

func (t *TaskRunner) captureError(err error) {
	t.errorChannel <- err
}

func (t *TaskRunner) Start(ctx context.Context) error {
	if t.status.Load() != stateInit {
		return ErrTaskRunnerAlreadyStarted
	}
	if !t.status.CompareAndSwap(stateInit, stateStarting) {
		return ErrRaceOccuredOnStart
	}

	// Span n workers to start consuming messages
	pool, err := ants.NewPoolWithFunc(t.cfg.NumWorkers, t.worker, ants.WithPanicHandler(t.workerPanicHandler))
	if err != nil {
		return err
	}
	t.workerPool = pool

	for fetcherID := 1; fetcherID <= t.cfg.NumFetchers; fetcherID++ {
		t.wg.Add(1)
		go t.addFetcher(ctx, fetcherID)
	}

	// Register Replication
	if err := t.registerReplication(); err != nil {
		return err
	}

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		t.startRenewReplication(ctx)
	}()

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()

		var ticker *time.Ticker
		if t.cfg.LongQueueThreshold > 0 {
			ticker = time.NewTicker(t.cfg.LongQueueThreshold / 2)
		} else {
			ticker = time.NewTicker(time.Minute)
		}

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

	t.StartElection(ctx)

	t.wg.Wait()

	// Wait at most 30 seconds for workers to stop
	t.workerPool.ReleaseTimeout(time.Second * 30)

	t.status.Store(stateStopped)

	t.shutdown()
	return nil
}

func (t *TaskRunner) shutdown() {
	t.tasksTimingBulkWriter.close()
	close(t.errorChannel)
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
