package runner

import (
	"errors"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/soroosh-tanzadeh/taskrunner/contracts"
)

const uniqueLockPrefix = "taskrunner:unique:"
const ErrTaskUnlockFailed = TaskRunnerError("ErrTaskUnlockFailed")

func (t *TaskRunner) acquireUniqueLock(uniqueKey string, expiry time.Duration) (contracts.Lock, error) {
	// Only the consumer that acquired the unique lock can unlock the job
	lock := t.locker.CreateMutexLock(uniqueLockPrefix+uniqueKey, contracts.LockOptions{
		Expiry:     expiry,
		RetryDelay: time.Millisecond,
		Retries:    2,
	})
	return lock, lock.Lock()
}

func (t *TaskRunner) releaseLock(uniqueKey, value string) error {
	result, err := t.locker.CreateMutexLock(uniqueLockPrefix+uniqueKey, contracts.LockOptions{
		RetryDelay: 100 * time.Millisecond,
		Retries:    5,
		Value:      value,
	}).Unlock()
	if err != nil {
		if errors.Is(err, redsync.ErrLockAlreadyExpired) {
			return nil
		}
		return err
	}
	if !result {
		return ErrTaskUnlockFailed
	}

	return nil
}
