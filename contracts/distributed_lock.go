package contracts

import "time"

type LockOptions struct {
	Expiry     time.Duration
	RetryDelay time.Duration
	Value      string
	Retries    int
}

type Lock interface {
	TryLock() error
	Lock() error
	Unlock() (bool, error)
	Value() string
}

type DistributedLocker interface {
	CreateMutexLock(name string, lockOptions LockOptions) Lock
}
