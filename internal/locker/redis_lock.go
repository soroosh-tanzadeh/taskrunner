package locker

import (
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncredis "github.com/go-redsync/redsync/v4/redis"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
	"github.com/soroosh-tanzadeh/taskrunner/contracts"
)

const DefaultRetryDuration = time.Duration(100) * time.Millisecond
const DefaultRetry = 10

type RedisMutexLocker struct {
	pool redsyncredis.Pool
	rs   *redsync.Redsync
}

func NewRedisMutexLocker(client *redis.Client) *RedisMutexLocker {
	pool := goredis.NewPool(client)
	rs := redsync.New(pool)
	return &RedisMutexLocker{
		pool: pool,
		rs:   rs,
	}
}

func (r *RedisMutexLocker) CreateMutexLock(name string, lockOptions contracts.LockOptions) contracts.Lock {
	var options []redsync.Option

	if lockOptions.Expiry > 0 {
		options = append(options, redsync.WithExpiry(lockOptions.Expiry))
	}
	if lockOptions.RetryDelay > 0 {
		options = append(options, redsync.WithRetryDelay(lockOptions.RetryDelay))
	} else {
		options = append(options, redsync.WithRetryDelay(DefaultRetryDuration))
	}

	if lockOptions.Retries > 0 {
		options = append(options, redsync.WithTries(lockOptions.Retries+1))
	} else {
		options = append(options, redsync.WithTries(DefaultRetry))
	}

	if len(lockOptions.Value) > 0 {
		options = append(options, redsync.WithValue(lockOptions.Value))
	}

	return r.rs.NewMutex(name, options...)
}
