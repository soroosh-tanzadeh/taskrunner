package redisstream

import (
	"time"
)

type PendingMesssage struct {
	ID         string
	Idle       time.Duration
	RetryCount int64
}
