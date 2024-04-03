package taskq

import (
	"context"
	"time"

	"git.arvaninternal.ir/cdn-go-kit/taskq/contracts"
)

type HeartBeatFunc func(ctx context.Context) error

type StreamConsumeFunc func(context.Context, contracts.Message, HeartBeatFunc) error

type PendingMesssage struct {
	ID         string
	Idle       time.Duration
	RetryCount int64
}
