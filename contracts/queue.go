package contracts

import (
	"context"
	"time"
)

type MessageQueue interface {
	Push(ctx context.Context, payload string) (string, error)
	Receive(ctx context.Context, pullDuration time.Duration, batchSize int, group, consumerName string) ([]Message, error)
	Ack(ctx context.Context, group, messageId string) error
}
