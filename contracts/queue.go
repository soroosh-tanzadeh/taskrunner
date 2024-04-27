package contracts

import (
	"context"
	"time"
)

type MessageQueue interface {
	Len() (int64, error)
	Add(ctx context.Context, message *Message) error
	Receive(ctx context.Context, pullDuration time.Duration, batchSize int, group, consumerName string) ([]Message, error)
	Consume(ctx context.Context, readBatchSize int, blockDuration time.Duration, group, consumerName string, errorChannel chan error, consumer StreamConsumeFunc)
	Delete(ctx context.Context, id string) error
	Purge(ctx context.Context) error
	Ack(ctx context.Context, group, messageId string) error
}
