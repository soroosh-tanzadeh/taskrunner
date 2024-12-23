package contracts

import (
	"context"
	"time"
)

// MessageQueue defines the contract for a message queue system.
// This interface provides methods for message management, including adding, receiving,
// consuming, and acknowledging messages.
type MessageQueue interface {
	Len() (int64, error)                                                                                                                                          // Returns the number of messages in the queue.
	Add(ctx context.Context, message *Message) error                                                                                                              // Adds a new message to the queue.
	Receive(ctx context.Context, pullDuration time.Duration, batchSize int, group, consumerName string) ([]Message, error)                                        // Receives messages from the queue.
	Consume(ctx context.Context, readBatchSize int, blockDuration time.Duration, group, consumerName string, errorChannel chan error, consumer StreamConsumeFunc) // Consumes messages using the provided consumer function.
	Delete(ctx context.Context, id string) error                                                                                                                  // Deletes a specific message from the queue.
	Purge(ctx context.Context) error                                                                                                                              // Clears all messages from the queue.
	Ack(ctx context.Context, group, messageId string) error                                                                                                       // Acknowledges the processing of a message.
	RequireHeartHeartBeat() bool                                                                                                                                  // Indicates if a heartbeat is required for message processing.
	HeartBeat(ctx context.Context, consumer, group, messageId string) error                                                                                       // Sends a heartbeat for a specific message.                                                                       // Sends a heartbeat for a specific message.
}
