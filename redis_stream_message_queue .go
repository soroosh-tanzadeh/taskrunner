package taskq

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"git.arvaninternal.ir/cdn-go-kit/taskq/contracts"
	"github.com/redis/go-redis/v9"
)

const payloadKey = "payload"

type RedisStreamMessageQueue struct {
	client   *redis.Client
	queueKey string

	reClaimDelay time.Duration

	deleteOnConsume bool
}

func NewRedisStreamMessageQueue(redisClient *redis.Client, prefix, queue string, reClaimDelay time.Duration, deleteOnConsume bool) *RedisStreamMessageQueue {
	return &RedisStreamMessageQueue{
		client:          redisClient,
		queueKey:        prefix + ":" + queue,
		deleteOnConsume: deleteOnConsume,
		reClaimDelay:    reClaimDelay,
	}
}

func (r *RedisStreamMessageQueue) GetMetrics(ctx context.Context) (map[string]interface{}, error) {
	// TODO
	return nil, nil
}

// Push to queue and returns message ID
func (r *RedisStreamMessageQueue) Push(ctx context.Context, payload string) (string, error) {
	return r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: r.queueKey,
		Values: map[string]interface{}{
			"payload": payload,
		},
	}).Result()
}

// Receive Fetch data for the stream
// duration: The maximum time to block
func (r *RedisStreamMessageQueue) Receive(ctx context.Context, duration time.Duration, batchSize int, group, consumerName string) ([]contracts.Message, error) {
	readResult, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumerName,
		Count:    int64(batchSize),
		Streams:  []string{r.queueKey, ">"},
		Block:    duration,
	}).Result()
	if err != nil {
		// Create Group if not exists
		if strings.Contains(err.Error(), "NOGROUP") {
			if err := r.upsertConsumerGroup(group); err != nil {
				return nil, err
			}
			return r.Receive(ctx, duration, batchSize, group, consumerName)
		}

		return nil, err
	}

	if len(readResult) == 0 {
		return nil, contracts.ErrNoNewMessage
	}

	messages := make([]contracts.Message, len(readResult[0].Messages))
	for i, msg := range readResult[0].Messages {
		values := msg.Values
		id := msg.ID

		// Retry Count on stream list is always 1
		messages[i] = contracts.NewMessage(id, r.queueKey, values[payloadKey].(string), 1)
	}

	return messages, nil
}

func (r *RedisStreamMessageQueue) getPendingMessages(ctx context.Context, duration time.Duration, batchSize int, group, consumerName string) ([]PendingMesssage, error) {
	readResult, err := r.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Group:    group,
		Consumer: consumerName,
		Count:    int64(batchSize),
		Stream:   r.queueKey,
		Idle:     r.reClaimDelay,
		Start:    "-",
		End:      "+",
	}).Result()
	if err != nil {
		// Create Group if not exists
		if strings.Contains(err.Error(), "NOGROUP") {
			if err := r.upsertConsumerGroup(group); err != nil {
				return nil, err
			}
			// Re-call after group creation
			return r.getPendingMessages(ctx, duration, batchSize, group, consumerName)
		}

		if errors.Is(err, redis.Nil) {
			return nil, contracts.ErrNoNewMessage
		}

		return nil, err
	}

	if len(readResult) == 0 {
		return nil, contracts.ErrNoNewMessage
	}

	messages := make([]PendingMesssage, len(readResult))
	for i, msg := range readResult {
		messages[i] = PendingMesssage{
			ID:         msg.ID,
			Idle:       msg.Idle,
			RetryCount: msg.RetryCount,
		}
	}
	return messages, nil
}

func (r *RedisStreamMessageQueue) upsertConsumerGroup(group string) error {
	_, err := r.client.XGroupCreateMkStream(context.Background(), r.queueKey, group, "0-0").Result()
	if err != nil {
		// https://redis.io/commands/xgroup-create
		if strings.Contains(err.Error(), "BUSYGROUP") {
			return nil
		}
		return err
	}

	return nil
}

func (r *RedisStreamMessageQueue) getHeartBeatFunction(group, consumerName, messageID string) HeartBeatFunc {
	return func(ctx context.Context) error {
		// XCLAIM will increment the count of attempted deliveries of the message unless the JUSTID option has been specified
		// TODO how to write test for this??
		return r.client.XClaimJustID(ctx, &redis.XClaimArgs{
			Stream:   r.queueKey,
			Group:    group,
			Consumer: consumerName,
			MinIdle:  0,
			Messages: []string{messageID},
		}).Err()
	}
}

func (r *RedisStreamMessageQueue) Consume(ctx context.Context,
	readBatchSize int, blockDuration time.Duration,
	group, consumerName string,
	errorChannel chan error,
	consumer StreamConsumeFunc) {
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()

		r.processIncomingMessages(ctx, readBatchSize, blockDuration, group, consumerName, errorChannel, consumer)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(r.reClaimDelay)
	loop:
		for {
			select {
			case <-ticker.C:
				go r.processPendingMessages(ctx, readBatchSize, blockDuration, group, consumerName, errorChannel, consumer)
			case <-ctx.Done():
				break loop
			}
		}
	}()

	wg.Wait()
}

func (r *RedisStreamMessageQueue) Ack(ctx context.Context, group, messageId string) error {
	_, err := r.client.XAck(ctx, r.queueKey, group, messageId).Result()
	return err
}

func (r *RedisStreamMessageQueue) processPendingMessages(ctx context.Context,
	readBatchSize int, blockDuration time.Duration, group, consumerName string, errorChannel chan error, consumer StreamConsumeFunc) {
	// Return immediately if ctx is canceled
	select {
	case <-ctx.Done():
		return
	default:
	}

	pendings, err := r.getPendingMessages(ctx, blockDuration, readBatchSize, group, consumerName)
	if err != nil {
		if errors.Is(err, contracts.ErrNoNewMessage) {
			return
		}

		errorChannel <- err
		return
	}

	// Consume Messages
	for _, pendingInfo := range pendings {
		// Should not execute if context is canceled
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := r.client.XClaim(ctx, &redis.XClaimArgs{
			Stream:   r.queueKey,
			Group:    group,
			Consumer: consumerName,
			MinIdle:  0,
			Messages: []string{pendingInfo.ID},
		}).Err()
		if err != nil {
			errorChannel <- err
			continue
		}

		messages, err := r.client.XRange(ctx, r.queueKey, pendingInfo.ID, pendingInfo.ID).Result()
		if err != nil {
			errorChannel <- err
			continue
		}

		message := contracts.NewMessage(pendingInfo.ID, r.queueKey, messages[0].Values[payloadKey].(string), pendingInfo.RetryCount)

		// heartbeat is required for run running tasks, to prevent the task from being acquired by another worker
		consumeErr := consumer(ctx, message, r.getHeartBeatFunction(group, consumerName, message.GetId()))
		if consumeErr != nil {
			errorChannel <- consumeErr
			continue
		}

		// Remove task from PEL (Pending Entries List)
		if err := r.Ack(ctx, group, message.GetId()); err != nil {
			errorChannel <- err
		}

		if r.deleteOnConsume {
			// To prevent stream from getting larger and larger we can delete task after it proccessed
			if err := r.client.XDel(ctx, r.queueKey, message.GetId()).Err(); err != nil {
				errorChannel <- err
			}
		}
	}
}

func (r *RedisStreamMessageQueue) processIncomingMessages(ctx context.Context,
	readBatchSize int,
	blockDuration time.Duration,

	group,
	consumerName string,
	errorChannel chan error,

	consumer StreamConsumeFunc) {
	for {
		// Return immediately if ctx is canceled
		select {
		case <-ctx.Done():
			return
		default:
		}

		messages, err := r.Receive(ctx, blockDuration, readBatchSize, group, consumerName)
		if err != nil {
			if errors.Is(err, contracts.ErrNoNewMessage) {
				continue
			}

			errorChannel <- err
			continue
		}

		// Consume Messages
		for _, message := range messages {
			// Should not execute if context is canceled
			select {
			case <-ctx.Done():
				return
			default:
			}

			// heartbeat is required for run running tasks, to prevent the task from being acquired by another worker
			consumeErr := consumer(ctx, message, r.getHeartBeatFunction(group, consumerName, message.GetId()))
			if consumeErr != nil {
				errorChannel <- consumeErr
				continue
			}

			// Remove task from PEL (Pending Entries List)
			if err := r.Ack(ctx, group, message.GetId()); err != nil {
				errorChannel <- err
			}

			if r.deleteOnConsume {
				// To prevent stream from getting larger and larger we can delete task after it proccessed
				if err := r.client.XDel(ctx, r.queueKey, message.GetId()).Err(); err != nil {
					errorChannel <- err
				}
			}
		}
	}
}
