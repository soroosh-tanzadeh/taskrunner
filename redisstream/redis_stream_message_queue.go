package redisstream

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/redis/go-redis/v9"

	log "github.com/sirupsen/logrus"
	"github.com/soroosh-tanzadeh/taskrunner/contracts"
	"github.com/soroosh-tanzadeh/taskrunner/internal/ring"
)

const payloadKey = "payload"
const metricsSampleSize = 1000

type RedisStreamMessageQueue struct {
	client *redis.Client

	stream string

	reClaimDelay time.Duration

	messageProcessingMetrics *ring.RedisRing

	deleteOnConsume bool

	redisVersion *semver.Version
}

func NewRedisStreamMessageQueue(redisClient *redis.Client, prefix, queue string, reClaimDelay time.Duration, deleteOnConsume bool) *RedisStreamMessageQueue {
	stream := &RedisStreamMessageQueue{
		client:                   redisClient,
		stream:                   prefix + ":" + queue,
		deleteOnConsume:          deleteOnConsume,
		reClaimDelay:             reClaimDelay,
		messageProcessingMetrics: ring.NewRedisRing(redisClient, metricsSampleSize, prefix+":metrics:"+queue),
	}

	redisInfo, _ := redisClient.InfoMap(context.Background()).Result()
	if server, ok := redisInfo["Server"]; ok {
		if redis_version, ok := server["redis_version"]; ok {
			version, _ := semver.NewVersion(redis_version)
			stream.redisVersion = version
		}
	}

	return stream
}

func (r *RedisStreamMessageQueue) GetMetrics(ctx context.Context) (map[string]interface{}, error) {
	var metrics = make(map[string]interface{})

	info, err := r.client.XInfoStreamFull(ctx, r.stream, 1).Result()
	if err != nil {
		return nil, err
	}

	metrics["queue_size"] = info.Length
	metrics["message_claim_delay_avg"] = -1
	metrics["message_claim_delay_std"] = -1
	metrics["message_claim_delay_max"] = -1
	metrics["message_claim_delay_last"] = -1

	currentTime := time.Now()
	for _, group := range info.Groups {
		groupInfo := map[string]interface{}{
			"pending_entries_count": group.PelCount,
			"lag":                   group.Lag,
		}
		consumersInfo := make(map[string]interface{})
		for _, consumer := range group.Consumers {
			consumersInfo["name"] = consumer.Name
			consumersInfo["time_since_last_interaction"] = currentTime.Sub(consumer.SeenTime)
			consumersInfo["pending_count"] = consumer.PelCount
		}

		metrics["group_"+group.Name] = groupInfo
	}

	values, err := r.messageProcessingMetrics.GetAll(ctx)
	if err != nil {
		log.WithError(err).Error()
		return nil, err
	}

	// At least 10 values is required for this metrics to have a meaning
	if len(values) > 10 {
		messageClaimDelayAvg := ring.AverageFloat64(values)
		metrics["message_claim_delay_avg"] = messageClaimDelayAvg
		metrics["message_claim_delay_std"] = ring.StandardDeviationFloat64(values, messageClaimDelayAvg)
		metrics["message_claim_delay_max"] = ring.MaxFloat64(values)
		metrics["message_claim_delay_last"] = values[len(values)-1]
	}

	return metrics, nil
}

// Add to queue and returns message ID
func (r *RedisStreamMessageQueue) Add(ctx context.Context, message *contracts.Message) error {
	msgId, err := r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: r.stream,
		Values: map[string]interface{}{
			"payload": message.GetPayload(),
		},
	}).Result()
	if err != nil {
		return err
	}

	message.ID = msgId

	return nil
}

// Receive Fetch data for the stream
// duration: The maximum time to block
func (r *RedisStreamMessageQueue) Receive(ctx context.Context, duration time.Duration, batchSize int, group, consumerName string) ([]contracts.Message, error) {
	readResult, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumerName,
		Count:    int64(batchSize),
		Streams:  []string{r.stream, ">"},
		Block:    duration,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, contracts.ErrNoNewMessage
		}

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
		messages[i] = contracts.NewMessage(id, values[payloadKey].(string), 1)
	}

	return messages, nil
}

func (r *RedisStreamMessageQueue) getPendingMessages(ctx context.Context, duration time.Duration, batchSize int, group string) ([]PendingMesssage, error) {
	commandConfig := &redis.XPendingExtArgs{
		Group:  group,
		Count:  int64(batchSize),
		Stream: r.stream,
		Start:  "-",
		End:    "+",
	}
	if r.redisVersion != nil {
		compare := r.redisVersion.Compare(semver.MustParse("6.2"))
		if compare == 1 || compare == 0 {
			commandConfig.Idle = r.reClaimDelay
		}
	}

	readResult, err := r.client.XPendingExt(ctx, commandConfig).Result()
	if err != nil {
		// Create Group if not exists
		if strings.Contains(err.Error(), "NOGROUP") {
			if err := r.upsertConsumerGroup(group); err != nil {
				return nil, err
			}
			// Re-call after group creation
			return r.getPendingMessages(ctx, duration, batchSize, group)
		}

		if errors.Is(err, redis.Nil) {
			return nil, contracts.ErrNoNewMessage
		}

		return nil, err
	}

	if len(readResult) == 0 {
		return nil, contracts.ErrNoNewMessage
	}

	messages := make([]PendingMesssage, 0)
	for _, msg := range readResult {
		if msg.Idle < r.reClaimDelay {
			continue
		}

		messages = append(messages, PendingMesssage{
			ID:         msg.ID,
			Idle:       msg.Idle,
			RetryCount: msg.RetryCount,
		})
	}
	return messages, nil
}

func (r *RedisStreamMessageQueue) upsertConsumerGroup(group string) error {
	_, err := r.client.XGroupCreateMkStream(context.Background(), r.stream, group, "0-0").Result()
	if err != nil {
		// https://redis.io/commands/xgroup-create
		if strings.Contains(err.Error(), "BUSYGROUP") {
			return nil
		}
		return err
	}

	return nil
}

func (r *RedisStreamMessageQueue) getHeartBeatFunction(group, consumerName, messageID string) contracts.HeartBeatFunc {
	return func(ctx context.Context) error {
		// XCLAIM will increment the count of attempted deliveries of the message unless the JUSTID option has been specified
		// TODO how to write test for this??
		return r.client.XClaimJustID(ctx, &redis.XClaimArgs{
			Stream:   r.stream,
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
	consumer contracts.StreamConsumeFunc) {
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
		for {
			select {
			case <-ticker.C:
				go r.processPendingMessages(ctx, readBatchSize, blockDuration, group, consumerName, errorChannel, consumer)
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Minute * 10)
		if err := r.cleanup(group); err != nil {
			errorChannel <- err
		}
		for {
			select {
			case <-ticker.C:
				if err := r.cleanup(group); err != nil {
					errorChannel <- err
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()
}

func (r *RedisStreamMessageQueue) Ack(ctx context.Context, group, messageId string) error {
	_, err := r.client.XAck(ctx, r.stream, group, messageId).Result()
	return err
}

func (r *RedisStreamMessageQueue) processPendingMessages(ctx context.Context,
	readBatchSize int, blockDuration time.Duration, group, consumerName string, errorChannel chan error, consumer contracts.StreamConsumeFunc) {
	// Return immediately if ctx is canceled

	if ctx.Err() != nil {
		return
	}

	pendings, err := r.getPendingMessages(ctx, blockDuration, readBatchSize, group)
	if err != nil {
		if errors.Is(err, contracts.ErrNoNewMessage) {
			return
		}

		errorChannel <- err
		return
	}

	pendingIds := []string{}
	pendingsMap := make(map[string]PendingMesssage)
	for _, pendingInfo := range pendings {
		pendingIds = append(pendingIds, pendingInfo.ID)
		pendingsMap[pendingInfo.ID] = pendingInfo
	}

	messages, err := r.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   r.stream,
		Group:    group,
		Consumer: consumerName,
		MinIdle:  r.reClaimDelay,
		Messages: pendingIds,
	}).Result()

	// Consume Messages
	for _, msg := range messages {
		// Should not execute if context is canceled
		if ctx.Err() != nil {
			return
		}

		if err != nil {
			errorChannel <- err
			continue
		}

		messages, err := r.client.XRange(ctx, r.stream, msg.ID, msg.ID).Result()
		if err != nil {
			errorChannel <- err
			continue
		}

		if len(messages) == 0 {
			continue
		}

		message := contracts.NewMessage(msg.ID, messages[0].Values[payloadKey].(string), pendingsMap[msg.ID].RetryCount)

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
			if err := r.client.XDel(ctx, r.stream, message.GetId()).Err(); err != nil {
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

	consumer contracts.StreamConsumeFunc) {
	for {
		// Return immediately if ctx is canceled
		if ctx.Err() != nil {
			return
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
			startNano := time.Now().UnixNano()

			// Should not execute if context is canceled
			if ctx.Err() != nil {
				return
			}

			// heartbeat is required for run running tasks, to prevent the task from being acquired by another worker
			consumeErr := consumer(ctx, message, r.getHeartBeatFunction(group, consumerName, message.GetId()))
			if consumeErr != nil {
				errorChannel <- consumeErr
				continue
			}

			// Remove task from PEL (Pending Entries List)
			if err := r.Ack(context.Background(), group, message.GetId()); err != nil {
				errorChannel <- err
			}

			if r.deleteOnConsume {
				// To prevent stream from getting larger and larger we can delete task after it processed
				if err := r.client.XDel(context.Background(), r.stream, message.GetId()).Err(); err != nil {
					errorChannel <- err
				}
			}

			// storing metrics should not interrupt message processing
			duration := time.Now().UnixNano() - startNano
			go func() {
				if err := r.messageProcessingMetrics.Add(context.Background(), float64(duration)); err != nil {
					log.Error(err)
				}
			}()
		}
	}
}

func (r *RedisStreamMessageQueue) Delete(ctx context.Context, id string) error {
	return r.client.XDel(ctx, r.stream, id).Err()
}

func (r *RedisStreamMessageQueue) Purge(ctx context.Context) error {
	return r.client.Del(ctx, r.stream).Err()
}

func (r *RedisStreamMessageQueue) Len() (int64, error) {
	return r.client.XLen(context.Background(), r.stream).Result()
}

func (r *RedisStreamMessageQueue) cleanup(consumerGroup string) error {
	ctx := context.Background()
	idleThreshold := time.Minute * 10
	consumers, err := r.client.XInfoConsumers(ctx, r.stream, consumerGroup).Result()
	if err != nil {
		if strings.Contains(err.Error(), "NOGROUP") {
			return nil
		}
		return err
	}

	for _, consumer := range consumers {
		// Each consumer has an Idle field in milliseconds
		idleDuration := consumer.Idle

		if idleDuration > idleThreshold {
			if err := r.client.XGroupDelConsumer(context.Background(), r.stream, consumerGroup, consumer.Name).Err(); err != nil {
				log.WithError(err).Error("error occurred while deleting consumer")
			}
		}
	}
	return nil
}
