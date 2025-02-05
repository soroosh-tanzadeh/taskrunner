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

type FetchMethod string

const (
	FetchNewest = FetchMethod("NEWEST")
	FetchOldest = FetchMethod("OLDEST")
)

// RedisStreamMessageQueue represents a message queue implemented using Redis streams.
// Messages require heartbeats to prevent reclaiming by other consumers in Redis Streams. (Do not forget to call HeartBeat function, while using Fetch)
// It supports features like pending message processing, metrics collection, and automatic
// message acknowledgment and deletion after consumption.
type RedisStreamMessageQueue struct {
	client *redis.Client

	stream string

	reClaimDelay time.Duration

	messageProcessingMetrics *ring.RedisRing

	deleteOnAck bool

	redisVersion *semver.Version

	lastPendingCheckTime time.Time
	checkPendingLock     *sync.Mutex

	fetchMethod FetchMethod
}

// NewRedisStreamMessageQueueWithOptions creates a new RedisStreamMessageQueue with the provided Redis client and optional configuration.
// The function initializes the queue with the following default values:
//   - Stream: "default:queue" (prefix: "default", queue: "queue")
//   - DeleteOnAck: true (messages are automatically deleted after acknowledgment)
//   - ReClaimDelay: 5 minutes (delay before reclaiming unacknowledged messages)
//   - FetchMethod: FetchOldest (messages are fetched in the order they were added)
//   - Metrics: A RedisRing is created with a default prefix of "default:metrics:queue"
//
// Use the provided Option functions (e.g., WithPrefix, WithQueue, WithReClaimDelay, WithDeleteOnAck, WithFetchMethod)
// to override these defaults and customize the behavior of the queue.
//
// Example:
//
//	redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	queue := NewRedisStreamMessageQueueWithOptions(
//	    redisClient,
//	    WithPrefix("myapp"),
//	    WithQueue("myqueue"),
//	    WithReClaimDelay(10*time.Minute),
//	    WithDeleteOnAck(false),
//	    WithFetchMethod(FetchNewest),
//	)
func NewRedisStreamMessageQueueWithOptions(redisClient *redis.Client, options ...Option) *RedisStreamMessageQueue {
	stream := &RedisStreamMessageQueue{
		client:                   redisClient,
		stream:                   "default:queue", // Default prefix and queue
		deleteOnAck:              true,            // Default value
		reClaimDelay:             5 * time.Minute, // Default value
		messageProcessingMetrics: ring.NewRedisRing(redisClient, metricsSampleSize, "default:metrics:queue"),
		checkPendingLock:         &sync.Mutex{},
		fetchMethod:              FetchOldest, // Default value
	}

	// Apply options to override defaults
	for _, option := range options {
		option(stream)
	}

	// Fetch Redis version
	if stream.redisVersion == nil {
		redisInfo, _ := redisClient.InfoMap(context.Background()).Result()
		if server, ok := redisInfo["Server"]; ok {
			if redis_version, ok := server["redis_version"]; ok {
				version, _ := semver.NewVersion(redis_version)
				stream.redisVersion = version
			}
		}
	}

	return stream
}

func NewRedisStreamMessageQueue(redisClient *redis.Client, prefix, queue string, reClaimDelay time.Duration, deleteOnAck bool) *RedisStreamMessageQueue {
	stream := &RedisStreamMessageQueue{
		client:                   redisClient,
		stream:                   prefix + ":" + queue,
		deleteOnAck:              deleteOnAck,
		reClaimDelay:             reClaimDelay,
		messageProcessingMetrics: ring.NewRedisRing(redisClient, metricsSampleSize, prefix+":metrics:"+queue),
		checkPendingLock:         &sync.Mutex{},
		fetchMethod:              FetchOldest,
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

// GetMetrics retrieves queue-related metrics, including queue size and message claim delays.
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

func (r *RedisStreamMessageQueue) RequireHeartHeartBeat() bool {
	return true
}

// Receive retrieves messages from the Redis stream.
// Messages require heartbeats to ensure they are not reclaimed by other consumers.
// If no messages are available, it blocks for the specified duration.
func (r *RedisStreamMessageQueue) fetchMessage(ctx context.Context, duration time.Duration, batchSize int, group, consumerName string) ([]contracts.Message, error) {
	id := ">"
	if r.fetchMethod == FetchNewest {
		id = "^"
	}

	readResult, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumerName,
		Count:    int64(batchSize),
		Streams:  []string{r.stream, id},
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
			return r.fetchMessage(ctx, duration, batchSize, group, consumerName)
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

func (r *RedisStreamMessageQueue) HeartBeat(ctx context.Context, group, consumerName, messageID string) error {
	_, err := r.client.XClaimJustID(ctx, &redis.XClaimArgs{
		Stream:   r.stream,
		Group:    group,
		Consumer: consumerName,
		MinIdle:  0,
		Messages: []string{messageID},
	}).Result()
	return err
}

func (r *RedisStreamMessageQueue) getHeartBeatFunction(group, consumerName, messageID string) contracts.HeartBeatFunc {
	return func(ctx context.Context) error {
		// XCLAIM will increment the count of attempted deliveries of the message unless the JUSTID option has been specified
		// TODO how to write test for this??
		return r.HeartBeat(ctx, group, consumerName, messageID)
	}
}

// Fetch retrieves messages from the Redis stream.
// It first checks for pending messages and reclaims them if they are idle beyond the reClaimDelay.
// If no pending messages are available, it fetches new messages from the stream.
// This method ensures efficient processing of messages by prioritizing pending ones before consuming new messages.
// Messages fetched using this function may require acknowledgment or deletion based on the queue's configuration.
func (r *RedisStreamMessageQueue) Receive(ctx context.Context,
	blockDuration time.Duration, batchSize int, group, consumerName string) ([]contracts.Message, error) {
	shouldProcessPendingMessage := false
	var messages []contracts.Message = make([]contracts.Message, 0)
	var err error

	if r.checkPendingLock.TryLock() {
		shouldProcessPendingMessage = time.Since(r.lastPendingCheckTime) >= r.reClaimDelay
		defer r.checkPendingLock.Unlock()
	}

	if shouldProcessPendingMessage {
		// Fetch Pending Messages
		r.lastPendingCheckTime = time.Now()

		pendings, err := r.getPendingMessages(ctx, blockDuration, batchSize, group)
		if err != nil {
			if errors.Is(err, contracts.ErrNoNewMessage) {
				goto ReceiveMessage
			}

			return nil, err
		}
		if len(pendings) > 0 {
			pendingIds := []string{}
			pendingsMap := make(map[string]PendingMesssage)
			for _, pendingInfo := range pendings {
				pendingIds = append(pendingIds, pendingInfo.ID)
				pendingsMap[pendingInfo.ID] = pendingInfo
			}
			rawMessages, err := r.client.XClaim(ctx, &redis.XClaimArgs{
				Stream:   r.stream,
				Group:    group,
				Consumer: consumerName,
				MinIdle:  r.reClaimDelay,
				Messages: pendingIds,
			}).Result()
			if err != nil {
				return nil, err
			}

			// Convert them to messages
			for _, msg := range rawMessages {
				values := msg.Values
				id := msg.ID

				// Retry Count on stream list is always 1
				messages = append(messages, contracts.NewMessage(id, values[payloadKey].(string), pendingsMap[id].RetryCount))
			}
		}

		return messages, nil
	}

ReceiveMessage:
	messages, err = r.fetchMessage(ctx, blockDuration, batchSize, group, consumerName)
	if err != nil {
		if errors.Is(err, contracts.ErrNoNewMessage) {
			return messages, nil
		}
		return nil, err
	}

	return messages, nil
}

// Consume starts consuming messages from the Redis stream using the provided consumer function.
// This method manages incoming and pending messages, ensuring their processing or reclamation.
func (r *RedisStreamMessageQueue) Consume(ctx context.Context,
	batchSize int, blockDuration time.Duration,
	group, consumerName string,
	errorChannel chan error,
	consumer contracts.StreamConsumeFunc) {
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		r.processIncomingMessages(ctx, batchSize, blockDuration, group, consumerName, errorChannel, consumer)
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

// Ack acknowledges the processing of a specific message by its ID.
func (r *RedisStreamMessageQueue) Ack(ctx context.Context, group, messageId string) error {
	_, err := r.client.XAck(ctx, r.stream, group, messageId).Result()

	if r.deleteOnAck {
		// To prevent stream from getting larger and larger we can delete task after it processed
		if err := r.client.XDel(ctx, r.stream, messageId).Err(); err != nil {
			log.Error(err)
		}
	}

	return err
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

// Delete removes a specific message from the Redis stream by its ID.
func (r *RedisStreamMessageQueue) Delete(ctx context.Context, id string) error {
	return r.client.XDel(ctx, r.stream, id).Err()
}

// Purge deletes all messages from the Redis stream.
func (r *RedisStreamMessageQueue) Purge(ctx context.Context) error {
	return r.client.Del(ctx, r.stream).Err()
}

// Len returns the total number of messages in the Redis stream.
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
