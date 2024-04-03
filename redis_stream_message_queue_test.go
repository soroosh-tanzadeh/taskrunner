package taskq

import (
	"context"
	"errors"
	"testing"
	"time"

	"git.arvaninternal.ir/cdn-go-kit/taskq/contracts"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func setupClient() *redis.Client {
	redisServer, err := miniredis.Run()
	if err != nil {
		panic(err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: redisServer.Addr(),
	})
	return client
}

func TestRedisStreamMessageQueue(t *testing.T) {
	client := setupClient()

	queue := NewRedisStreamMessageQueue(client, "test", "queue", 1, true)

	t.Run("Push", func(t *testing.T) {
		id, err := queue.Push(context.Background(), "test payload")
		assert.NoError(t, err)
		assert.NotEmpty(t, id)
	})

	t.Run("Receive", func(t *testing.T) {
		_, err := queue.Push(context.Background(), "test payload")
		assert.NoError(t, err)

		msg, err := queue.Receive(context.Background(), 1000, 1, "test", "test1")
		assert.NoError(t, err)
		assert.Equal(t, "test payload", msg[0].GetPayload())
	})

	t.Run("Ack", func(t *testing.T) {
		_, err := queue.Push(context.Background(), "test payload")
		assert.NoError(t, err)

		msg, err := queue.Receive(context.Background(), 1000, 1, "test", "test1")
		assert.NoError(t, err)
		err = queue.Ack(context.Background(), "test", msg[0].GetId())
		assert.NoError(t, err)
	})

	client.FlushAll(context.Background())
}

func TestRedisStreamMessageQueue_Consume_ShouldCallConsumeFunction(t *testing.T) {
	client := setupClient()

	queue := NewRedisStreamMessageQueue(client, "test", "queue", time.Second*20, true)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errorChannel := make(chan error)

	callChannel := make(chan string, 1)
	go func() {
		queue.Consume(ctx, 1, time.Second*4, "mygroup", "consumer1", errorChannel, func(ctx context.Context, msg contracts.Message, heartBeat HeartBeatFunc) error {
			assert.Equal(t, "test payload", msg.GetPayload())
			callChannel <- msg.GetId()
			return nil
		})
	}()

	id, err := queue.Push(context.Background(), "test payload")
	assert.NoError(t, err)

	select {
	case err := <-errorChannel:
		t.Error(err)
	case actualId := <-callChannel:
		assert.Equal(t, id, actualId)
	case <-time.After(time.Second * 5):
		t.Error("Should Call consume function")
	}
}
func TestRedisStreamMessageQueue_Consume_ShouldDeleteMessageAfterConsume(t *testing.T) {
	client := setupClient()

	queue := NewRedisStreamMessageQueue(client, "test", "queue", time.Second*20, true)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errorChannel := make(chan error)

	callChannel := make(chan string, 1)
	go func() {
		queue.Consume(ctx, 1, time.Second*4, "mygroup", "consumer1", errorChannel, func(ctx context.Context, msg contracts.Message, heartBeat HeartBeatFunc) error {
			assert.Equal(t, "test payload", msg.GetPayload())
			callChannel <- msg.GetId()
			return nil
		})
	}()

	id, err := queue.Push(context.Background(), "test payload")
	assert.NoError(t, err)

	// Wait for consumer to call delete
	time.Sleep(time.Second)

	select {
	case err := <-errorChannel:
		t.Error(err)
	case actualId := <-callChannel:
		assert.Equal(t, id, actualId)
		result, err := client.XRange(ctx, "test:queue", id, id).Result()
		assert.NoError(t, err)
		assert.Len(t, result, 0)
	case <-time.After(time.Second * 5):
		t.Error("Should Call consume function")
	}
}

func TestRedisStreamMessageQueue_Consume_ShouldRetryFailedMessage(t *testing.T) {
	client := setupClient()

	queue := NewRedisStreamMessageQueue(client, "test", "queue", time.Second*1, true)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	errorChannel := make(chan error)

	callChannel := make(chan string, 3)
	callCount := 0
	go func() {
		queue.Consume(ctx, 1, time.Second*4, "mygroup", "consumer1", errorChannel, func(ctx context.Context, msg contracts.Message, heartBeat HeartBeatFunc) error {
			assert.Equal(t, "test payload", msg.GetPayload())
			if callCount < 3 {
				callCount++
				return errors.New("fake")
			}

			callChannel <- msg.GetId()
			return nil
		})
	}()

	id, err := queue.Push(context.Background(), "test payload")
	assert.NoError(t, err)

	// Wait for consumer to call delete
	time.Sleep(time.Second)

	select {
	case err := <-errorChannel:
		if err.Error() != "fake" {
			t.Error(err)
		}
	case actualId := <-callChannel:
		assert.Equal(t, id, actualId)
		assert.Equal(t, 3, callCount)
	case <-time.After(time.Second * 5):
		t.Error("Should Call consume function")
	}
}
