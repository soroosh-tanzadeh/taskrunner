package redisstream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/soroosh-tanzadeh/taskrunner/contracts"
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

	t.Run("Add_ShouldSetID", func(t *testing.T) {
		msg := contracts.Message{
			Payload: "test payload",
		}
		err := queue.Add(context.Background(), &msg)
		assert.NoError(t, err)
		assert.NotEmpty(t, msg.ID)
	})

	t.Run("Receive", func(t *testing.T) {
		msg := contracts.Message{
			Payload: "test payload",
		}
		err := queue.Add(context.Background(), &msg)
		assert.NoError(t, err)

		msgReceived, err := queue.Receive(context.Background(), 1000, 1, "test", "test1")
		assert.NoError(t, err)
		assert.Equal(t, "test payload", msgReceived[0].GetPayload())
	})

	t.Run("Ack", func(t *testing.T) {
		msg := contracts.Message{
			Payload: "test payload",
		}
		err := queue.Add(context.Background(), &msg)
		assert.NoError(t, err)

		msgReceived, err := queue.Receive(context.Background(), 1000, 1, "test", "test1")
		assert.NoError(t, err)
		err = queue.Ack(context.Background(), "test", msgReceived[0].GetId())
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
		queue.Consume(ctx, 1, time.Second*4, "mygroup", "consumer1", errorChannel, func(ctx context.Context, msg contracts.Message, heartBeat contracts.HeartBeatFunc) error {
			assert.Equal(t, "test payload", msg.GetPayload())
			callChannel <- msg.GetId()
			return nil
		})
	}()

	msg := contracts.Message{
		Payload: "test payload",
	}
	err := queue.Add(context.Background(), &msg)
	assert.NoError(t, err)

	select {
	case err := <-errorChannel:
		t.Error(err)
	case actualId := <-callChannel:
		assert.Equal(t, msg.ID, actualId)
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
		queue.Consume(ctx, 1, time.Second*4, "mygroup", "consumer1", errorChannel, func(ctx context.Context, msg contracts.Message, heartBeat contracts.HeartBeatFunc) error {
			assert.Equal(t, "test payload", msg.GetPayload())
			callChannel <- msg.GetId()
			return nil
		})
	}()

	msg := contracts.Message{
		Payload: "test payload",
	}
	err := queue.Add(context.Background(), &msg)
	assert.NoError(t, err)

	// Wait for consumer to call delete
	time.Sleep(time.Second)

	select {
	case err := <-errorChannel:
		t.Error(err)
	case actualId := <-callChannel:
		assert.Equal(t, msg.ID, actualId)
		result, err := client.XRange(ctx, "test:queue", msg.ID, msg.ID).Result()
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
	mutex := sync.Mutex{}
	go func() {
		queue.Consume(ctx, 1, time.Second*4, "mygroup", "consumer1", errorChannel, func(ctx context.Context, msg contracts.Message, heartBeat contracts.HeartBeatFunc) error {
			assert.Equal(t, "test payload", msg.GetPayload())
			mutex.Lock()
			defer mutex.Unlock()
			if callCount < 3 {
				callCount++
				return errors.New("fake")
			}

			callChannel <- msg.GetId()
			return nil
		})
	}()

	msg := contracts.Message{
		Payload: "test payload",
	}
	err := queue.Add(context.Background(), &msg)
	assert.NoError(t, err)

	// Wait for consumer to call delete
	time.Sleep(time.Second * 3)

	select {
	case err := <-errorChannel:
		if err.Error() != "fake" {
			t.FailNow()
		}
	case actualId := <-callChannel:
		assert.Equal(t, msg.ID, actualId)
		assert.Equal(t, 3, callCount)
	case <-time.After(time.Second * 5):
		t.Error("Should Call consume function")
	}
}

func Test_Delete_ShouldRemoveMessageFromStream(t *testing.T) {
	client := setupClient()

	queue := NewRedisStreamMessageQueue(client, "test", "queue", time.Second*1, true)

	msg := contracts.Message{
		Payload: "test payload",
	}
	err := queue.Add(context.Background(), &msg)
	assert.NoError(t, err)

	queue.Delete(context.Background(), msg.ID)

	result, err := client.XRange(context.Background(), "test:queue", msg.ID, msg.ID).Result()
	assert.NoError(t, err)
	assert.Len(t, result, 0)
}

func Test_Purge_ShouldRemoveAllMessagesFromStream(t *testing.T) {
	client := setupClient()

	queue := NewRedisStreamMessageQueue(client, "test", "queue", time.Second*1, true)

	for i := 0; i < 10; i++ {
		err := queue.Add(context.Background(), &contracts.Message{
			Payload: "test payload",
		})
		assert.NoError(t, err)
	}

	queue.Purge(context.Background())

	result, err := client.XRange(context.Background(), "test:queue", "0", "+").Result()
	assert.NoError(t, err)
	assert.Len(t, result, 0)
}

func Test_Len_ShouldReturnQueueLen(t *testing.T) {
	client := setupClient()

	queue := NewRedisStreamMessageQueue(client, "test", "queue", time.Second*1, true)

	for i := 0; i < 10; i++ {
		err := queue.Add(context.Background(), &contracts.Message{
			Payload: "test payload",
		})
		assert.NoError(t, err)
	}

	len, err := queue.Len()
	assert.NoError(t, err)

	assert.Equal(t, int64(10), len)
}

func Test_ShouldNotDeleteConsumers_WhenDeleteOnShutdownIsSettedToFalse(t *testing.T) {
	client := setupClient()

	queue := NewRedisStreamMessageQueue(client, "test", "queue", time.Second*1, true)

	for i := 0; i < 10; i++ {
		err := queue.Add(context.Background(), &contracts.Message{
			Payload: "test payload",
		})
		assert.NoError(t, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	errorChannel := make(chan error)

	go func() {
		queue.Consume(ctx, 1, time.Second*4, "mygroup", "consumer1", errorChannel, func(ctx context.Context, msg contracts.Message, heartBeat contracts.HeartBeatFunc) error {
			assert.Equal(t, "test payload", msg.GetPayload())
			return nil
		})
	}()

	<-time.After(time.Second * 5)
	cancel()

	result, _ := client.XInfoConsumers(context.Background(), "test:queue", "mygroup").Result()
	assert.Equal(t, len(result), 1)
}
