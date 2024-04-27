package ring

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func createRedisClient() *redis.Client {
	mr := miniredis.NewMiniRedis()
	mr.Start()

	return redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

func Test_Add_ShouldAddItemToList(t *testing.T) {
	client := createRedisClient()
	ring := NewRedisRing(client, 1000, "ring")
	err := ring.Add(context.Background(), 23)
	assert.Nil(t, err)
	r := client.LIndex(context.Background(), "ring:ring", 0).Val()
	assert.Equal(t, "23", r)
}

func Test_Add_ShouldPopLastToTopOfList_WhenLenghtExceed(t *testing.T) {
	client := createRedisClient()
	ring := NewRedisRing(client, 100, "ring")
	for i := 0; i < 200; i++ {
		err := ring.Add(context.Background(), float64(i))
		assert.Nil(t, err)
	}

	r := client.LIndex(context.Background(), "ring:ring", 0).Val()
	assert.Equal(t, "99", r)
}

func Test_Size_ShouldReturnListLength(t *testing.T) {
	client := createRedisClient()
	ring := NewRedisRing(client, 1000, "ring")
	for i := 0; i < 100; i++ {
		err := ring.Add(context.Background(), float64(i))
		assert.Nil(t, err)
	}
	assert.Equal(t, 100, ring.Size())
}

func Test_Get_ShouldReturnExpectedValueInList(t *testing.T) {
	client := createRedisClient()
	ring := NewRedisRing(client, 1000, "ring")
	for i := 0; i < 100; i++ {
		err := ring.Add(context.Background(), float64(i))
		assert.Nil(t, err)
	}
	assert.Equal(t, 100, ring.Size())
}

func Test_GetAll_ShouldOnlyReturnExistingCountOfValuesInsideTheRing(t *testing.T) {
	client := createRedisClient()
	ring := NewRedisRing(client, 1000, "ring")

	expectedCount := 102
	for i := 0; i < expectedCount; i++ {
		err := ring.Add(context.Background(), float64(i))
		assert.Nil(t, err)
	}

	all, err := ring.GetAll(context.Background())
	assert.Nil(t, err)

	assert.Equal(t, expectedCount, len(all))
}
