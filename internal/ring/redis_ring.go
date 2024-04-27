package ring

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
)

// Ring Implementation of https://en.wikipedia.org/wiki/Circular_buffer
type RedisRing struct {
	client   *redis.Client
	len      int
	capacity int
	key      string
}

func NewRedisRing(redisClient *redis.Client, capacity int, key string) *RedisRing {
	return &RedisRing{
		client:   redisClient,
		capacity: capacity,
		len:      0,
		key:      "ring:" + key,
	}
}

func (r *RedisRing) Size() int {
	length, _ := r.client.LLen(context.Background(), r.key).Result()
	return int(length)
}

func (r *RedisRing) Add(ctx context.Context, item float64) error {
	length, err := r.client.LLen(ctx, r.key).Result()
	if err != nil {
		return err
	}

	if length >= int64(r.capacity) {
		if err := r.client.RPopLPush(ctx, r.key, r.key).Err(); err != nil {
			return err
		}
		return nil
	}
	return r.client.LPush(ctx, r.key, item).Err()
}

func (r *RedisRing) GetAll(ctx context.Context) ([]float64, error) {
	res, err := r.client.LRange(ctx, r.key, 0, int64(r.capacity)).Result()
	if err != nil {
		return []float64{}, err
	}

	total := len(res)

	if total < 1 {
		return []float64{}, nil
	}

	result := make([]float64, total)

	for i := 0; i < total; i++ {
		v, err := strconv.ParseFloat(res[i], 64)
		if err != nil {
			log.WithError(err).Error()
			continue
		}

		result[i] = v
	}

	return result, err
}
