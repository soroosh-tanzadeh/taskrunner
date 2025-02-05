package redisstream

import (
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
)

type Option func(*RedisStreamMessageQueue)

func WithPrefix(prefix string) Option {
	return func(r *RedisStreamMessageQueue) {
		r.stream = prefix + ":" + r.stream
	}
}

func WithQueue(queue string) Option {
	return func(r *RedisStreamMessageQueue) {
		r.stream = strings.Split(r.stream, ":")[0] + ":" + queue
	}
}

func WithReClaimDelay(reClaimDelay time.Duration) Option {
	return func(r *RedisStreamMessageQueue) {
		r.reClaimDelay = reClaimDelay
	}
}

func WithRedisVersion(version string) Option {
	return func(r *RedisStreamMessageQueue) {
		r.redisVersion = semver.MustParse(version)
	}
}
func WithDeleteOnAck(deleteOnAck bool) Option {
	return func(r *RedisStreamMessageQueue) {
		r.deleteOnAck = deleteOnAck
	}
}

func WithFetchMethod(fetchMethod FetchMethod) Option {
	return func(r *RedisStreamMessageQueue) {
		r.fetchMethod = fetchMethod
	}
}
