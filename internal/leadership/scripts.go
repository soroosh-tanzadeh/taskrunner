package leadership

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

var atomicSet = redis.NewScript(`
local key = KEYS[1]
local id = ARGV[1]
local ms = ARGV[2]

if (redis.call('SET', key, id, 'PX', ms, 'NX') == false) then
    return 0
else
    return 1
end
`)

var atomicRenew = redis.NewScript(`
local key = KEYS[1]
local id = ARGV[1]
local ms = ARGV[2]

if (id == redis.call('GET', key)) then
  redis.call('PEXPIRE', key, ms)
  return 1
else
  return 0
end
`)

var atomicDelete = redis.NewScript(`
local key = KEYS[1]
local id = ARGV[1]

if (redis.call('GET', key) == id) then
  redis.call('DEL', key)
  return 1
else
  return 0
end
`)

func registerScripts(r redis.Scripter) error {
	if err := atomicSet.Load(context.Background(), r).Err(); err != nil {
		return err
	}
	if err := atomicRenew.Load(context.Background(), r).Err(); err != nil {
		return err
	}
	if err := atomicDelete.Load(context.Background(), r).Err(); err != nil {
		return err
	}
	return nil
}

func doAtomicSet(r redis.Scripter, key string, id string, timeout time.Duration) (bool, error) {
	status := false
	v, err := atomicSet.Run(context.Background(), r, []string{key}, id, timeout.Milliseconds()).Result()
	if err != nil {
		return false, err
	}

	if i, ok := v.(int64); ok {
		status = i == 1
	}
	return status, nil
}

func doAtomicDelete(r redis.Scripter, key string, id string) (bool, error) {
	status := false
	v, err := atomicDelete.Run(context.Background(), r, []string{key}, id).Result()
	if err != nil {
		return false, err
	}

	if i, ok := v.(int64); ok {
		status = i == 1
	}
	return status, nil
}

func doAtomicRenew(r redis.Scripter, key string, id string, timeout time.Duration) (bool, error) {
	status := false
	v, err := atomicRenew.Run(context.Background(), r, []string{key}, id, timeout.Milliseconds()).Result()
	if err != nil {
		return false, err
	}

	if i, ok := v.(int64); ok {
		status = i == 1
	}
	return status, nil
}
