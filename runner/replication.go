package runner

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const replicationPrefix = "taskrunner:replication:"
const replicationRenewDuration = time.Second * 10
const replicationKeyTTL = time.Second * 15

func (t *TaskRunner) registerReplication() error {
	t.id = uuid.NewString()
	if _, err := t.redisClient.Set(context.Background(), replicationPrefix+t.host, t.id, replicationKeyTTL).Result(); err != nil {
		return err
	}

	return nil
}

func (t *TaskRunner) startRenewReplication(ctx context.Context) {
	ticker := time.NewTicker(replicationRenewDuration)
	for {
		select {
		case <-ticker.C:
			if _, err := t.redisClient.Set(context.Background(), replicationPrefix+t.host, t.id, replicationKeyTTL).Result(); err != nil {
				t.captureError(err)
			}
		case <-ctx.Done():
			t.removeReplication()
			return
		}
	}
}

func (t *TaskRunner) GetNumberOfReplications() (int, error) {
	replicationFactor, exist := t.cache.Get("replication_factor")
	if !exist {
		keys, err := t.redisClient.Keys(context.Background(), replicationPrefix+"*").Result()
		if err != nil {
			return 0, err
		}
		replicationFactor = len(keys)
		t.cache.Set("replication_factor", replicationFactor, replicationRenewDuration)
	}

	return replicationFactor, nil
}

func (t *TaskRunner) removeReplication() {
	if _, err := t.redisClient.Del(context.Background(), replicationPrefix+t.host).Result(); err != nil {
		t.captureError(err)
	}
}
