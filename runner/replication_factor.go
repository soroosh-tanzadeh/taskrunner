package runner

import (
	"context"
	"time"
)

const replicationPrefix = "taskrunner:replication:"

func (t *TaskRunner) registerReplication() error {
	if _, err := t.redisClient.Set(context.Background(), replicationPrefix+t.host, t.id, time.Second*10).Result(); err != nil {
		return err
	}

	return nil
}

func (t *TaskRunner) renewReplication() {
	if _, err := t.redisClient.Set(context.Background(), replicationPrefix+t.host, t.id, time.Second*10).Result(); err != nil {
		t.captureError(err)
	}
}

func (t *TaskRunner) removeReplication() {
	if _, err := t.redisClient.Del(context.Background(), replicationPrefix+t.host).Result(); err != nil {
		t.captureError(err)
	}
}
