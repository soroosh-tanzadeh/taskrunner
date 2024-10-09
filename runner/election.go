package runner

import (
	"context"
	"time"

	"github.com/soroosh-tanzadeh/taskrunner/election"
)

func (t *TaskRunner) IsLeader() bool {
	return t.isLeader.Load()
}

func (t *TaskRunner) StartElection(ctx context.Context) {
	elector, OnPromote, onDemote, OnError := election.NewElector(election.Opts{
		Redis:    t.redisClient,
		Key:      t.ConsumerGroup(),
		JitterMS: 5,
		TTL:      time.Second * 5,
		Wait:     time.Second * 6,
	})

	go func() {
		for {
			select {
			case <-OnPromote:
				t.isLeader.Store(true)
			case <-onDemote:
				t.isLeader.Store(false)
			case err := <-OnError:
				t.captureError(err)
			case <-ctx.Done():
				elector.Stop()
				return
			}
		}
	}()

	elector.Start()

	t.elector = elector
}
