package election

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type leaderWatcher struct {
	lock                       *sync.RWMutex
	promoted, demoted, errored *atomic.Bool
	lastError                  error
}

func (l *leaderWatcher) reset() {
	l.promoted.Store(false)
	l.demoted.Store(false)
	l.errored.Store(false)

	l.lock.Lock()
	defer l.lock.Unlock()
	l.lastError = nil
}

func makeRedis(t *testing.T) *redis.Client {
	redisServer := os.Getenv("REDIS_SERVER")
	if len(redisServer) == 0 {
		redisServer = "127.0.0.1:6379"
	}
	cli := redis.NewClient(&redis.Options{Addr: redisServer})
	err := cli.FlushDB(context.Background()).Err()
	require.NoError(t, err)
	err = cli.ScriptFlush(context.Background()).Err()
	require.NoError(t, err)

	return cli
}

func makeWatcher(opts Opts) (*Elector, *leaderWatcher) {
	lead, promote, demote, err := NewElector(opts)
	watcher := &leaderWatcher{promoted: &atomic.Bool{}, demoted: &atomic.Bool{}, errored: &atomic.Bool{}, lock: &sync.RWMutex{}}
	go func() {
		for {
			select {
			case <-promote:
				watcher.promoted.Store(true)
			case <-demote:
				watcher.demoted.Store(true)
			case err := <-err:
				watcher.lock.Lock()
				watcher.errored.Store(true)
				watcher.lastError = err
				watcher.lock.Unlock()

			}
		}
	}()
	return lead, watcher
}

func TestLeader(t *testing.T) {
	cli := makeRedis(t)

	cli1 := NewFaultyScripter(false, cli)
	leader1, watch1 := makeWatcher(Opts{
		Redis:    cli1,
		TTL:      1 * time.Second,
		Wait:     2 * time.Second,
		JitterMS: 10,
		Key:      "test1",
	})
	leader1.Start()

	// Wait a sec until l1 is done
	time.Sleep(100 * time.Millisecond)

	cli2 := NewFaultyScripter(false, cli)
	leader2, watch2 := makeWatcher(Opts{
		Redis:    cli2,
		TTL:      1 * time.Second,
		Wait:     2 * time.Second,
		JitterMS: 10,
		Key:      "test1",
	})
	leader2.Start()
	time.Sleep(100 * time.Millisecond)

	require.True(t, watch1.promoted.Load())
	require.False(t, watch2.promoted.Load())

	t.Run("leader 1 retains its lease", func(t *testing.T) {
		// Here we will wait three seconds and see whether 1 is still the leader.
		// In that case, no watcher should have changed.
		watch1.reset()
		watch2.reset()
		time.Sleep(3 * time.Second)

		assert.False(t, watch1.errored.Load(), "leader1 should not have errored")
		assert.Nil(t, watch1.lastError)
		assert.False(t, watch1.demoted.Load(), "leader1 should not be demoted")
		assert.False(t, watch1.promoted.Load(), "leader1 should not be promoted")

		assert.False(t, watch2.errored.Load(), "leader2 should not have errored")
		assert.Nil(t, watch2.lastError)
		assert.False(t, watch2.demoted.Load(), "leader2 should not be demoted")
		assert.False(t, watch2.promoted.Load(), "leader2 should not be promoted")
	})

	t.Run("it automatically takes the place of another leader", func(t *testing.T) {
		// Break leader1, wait leader2 take
		watch1.reset()
		watch2.reset()
		cli1.breakFlag.Store(true)

		time.Sleep(5 * time.Second)

		assert.True(t, watch1.demoted.Load(), "leader1 should be demoted")
		assert.False(t, watch1.promoted.Load(), "leader1 should not be promoted")

		assert.False(t, watch2.errored.Load(), "leader2 should not have errored")
		assert.Nil(t, watch2.lastError)
		assert.False(t, watch2.demoted.Load(), "leader2 should not be demoted")
		assert.True(t, watch2.promoted.Load(), "leader2 should be promoted")
	})

	t.Run("leader 2 retains its lease", func(t *testing.T) {
		// Here we will wait three seconds and see whether 2 is still the leader.
		// In that case, no watcher should have changed.
		watch1.reset()
		watch2.reset()
		time.Sleep(5 * time.Second)

		assert.False(t, watch1.demoted.Load(), "leader1 should not be demoted")
		assert.False(t, watch1.promoted.Load(), "leader1 should not be promoted")

		assert.False(t, watch2.errored.Load(), "leader2 should not have errored")
		assert.Nil(t, watch2.lastError)
		assert.False(t, watch2.demoted.Load(), "leader2 should not be demoted")
		assert.False(t, watch2.promoted.Load(), "leader2 should not be promoted")
	})

	t.Run("leader 1 takes its state back once leader2 breaks", func(t *testing.T) {
		cli1.breakFlag.Store(false)
		cli2.breakFlag.Store(true)

		watch1.reset()
		watch2.reset()
		time.Sleep(5 * time.Second)

		assert.False(t, watch1.errored.Load(), "leader1 should not have errored")
		assert.Nil(t, watch1.lastError)
		assert.False(t, watch1.demoted.Load(), "leader1 should not be demoted")
		assert.True(t, watch1.promoted.Load(), "leader1 be promoted")

		assert.True(t, watch2.demoted.Load(), "leader2 should be demoted")
		assert.False(t, watch2.promoted.Load(), "leader2 should not be promoted")
	})

	t.Run("Stop", func(t *testing.T) {
		cli2.breakFlag.Store(false)
		cli1.breakFlag.Store(false)

		err := leader1.Stop()
		assert.NoError(t, err)
		err = leader2.Stop()
		assert.NoError(t, err)
	})
}
