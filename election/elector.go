package election

import (
	"context"
	"crypto/sha1"
	"encoding/base32"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Elector struct {
	host      string
	redis     redis.Scripter
	key       string
	id        string
	promoteCh chan time.Time
	demoteCh  chan time.Time
	errorCh   chan error
	ttl       time.Duration
	wait      time.Duration
	jitter    time.Duration

	running           bool
	leading           bool
	cancelRenewRun    func()
	cancelElectionRun func()
	startStopLock     *sync.Mutex
	electionLock      *sync.Mutex
}

type Opts struct {
	Redis    redis.Scripter
	TTL      time.Duration
	Wait     time.Duration
	JitterMS int
	Key      string
}

func makeKey(input string) string {
	sha := sha1.New()
	sha.Write([]byte(input))
	return "taskrunner:leader:" + base32.StdEncoding.EncodeToString(sha.Sum(nil))
}

func clientID() string {
	src := rand.New(rand.NewSource(time.Now().UnixMicro()))
	id := make([]byte, 16)
	src.Read(id)

	return base32.StdEncoding.EncodeToString(id)
}

func NewElector(host string, opts Opts) (leader *Elector, onPromote <-chan time.Time, onDemote <-chan time.Time, onError <-chan error) {
	if opts.TTL == 0 {
		panic("NewLeader received a zero TTL")
	}

	if opts.Wait == 0 {
		panic("NewLeader received a zero Wait value")
	}

	prom := make(chan time.Time, 10)
	demo := make(chan time.Time, 10)
	err := make(chan error, 10)

	return &Elector{
		host:    host,
		redis:   opts.Redis,
		leading: false,
		key:     makeKey(opts.Key),
		ttl:     opts.TTL,
		wait:    opts.Wait,
		jitter:  time.Duration(opts.JitterMS) * time.Millisecond,
		id:      clientID(),

		startStopLock: &sync.Mutex{},
		electionLock:  &sync.Mutex{},

		promoteCh: prom,
		demoteCh:  demo,
		errorCh:   err,
	}, prom, demo, err
}

func randomJitter(val time.Duration) time.Duration {
	if val == 0 {
		return 0
	}
	return time.Duration(rand.Intn(int(val.Milliseconds()))) * time.Millisecond
}

func (i *Elector) renew() {
	i.electionLock.Lock()
	defer i.electionLock.Unlock()

	i.cancelRenewRun = nil

	leading, err := doAtomicRenew(i.redis, i.key, i.id, i.ttl)
	if err != nil {
		// This is bad, as we failed to renew our lease. Pretend we just lost
		// the lease, and we will retry to become leader in case the error
		// goes away on the next election.
		i.errorCh <- fmt.Errorf("trying to renew lease: %w", err)

		if i.leading {
			i.leading = false
			i.demoteCh <- time.Now()
		}

		i.cancelElectionRun = runAfter(i.wait, i.runElection)
		return
	}

	if leading {
		// We are still leading. Just prepare to the next renew.
		i.cancelRenewRun = runAfter((i.ttl/2)+randomJitter(i.jitter), i.renew)
		return
	}

	// We are no longer leading. Announce the demotion, and look for the
	// next election.
	if i.leading {
		i.leading = false
		i.demoteCh <- time.Now()
	}
	i.cancelElectionRun = runAfter(i.wait, i.runElection)
}

func (i *Elector) runElection() {
	i.electionLock.Lock()
	defer i.electionLock.Unlock()
	i.cancelElectionRun = nil

	set, err := doAtomicSet(i.redis, i.key, i.id, i.ttl)
	if err != nil {
		fmt.Println(i.host, " I'm broken")
		i.errorCh <- fmt.Errorf("trying to run election: %w", err)
		i.cancelElectionRun = runAfter(i.wait, i.runElection)
		return
	}

	if set {
		i.leading = true
		fmt.Println(i.host, " I'm the leader now")
		i.promoteCh <- time.Now()

		i.cancelRenewRun = runAfter((i.ttl/2)+randomJitter(i.jitter), i.renew)
	} else {
		if i.leading {
			fmt.Println(i.host, " I'm not leader anymore")
			i.demoteCh <- time.Now()
		}
		i.leading = false
		if i.cancelRenewRun != nil {
			i.cancelRenewRun()
			i.cancelRenewRun = nil
		}
		i.cancelElectionRun = runAfter(i.wait, i.runElection)
	}

}

func (i *Elector) voidTimers() {
	if i.cancelElectionRun != nil {
		i.cancelElectionRun()
		i.cancelRenewRun = nil
	}
	if i.cancelRenewRun != nil {
		i.cancelRenewRun()
		i.cancelRenewRun = nil
	}
}

func (i *Elector) resign() error {
	i.electionLock.Lock()
	defer i.electionLock.Unlock()
	i.voidTimers()

	leading, err := doAtomicDelete(i.redis, i.key, i.id)
	if err != nil {
		return err
	}

	i.running = false
	if leading {
		i.demoteCh <- time.Now()
	}

	i.leading = false

	return nil
}

func (i *Elector) Start() {
	i.startStopLock.Lock()
	defer i.startStopLock.Unlock()
	if i.running {
		return
	}
	i.running = true
	registerScripts(i.redis)
	go i.runElection()
}

func (i *Elector) Stop() error {
	i.startStopLock.Lock()
	defer i.startStopLock.Unlock()
	if !i.running {
		return nil
	}
	return i.resign()
}

func (i *Elector) IsLeader(context.Context) error {
	if i.leading {
		return nil
	}

	return errors.New("it's not leader")
}
