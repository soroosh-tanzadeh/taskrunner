package runner

import (
	"context"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/soroosh-tanzadeh/taskrunner/contracts"
)

type fetchedMessage struct {
	message  contracts.Message
	consumer string
}

func (t *TaskRunner) addFetcher(ctx context.Context, fetcherID int) {
	defer t.wg.Done()
	defer func() {
		r := recover()
		if r != nil {
			err, ok := r.(error)
			logEntry := log.WithField("fetcher_id", fetcherID).WithField("cause", r)
			if ok {
				logEntry = logEntry.WithError(err)
			}
			logEntry.Error("Fetcher Panic")

			if ctx.Err() != nil {
				return
			}

			// Add new fetcher to keep number of fetchers constant
			t.wg.Add(1)
			go func() {
				defer t.wg.Done()
				t.addFetcher(ctx, int(fetcherID))
			}()
		}
	}()

	t.fetchMessage(ctx, fetcherID)
}

func (t *TaskRunner) fetchMessage(ctx context.Context, fetcherID int) {
	batchSize := t.cfg.BatchSize
	consumerName := t.consumerName() + "_" + strconv.Itoa(fetcherID)

	for {
		if ctx.Err() != nil {
			return
		}

		actionContext := context.Background()
		messages, err := t.queue.Receive(actionContext, time.Second*5, batchSize, t.ConsumerGroup(), consumerName)
		if err != nil {
			t.captureError(err)
			continue
		}
		for _, message := range messages {
			err := t.workerPool.Invoke(fetchedMessage{
				message:  message,
				consumer: consumerName,
			})
			if err != nil {
				t.captureError(err)
				continue
			}
		}
	}

}
