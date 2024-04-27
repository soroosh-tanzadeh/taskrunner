package runner

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
)

type TaskAction func(ctx context.Context, payload any) error
type UniqueKeyFunc func(payload any) string

type Task struct {
	Name     string
	MaxRetry int

	// When a task takes more than reservation time, woker will send a HeartBeat to queue, to prevent reClaim by
	ReservationTimeout time.Duration

	Action TaskAction `json:"-"`

	// If 'unique' is set to true, the task will not be dispatched if another instance of same task is already in the queue and has not finished processing.
	Unique bool

	// If 'unique' is set to true and a 'UniqueKey' is specified, the task with the same 'UniqueKey' will not be dispatched if another instance of it with the same 'UniqueKey' is already in the queue and has not finished processing.
	UniqueKey UniqueKeyFunc

	// If `UniqueFor` seconds have passed since a task with the same name and UniqueKey was enqueued and it has not finished processing yet, the new task with the same name and UniqueKey can now be dispatched.
	// this option is required when Unique is true
	UniqueFor int64

	lockValue string
}

func (t *Task) CreateMessage(paylaod any) TaskMessage {
	msg := TaskMessage{
		TaskName:           t.Name,
		Unique:             t.Unique,
		UniqueFor:          t.UniqueFor,
		ReservationTimeout: int64(t.ReservationTimeout),
		Payload:            paylaod,
	}

	if t.UniqueKey != nil {
		msg.UniqueKey = t.lockKey(paylaod)
	} else {
		msg.UniqueKey = ""
	}

	return msg
}

func (t *Task) lockKey(payload any) string {
	var lockKey string
	if t.UniqueKey != nil {
		if lockKey = t.UniqueKey(payload); len(lockKey) == 0 {
			lockKey = t.Name
		}
	}

	if len(lockKey) != 0 {
		lockKey = t.Name + ":" + lockKey
	} else {
		lockKey = t.Name
		log.Warnf("The unique key for Task %s is `nil`. Another task with the same name cannot be dispatched.", t.Name)
	}

	return lockKey
}
