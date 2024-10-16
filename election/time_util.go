package election

import (
	"sync/atomic"
	"time"
)

type cancelFn func()

func runAfter(timeout time.Duration, fn func()) cancelFn {
	cancelCh := make(chan bool)
	canceled := atomic.Bool{}

	go func() {
		select {
		case <-cancelCh:
			return
		case <-time.After(timeout):
			if canceled.Swap(true) {
				return
			}
			close(cancelCh)
			fn()
			return
		}
	}()

	return func() {
		if !canceled.Swap(true) {
			close(cancelCh)
		}
	}
}
