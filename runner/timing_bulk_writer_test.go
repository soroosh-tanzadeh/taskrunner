package runner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFlush(t *testing.T) {
	c := make(chan bool)
	timing := timingDto{
		timing:   time.Millisecond * 100,
		taskName: "dummy-task",
	}

	w := NewBulkWriter(time.Millisecond, func(data []timingDto) error {
		assert.Equal(t, data[0], timing)
		c <- true
		return nil
	})

	_ = w.write(timing)

	<-c
}

func TestClose(t *testing.T) {
	w := NewBulkWriter(time.Millisecond, func(data []timingDto) error {
		return nil
	})

	w.close()

	assert.True(t, w.closed)
	assert.Len(t, w.buf, 0)
	assert.Len(t, w.data, 0)
	assert.Len(t, w.tickerCh, 0)
}
