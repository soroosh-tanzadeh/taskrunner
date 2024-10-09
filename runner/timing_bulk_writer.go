package runner

import (
	"errors"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type FlushFunc func(data []timingDto) error

type TimingBulkWriter struct {
	ticker    *time.Ticker
	tickerCh  <-chan time.Time
	buf       []timingDto
	data      chan timingDto
	closeLock *sync.Mutex
	closed    bool
	quit      chan bool
	flushFunc FlushFunc
}

func NewBulkWriter(flushInterval time.Duration, flushFunc FlushFunc) *TimingBulkWriter {
	bw := &TimingBulkWriter{
		buf:       make([]timingDto, 0),
		data:      make(chan timingDto),
		quit:      make(chan bool),
		flushFunc: flushFunc,
		tickerCh:  make(chan time.Time),
		closeLock: &sync.Mutex{},
	}

	if flushInterval > 0 {
		bw.ticker = time.NewTicker(flushInterval)
		bw.tickerCh = bw.ticker.C
	}

	go bw.processor()

	return bw
}

func (b *TimingBulkWriter) processor() {
	for {
		select {
		case d := <-b.data:
			b.buf = append(b.buf, d)
		case <-b.tickerCh:
			b.flush()
		case <-b.quit:
			b.flush()
			return
		}
	}
}

func (b *TimingBulkWriter) write(data timingDto) error {
	if b.closed {
		return errors.New("writing on a closed bulk.BulkWriter")
	}

	b.data <- data

	return nil
}

func (b *TimingBulkWriter) flush() {
	if len(b.buf) == 0 {
		return
	}

	if err := b.flushFunc(b.buf); err != nil {
		log.WithError(err).Error("error while flushing")
	}

	b.buf = []timingDto{}
}

func (b *TimingBulkWriter) close() {
	b.closeLock.Lock()
	defer b.closeLock.Unlock()

	if b.closed {
		log.Error("closing a closed bulk.BulkWriter")
		return
	}

	b.closed = true

	close(b.quit)

	if b.ticker != nil {
		b.ticker.Stop()
	}
}
