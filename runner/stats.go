package runner

import "time"

type Stats struct {
	PerTaskTiming     map[string]int64
	PredictedWaitTime float64
	AvgTiming         time.Duration
}
