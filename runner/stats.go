package runner

import "time"

type Stats struct {
	PerTaskTiming     map[string]int64
	PredictedWaitTime float64
	AvgTiming         time.Duration
	// Estimated TPS, it's not actual tps
	TPS float64
}
