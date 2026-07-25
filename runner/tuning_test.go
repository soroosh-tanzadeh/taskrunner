package runner

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/soroosh-tanzadeh/taskrunner/contracts"
)

func TestComputeTunedWorkers_InBand_ReturnsCurrent(t *testing.T) {
	current := 10
	next, reason := computeTunedWorkers(current, 3, 1, 100, 95, 100, 10) // 95 within [90,110]
	if next != current {
		t.Fatalf("expected %d, got %d", current, next)
	}
	if reason != "in_tolerance_band" {
		t.Fatalf("expected reason in_tolerance_band, got %s", reason)
	}
}

func TestComputeTunedWorkers_AboveBand_BoundaryTuning_SymmetricRounding(t *testing.T) {
	current := 10
	// predicted=120ms desired=100ms tol=10ms -> above upper (110)
	next, reason := computeTunedWorkers(current, 1, 1, 100, 120, 100, 10)
	// targetWait=110 -> desiredTotalWorkers=10*(120/110)=10.909... -> round=11
	if next != 11 {
		t.Fatalf("expected 11, got %d", next)
	}
	if reason != "above_upper_boundary" {
		t.Fatalf("expected reason above_upper_boundary, got %s", reason)
	}
}

func TestComputeTunedWorkers_BelowBand_BoundaryTuning_SymmetricRounding(t *testing.T) {
	current := 10
	// predicted=80ms desired=100ms tol=10ms -> below lower (90)
	next, reason := computeTunedWorkers(current, 1, 1, 100, 80, 100, 10)
	// targetWait=90 -> desiredTotalWorkers=10*(80/90)=8.888... -> round=9
	if next != 9 {
		t.Fatalf("expected 9, got %d", next)
	}
	if reason != "below_lower_boundary" {
		t.Fatalf("expected reason below_lower_boundary, got %s", reason)
	}
}

func TestComputeTunedWorkers_ReplicationFactorGreaterThanOne_NoOscillationLikeFlip(t *testing.T) {
	// With rep=2, an asymmetric ceil/floor could cause oscillation between adjacent capacities.
	// Symmetric rounding should keep the worker count stable near the boundaries.
	minW, maxW := 1, 100
	rep := 2
	current := 5
	desired := 100.0
	tol := 10.0
	lower := desired - tol // 90
	upper := desired + tol // 110

	// Slightly above upper: should stay at 5.
	next, _ := computeTunedWorkers(current, rep, minW, maxW, upper+0.1, desired, tol)
	if next != current {
		t.Fatalf("expected no change (replication fix), got %d", next)
	}

	// Slightly below lower: should also stay at 5.
	next, _ = computeTunedWorkers(current, rep, minW, maxW, lower-0.1, desired, tol)
	if next != current {
		t.Fatalf("expected no change (replication fix), got %d", next)
	}
}

func TestComputeTunedWorkers_ZeroTolerance_SinglePointBand(t *testing.T) {
	current := 7
	desired := 100.0

	// predicted exactly at desired -> in band
	next, reason := computeTunedWorkers(current, 1, 1, 100, desired, desired, 0)
	if next != current || reason != "in_tolerance_band" {
		t.Fatalf("expected in-band (current=%d), got next=%d reason=%s", current, next, reason)
	}

	// predicted well above desired -> tune using upper/boundary (which equals desired).
	// Use enough delta so that symmetric rounding actually increments the result.
	next, _ = computeTunedWorkers(current, 1, 1, 100, desired+10, desired, 0)
	if next <= current {
		t.Fatalf("expected scale up, got %d", next)
	}
}

func TestComputeTunedWorkers_PredictedWaitZero_ReturnsToMin(t *testing.T) {
	current := 15
	next, reason := computeTunedWorkers(current, 2, 10, 20, 0, 100, 10)
	if next != 10 {
		t.Fatalf("expected %d, got %d", 10, next)
	}
	if reason != "predicted_wait_zero" {
		t.Fatalf("expected reason predicted_wait_zero, got %s", reason)
	}
}

func TestComputeTunedWorkers_BoundaryEquality_InBand(t *testing.T) {
	current := 8
	desired := 100.0
	tol := 10.0
	lower := desired - tol
	upper := desired + tol

	next, _ := computeTunedWorkers(current, 1, 1, 100, lower, desired, tol)
	if next != current {
		t.Fatalf("expected current at lower bound, got %d", next)
	}

	next, _ = computeTunedWorkers(current, 1, 1, 100, upper, desired, tol)
	if next != current {
		t.Fatalf("expected current at upper bound, got %d", next)
	}
}

func TestComputeTunedWorkers_DeviationLarge_TunesToCenter(t *testing.T) {
	// Above extended upper => targetWait=desired.
	next, reason := computeTunedWorkers(10, 1, 1, 100, 151, 100, 10)
	// desiredTotalWorkers = 10*(151/100)=15.1 -> round=15.
	if next != 15 {
		t.Fatalf("expected 15, got %d", next)
	}
	if reason != "deviation_large_above_center" {
		t.Fatalf("expected reason deviation_large_above_center, got %s", reason)
	}

	// Below extended lower => targetWait=desired.
	next, reason = computeTunedWorkers(10, 1, 1, 100, 49, 100, 10)
	// desiredTotalWorkers=10*(49/100)=4.9 -> round=5
	if next != 5 {
		t.Fatalf("expected 5, got %d", next)
	}
	if reason != "deviation_large_below_center" {
		t.Fatalf("expected reason deviation_large_below_center, got %s", reason)
	}
}

func TestComputeTunedWorkers_ClampsToMaxAndMin_ExtremeValues(t *testing.T) {
	// Clamp to max.
	next, _ := computeTunedWorkers(50, 2, 10, 60, 1_000_000, 100, 0)
	if next != 60 {
		t.Fatalf("expected 60, got %d", next)
	}

	// Clamp to min. Use rep factor big enough to hit the next<=0 branch.
	// predicted is tiny but >0 so we go through tuning (not early return).
	next, _ = computeTunedWorkers(1, 1000, 10, 60, 0.001, 100, 0)
	if next != 10 {
		t.Fatalf("expected min clamp (10), got %d", next)
	}
}

func TestComputeTunedWorkers_InvalidInputs_MinEqualsMax(t *testing.T) {
	// minWorkers == maxWorkers should clamp deterministically.
	next, _ := computeTunedWorkers(10, 1, 5, 5, 200, 100, 10)
	if next != 5 {
		t.Fatalf("expected 5, got %d", next)
	}
}

func TestComputeTunedWorkers_InvalidInputs_GracefulReturn(t *testing.T) {
	next, _ := computeTunedWorkers(-1, 1, 1, 10, 100, 100, 10)
	if next != -1 {
		t.Fatalf("expected passthrough current, got %d", next)
	}

	next, reason := computeTunedWorkers(5, 1, 1, 10, 100, 0, 10)
	if reason != "invalid_wait_params" {
		t.Fatalf("expected invalid_wait_params, got %s", reason)
	}

	next, _ = computeTunedWorkers(5, 0, 1, 10, 100, 100, 10) // rep<=0 -> rep=1
	if next <= 0 {
		t.Fatalf("expected positive next, got %d", next)
	}
}

func TestComputeTunedWorkers_LowerClampedToZero(t *testing.T) {
	// lower = desired - tolerance => negative, then clamped to 0.
	current := 9
	next, reason := computeTunedWorkers(current, 1, 1, 100, 2, 5, 10)
	if next != current {
		t.Fatalf("expected %d, got %d", current, next)
	}
	if reason != "in_tolerance_band" {
		t.Fatalf("expected in_tolerance_band, got %s", reason)
	}
}

func TestComputeTunedWorkers_InvalidInputs_NegativeTolerance(t *testing.T) {
	current := 4
	next, reason := computeTunedWorkers(current, 1, 1, 100, 200, 100, -1)
	if next != current {
		t.Fatalf("expected passthrough current, got %d", next)
	}
	if reason != "invalid_wait_params" {
		t.Fatalf("expected invalid_wait_params, got %s", reason)
	}
}

func BenchmarkComputeTunedWorkers(b *testing.B) {
	for b.Loop() {
		_, _ = computeTunedWorkers(20, 3, 1, 100, 250, 100, 10)
	}
}

func TestTimingAggregator_FollowerResetsLocalTime(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})

	cfg := TaskRunnerConfig{
		ConsumerGroup:        "test-group",
		MetricsResetInterval: 10 * time.Millisecond,
	}
	tr := NewTaskRunner(cfg, rdb, nil)
	tr.isLeader.Store(false)
	initialResetTime := tr.lastMetricsResetTime
	time.Sleep(15 * time.Millisecond)
	tr.timingAggregator()

	if !tr.lastMetricsResetTime.After(initialResetTime) {
		t.Errorf("expected lastMetricsResetTime to be updated for followers, but it wasn't")
	}
}

type fallbackMockQueue struct {
	contracts.MessageQueue
}

func (m *fallbackMockQueue) Len() (int64, error) {
	return 100, nil
}
func TestGetTimingStatistics_FallbackWhenAvgTimingIsZero(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	mockQueue := &fallbackMockQueue{}
	cfg := TaskRunnerConfig{
		ConsumerGroup: "test-group",
		NumWorkers:    10,
	}
	tr := NewTaskRunner(cfg, rdb, mockQueue)

	tr.RegisterTask(&Task{Name: "test_task"})
	stats, err := tr.GetTimingStatistics()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.PredictedWaitTime <= 0 {
		t.Errorf("expected PredictedWaitTime to be greater than 0 when queue has tasks, got %f", stats.PredictedWaitTime)
	}
}
