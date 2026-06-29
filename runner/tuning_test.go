package runner

import "testing"

func TestComputeTunedWorkers_InBand_ReturnsCurrent(t *testing.T) {
	current := 10
	next := computeTunedWorkers(current, 3, 1, 100, 95, 100, 10) // 95 within [90,110]
	if next != current {
		t.Fatalf("expected %d, got %d", current, next)
	}
}

func TestComputeTunedWorkers_AboveBand_IncreasesWorkers(t *testing.T) {
	current := 10
	// predicted=120ms desired=100ms tol=10ms -> above upper (110)
	next := computeTunedWorkers(current, 1, 1, 100, 120, 100, 10)
	// We target the upper boundary (110ms):
	// desiredTotalWorkers = 10 * (120/110) = 10.909... -> ceil=11
	if next != 11 {
		t.Fatalf("expected 11, got %d", next)
	}
}

func TestComputeTunedWorkers_BelowBand_DecreasesWorkers(t *testing.T) {
	current := 10
	// predicted=80ms desired=100ms tol=10ms -> below lower (90)
	next := computeTunedWorkers(current, 1, 1, 100, 80, 100, 10)
	// ratio=0.8 -> floor(8)=8
	if next != 8 {
		t.Fatalf("expected 8, got %d", next)
	}
}

func TestComputeTunedWorkers_ClampsToMaxAndMin(t *testing.T) {
	// Clamp to max
	next := computeTunedWorkers(50, 2, 10, 60, 1000, 100, 0) // huge ratio -> would exceed max
	if next != 60 {
		t.Fatalf("expected 60, got %d", next)
	}

	// Clamp to min
	next = computeTunedWorkers(50, 2, 10, 60, 1, 100, 0) // tiny ratio -> would go below min
	if next != 10 {
		t.Fatalf("expected 10, got %d", next)
	}
}

