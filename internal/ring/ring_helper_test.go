package ring

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAverageFloat64(t *testing.T) {
	values := []float64{5.0, 7.2, 3.8, 9.5, 6.1}

	avg := AverageFloat64(values)

	assert.Equal(t, 6.32, avg)
}

func TestMaxFloat64(t *testing.T) {
	values := []float64{5.0, 7.2, 3.8, 9.5, 6.1}

	max := MaxFloat64(values)

	assert.Equal(t, 9.5, max)
}

func TestMinFloat64(t *testing.T) {
	values := []float64{5.0, 7.2, 3.8, 9.5, 6.1}

	min := MinFloat64(values)

	assert.Equal(t, 3.8, min)
}

func TestStandardDeviationFloat64(t *testing.T) {
	values := []float64{5.0, 7.2, 3.8, 9.5, 6.1}

	sd := StandardDeviationFloat64(values)

	assert.Equal(t, 1.9507947098554477, sd)
}

// ---- Benchmarks ----

// BenchmarkAverageFloat64_Small covers the average over a small slice, the size
// most commonly seen in queue metric windows.
func BenchmarkAverageFloat64_Small(b *testing.B) {
	values := []float64{5.0, 7.2, 3.8, 9.5, 6.1}

	for b.Loop() {
		_ = AverageFloat64(values)
	}
}

// BenchmarkAverageFloat64_Large covers the average over a larger window.
func BenchmarkAverageFloat64_Large(b *testing.B) {
	values := make([]float64, 1024)
	for i := range values {
		values[i] = float64(i)
	}

	for b.Loop() {
		_ = AverageFloat64(values)
	}
}

// BenchmarkStandardDeviationFloat64 is the more expensive ring helper (it
// calls math.Pow/Sqrt per element), used when computing queue metric spread.
func BenchmarkStandardDeviationFloat64(b *testing.B) {
	values := make([]float64, 256)
	for i := range values {
		values[i] = float64(i)
	}

	for b.Loop() {
		_ = StandardDeviationFloat64(values)
	}
}
