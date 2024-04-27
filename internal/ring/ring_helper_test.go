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
