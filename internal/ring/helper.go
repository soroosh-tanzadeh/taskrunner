package ring

import (
	"math"
)

func AverageFloat64(items []float64) float64 {
	count := len(items)
	sum := 0.0

	for _, v := range items {
		sum = sum + v
	}
	return sum / float64(count)
}

func MaxFloat64(items []float64) float64 {
	max := items[0]
	for _, v := range items {
		if v > max {
			max = v
		}
	}
	return max
}

func MinFloat64(items []float64) float64 {
	min := items[0]
	for _, v := range items {
		if v < min {
			min = v
		}
	}
	return min
}

// StandardDeviationFloat64
func StandardDeviationFloat64(items []float64, calculatedAvg ...float64) float64 {
	n := len(items)
	var average float64
	if len(calculatedAvg) > 0 {
		average = calculatedAvg[0]
	} else {
		average = AverageFloat64(items)
	}
	vn := 0.0

	for _, v := range items {
		vn += math.Pow(v-average, 2)
	}

	return math.Sqrt(vn / float64(n))
}
