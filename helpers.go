package par

import (
	"runtime"
)

// partsCount returns the number of partitions to use optimised for
// the available CPUs and the given values.
func partsCount[In any](values []In) int {
	if p := runtime.GOMAXPROCS(0); p <= len(values) {
		return p
	}

	return len(values)
}

// parts returns the number of partitions and the size optimised for
// the available CPUs and given values.
func parts[In any](values []In) (count, size int) {
	count = partsCount(values)

	return count, len(values) / count
}
