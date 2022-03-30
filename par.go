// Package par provides utilities for parallelizing computations.
//
// Most implementations are built on parallelization via partitioning, i.e.
// data is divided into partitions, the partitions are mapped to intermediate
// representations in parallel, then the intermediate representations are
// combined (reduced) in parallel (where possible) to produce the desired
// result. This approach to parallelization provides a few key benefits:
// - No synchronization required between threads during execution
//   (coordination happens in the main thread).
// - Number of allocations can be minimized as sizes become known before use.
// - Sacrificing determinism is usually not necessary to get the maximum
//   performance. In fact, in most cases deterministic implementations are the
//   fastest option as they access memory in a linear fashion.
//
// As with every performance-oriented tool, measure before applying. Most of the provided functionality is only beneficial if the datasets are large enough or the computations are expensive.
package par

import (
	"runtime"
	"sync"
)

// Map returns a slice of type Out by applying the transform function on every
// item in values.
//
// The implementation is deterministic, and the returned slice maintains the
// order of the original values.
func Map[In, Out any](values []In, transform func(In) Out) []Out {
	if len(values) == 0 {
		return []Out(nil)
	}

	partitions, partitionSize := parts(values)
	result := make([]Out, len(values))
	var wg sync.WaitGroup
	wg.Add(partitions)
	for p := 0; p < partitions; p++ {
		start := partitionSize * p
		end := start + partitionSize
		if p == partitions-1 {
			end = len(values)
		}
		go func(start, end int) {
			defer wg.Done()
			for i := start; i < end; i++ {
				result[i] = transform(values[i])
			}
		}(start, end)
	}
	wg.Wait()

	return result
}

// Filter returns a copy of the values slice without the values for which the
// predicate returns false.
//
// The implementation is deterministic, and the returned slice maintains the
// order of the original values.
//
// Internally, the implementation maps the values into per-partition bitmaps
// in parallel using the predicate, then creates a slice to store the results,
// then the bitmaps are used to map the values into the results slice in
// parallel.
func Filter[T any](values []T, predicate func(T) bool) []T {
	if len(values) == 0 {
		return []T(nil)
	}

	partitions, partitionSize := parts(values)
	bitmapSize := partitionSize/64 + 1
	lastBitmapSize := (len(values)-(partitions-1)*partitionSize)/64 + 1
	fullBitmap := make([]uint64, bitmapSize*(partitions-1)+lastBitmapSize)
	jobs := make([]struct {
		bitmap []uint64
		start  int
		end    int
		offset int
		count  int
	}, partitions)

	var wg sync.WaitGroup
	wg.Add(partitions)
	for p := range jobs {
		jobs[p].bitmap = fullBitmap[bitmapSize*p:]
		jobs[p].start = p * partitionSize
		jobs[p].end = jobs[p].start + partitionSize
		if p == partitions-1 {
			jobs[p].end = len(values)
		}
		go func(p int) {
			defer wg.Done()
			j := jobs[p]
			for i := j.start; i < j.end; i++ {
				if predicate(values[i]) {
					pos := i - j.start
					j.bitmap[pos/64] |= 1 << (pos % 64)
					j.count++
				}
			}
			jobs[p].count = j.count
		}(p)
	}
	wg.Wait()

	var totalCount int
	for p := range jobs {
		jobs[p].offset = totalCount
		totalCount += jobs[p].count
	}

	result := make([]T, totalCount)
	wg.Add(partitions)
	for p := range jobs {
		go func(p int) {
			defer wg.Done()
			j := jobs[p]
			for i := j.start; i < j.end; i++ {
				pos := i - j.start
				if (j.bitmap[pos/64] & (1 << (pos % 64))) > 0 {
					result[j.offset] = values[i]
					j.offset++
				}
			}
		}(p)
	}
	wg.Wait()

	return result
}

// Reduce reduces the values to a single one, by repeatedly applying an
// accumulator.
//
// The accumulator is provided with two arguments:
// - The result of the previous call to accumulator, OR, for the first call,
//   the first element.
// - The current element being processed.
// The accumulator then returns the result of combining these two values.
//
// The ordering of the accumulations is deterministic and linear only within a
// partition. The implementation works by reducing each partition into a single
// value and then reducing the values from each partition as they become ready.
//
// Panics if values is an empty slice.
func Reduce[T any](values []T, accumulator func(T, T) T) T {
	if len(values) < 1 {
		panic("cannot reduce an empty slice")
	}

	partitions, partitionSize := parts(values)
	results := make(chan T)
	for p := 0; p < partitions; p++ {
		start := partitionSize * p
		end := start + partitionSize
		if p == partitions-1 {
			end = len(values)
		}
		go func(start, end int) {
			v := values[start]
			for i := start + 1; i < end; i++ {
				v = accumulator(v, values[i])
			}
			results <- v
		}(start, end)
	}

	v := <-results
	for p := 1; p < partitions; p++ {
		v = accumulator(v, <-results)
	}
	return v
}

// Any returns a boolean indicating if predicate returns true for any of the
// values.
//
// A partition will terminate upon the first encountered value for which the
// predicate returns true, and as such, the predicate may not be called for
// every value.
func Any[T any](values []T, predicate func(T) bool) bool {
	if len(values) == 0 {
		return false
	}

	partitions, partitionSize := parts(values)

	results := make(chan bool, partitions) // buffer to prevent processors from blocking.
	done := make(chan struct{})
	for p := 0; p < partitions; p++ {
		start := partitionSize * p
		end := start + partitionSize
		if p == partitions-1 {
			end = len(values)
		}
		go func() {
			for i := start; i < end; i++ {
				select {
				case <-done:
					results <- false
					return
				default:
					if predicate(values[i]) {
						results <- true
						return
					}
				}
			}
			results <- false
		}()
	}

	// Ensure that all processing goroutines have exited otherwise we could trigger
	// a data race in the caller due use of predicate or values after we return.
	var result bool
	for p := 0; p < partitions; p++ {
		if <-results && !result {
			close(done) // trigger early return of remaining processors.
			result = true
		}
	}
	return result
}

// All returns a boolean indicating if predicate returns true for all of the
// values.
//
// A partition will terminate upon the first encountered value for which the
// predicate returns false, and as such, the predicate may not be called for
// every value.
func All[T any](values []T, predicate func(T) bool) bool {
	return None(values, func(v T) bool { return !predicate(v) })
}

// None returns a boolean indicating if predicate returns true for none of the
// values.
//
// A partition will terminate upon the first encountered value for which the
// predicate returns true, and as such, the predicate may not be called for
// every value.
func None[T any](values []T, predicate func(T) bool) bool {
	return !Any(values, predicate)
}

// parts returns the number of partitions and the size optimised for
// the available CPUs and given values.
func parts[In any](values []In) (count, size int) {
	if p := runtime.GOMAXPROCS(0); p <= len(values) {
		return p, len(values) / p
	}
	return len(values), 1
}
