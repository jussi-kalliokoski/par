package par_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/jussi-kalliokoski/par"
)

func TestMap(t *testing.T) {
	values := make([]int, 10000)
	for i := range values {
		values[i] = i
	}
	expected := make([]int, len(values))
	for i := range expected {
		expected[i] = i * 2
	}

	t.Run("lengths", func(t *testing.T) {
		tests := []int(nil)
		for i := 0; i < 128; i++ {
			tests = append(tests, i)
		}
		for i := 128; i < 2048; i = i << 1 {
			tests = append(tests, i)
		}
		for _, l := range tests {
			t.Run(fmt.Sprintf("len %d", l), func(t *testing.T) {
				received := par.Map(values[:l], func(v int) int {
					return v * 2
				})
				assertSliceEquals(t, expected[:l], received)
			})
		}
	})
}

func TestFilter(t *testing.T) {
	values := make([]int, 10000)
	for i := range values {
		values[i] = i
	}

	t.Run("lengths", func(t *testing.T) {
		tests := []int(nil)
		for i := 0; i < 128; i++ {
			tests = append(tests, i)
		}
		for i := 128; i < 2048; i = i << 1 {
			tests = append(tests, i)
		}
		for _, l := range tests {
			t.Run(fmt.Sprintf("len %d", l), func(t *testing.T) {
				expected := []int(nil)
				for _, v := range values[:l] {
					if v%2 == 0 {
						expected = append(expected, v)
					}
				}

				received := par.Filter(values[:l], func(v int) bool {
					return v%2 == 0
				})

				assertSliceEquals(t, expected, received)
			})
		}
	})
}

func TestReduce(t *testing.T) {
	values := make([]int, 10000)
	for i := range values {
		values[i] = i
	}

	t.Run("lengths", func(t *testing.T) {
		t.Run("len 0", func(t *testing.T) {
			assertPanics(t, func() {
				par.Reduce([]int(nil), func(a, b int) int {
					return a + b
				})
			})
		})

		tests := []int(nil)
		for i := 1; i < 128; i++ {
			tests = append(tests, i)
		}
		for i := 128; i < 2048; i = i << 1 {
			tests = append(tests, i)
		}
		for _, l := range tests {
			t.Run(fmt.Sprintf("len %d", l), func(t *testing.T) {
				var expected int
				for _, v := range values[:l] {
					expected += v
				}

				received := par.Reduce(values[:l], func(a, b int) int {
					return a + b
				})

				assertEquals(t, expected, received)
			})
		}
	})
}

func TestAny(t *testing.T) {
	t.Run("lengths", func(t *testing.T) {
		t.Run("len 0", func(t *testing.T) {
			assertEquals(t, false, par.Any([]int(nil), func(int) bool {
				return true
			}))
		})

		tests := []int(nil)
		for i := 1; i < 128; i++ {
			tests = append(tests, i)
		}
		for i := 128; i < 2048; i = i << 1 {
			tests = append(tests, i)
		}
		for _, l := range tests {
			t.Run(fmt.Sprintf("len %d", l), func(t *testing.T) {
				t.Run("true", func(t *testing.T) {
					values := make([]int, l)
					for i := range values {
						values[i] = i
					}
					rand.Seed(int64(l))
					values[rand.Intn(l)] = l

					received := par.Any(values, func(v int) bool {
						return v == l
					})

					assertEquals(t, true, received)
				})

				t.Run("false", func(t *testing.T) {
					values := make([]int, l)
					for i := range values {
						values[i] = i
					}

					received := par.Any(values, func(v int) bool {
						return v == l
					})

					assertEquals(t, false, received)
				})
			})
		}
	})
}

func TestAll(t *testing.T) {
	t.Run("lengths", func(t *testing.T) {
		t.Run("len 0", func(t *testing.T) {
			assertEquals(t, true, par.All([]int(nil), func(int) bool {
				return false
			}))
		})

		tests := []int(nil)
		for i := 1; i < 128; i++ {
			tests = append(tests, i)
		}
		for i := 128; i < 2048; i = i << 1 {
			tests = append(tests, i)
		}
		for _, l := range tests {
			t.Run(fmt.Sprintf("len %d", l), func(t *testing.T) {
				t.Run("true", func(t *testing.T) {
					values := make([]int, l)
					for i := range values {
						values[i] = i
					}

					received := par.All(values, func(v int) bool {
						return v < l
					})

					assertEquals(t, true, received)
				})

				t.Run("false", func(t *testing.T) {
					values := make([]int, l)
					for i := range values {
						values[i] = i
					}
					rand.Seed(int64(l))
					values[rand.Intn(l)] = l

					received := par.All(values, func(v int) bool {
						return v < l
					})

					assertEquals(t, false, received)
				})
			})
		}
	})
}

func TestNone(t *testing.T) {
	t.Run("lengths", func(t *testing.T) {
		t.Run("len 0", func(t *testing.T) {
			assertEquals(t, true, par.None([]int(nil), func(int) bool {
				return true
			}))
		})

		tests := []int(nil)
		for i := 1; i < 128; i++ {
			tests = append(tests, i)
		}
		for i := 128; i < 2048; i = i << 1 {
			tests = append(tests, i)
		}
		for _, l := range tests {
			t.Run(fmt.Sprintf("len %d", l), func(t *testing.T) {
				t.Run("true", func(t *testing.T) {
					values := make([]int, l)
					for i := range values {
						values[i] = i
					}

					received := par.None(values, func(v int) bool {
						return v == l
					})

					assertEquals(t, true, received)
				})

				t.Run("false", func(t *testing.T) {
					values := make([]int, l)
					for i := range values {
						values[i] = i
					}
					rand.Seed(int64(l))
					values[rand.Intn(l)] = l

					received := par.None(values, func(v int) bool {
						return v == l
					})

					assertEquals(t, false, received)
				})
			})
		}
	})
}

// deadBool is used for global assignment to prevent benchmark rounds from getting optimized out
var deadBool bool

func BenchmarkMap(b *testing.B) {
	rand.Seed(1)
	collections := CreateCollections(10000)

	b.Run("serial", func(b *testing.B) {
		var r bool
		for n := 0; n < b.N; n++ {
			result := make([]int, len(collections))
			for i, c := range collections {
				result[i] = c.NumbersSum()
			}
			r = len(result) == 123
		}
		deadBool = r
	})
	b.Run("parallel", func(b *testing.B) {
		var r bool
		for n := 0; n < b.N; n++ {
			result := par.Map(collections, Collection.NumbersSum)
			r = len(result) == 123
		}
		deadBool = r
	})
}

func BenchmarkFilter(b *testing.B) {
	rand.Seed(1)
	collections := CreateCollections(10000)

	b.Run("serial", func(b *testing.B) {
		var r bool
		for n := 0; n < b.N; n++ {
			result := []Collection(nil)
			for _, c := range collections {
				if c.NumbersSumIsPositive() {
					result = append(result, c)
				}
			}
			r = len(result) == 123
		}
		deadBool = r
	})
	b.Run("parallel", func(b *testing.B) {
		var r bool
		for n := 0; n < b.N; n++ {
			result := par.Filter(collections, Collection.NumbersSumIsPositive)
			r = len(result) == 123
		}
		deadBool = r
	})
}

func BenchmarkReduce(b *testing.B) {
	rand.Seed(1)
	collections := CreateCollections(10000)

	b.Run("serial", func(b *testing.B) {
		var r bool
		for n := 0; n < b.N; n++ {
			result := collections[0]
			for _, c := range collections[1:] {
				result = result.JoinSums(c)
			}
			r = len(result.Numbers) == 123
		}
		deadBool = r
	})
	b.Run("parallel", func(b *testing.B) {
		var r bool
		for n := 0; n < b.N; n++ {
			result := par.Reduce(collections, Collection.JoinSums)
			r = len(result.Numbers) == 123
		}
		deadBool = r
	})
}

func BenchmarkAny(b *testing.B) {
	rand.Seed(1)
	collections := CreateCollections(10000)

	b.Run("serial with match", func(b *testing.B) {
		var r bool
		for n := 0; n < b.N; n++ {
			needle := collections[len(collections)*n/b.N].NumbersSum()
			var result bool
			for _, c := range collections {
				if c.NumbersSum() == needle {
					result = true
					break
				}
			}
			r = result
		}
		deadBool = r
	})
	b.Run("parallel with match", func(b *testing.B) {
		var r bool
		for n := 0; n < b.N; n++ {
			needle := collections[len(collections)*n/b.N].NumbersSum()
			r = par.Any(collections, func(c Collection) bool {
				return c.NumbersSum() == needle
			})
		}
		deadBool = r
	})
	b.Run("serial without match", func(b *testing.B) {
		var r bool
		for n := 0; n < b.N; n++ {
			needle := ^int(0)
			var result bool
			for _, c := range collections {
				if c.NumbersSum() == needle {
					result = true
					break
				}
			}
			r = result
		}
		deadBool = r
	})
	b.Run("parallel without match", func(b *testing.B) {
		var r bool
		for n := 0; n < b.N; n++ {
			needle := ^int(0)
			r = par.Any(collections, func(c Collection) bool {
				return c.NumbersSum() == needle
			})
		}
		deadBool = r
	})
}

func BenchmarkAll(b *testing.B) {
	rand.Seed(1)
	collections := CreateCollections(10000)

	b.Run("serial with match", func(b *testing.B) {
		var r bool
		for n := 0; n < b.N; n++ {
			needle := collections[len(collections)*n/b.N].NumbersSum()
			result := true
			for _, c := range collections {
				if c.NumbersSum() == needle {
					result = false
					break
				}
			}
			r = result
		}
		deadBool = r
	})
	b.Run("parallel with match", func(b *testing.B) {
		var r bool
		for n := 0; n < b.N; n++ {
			needle := collections[len(collections)*n/b.N].NumbersSum()
			r = par.All(collections, func(c Collection) bool {
				return c.NumbersSum() != needle
			})
		}
		deadBool = r
	})
	b.Run("serial without match", func(b *testing.B) {
		var r bool
		for n := 0; n < b.N; n++ {
			needle := ^int(0)
			result := true
			for _, c := range collections {
				if c.NumbersSum() == needle {
					result = false
					break
				}
			}
			r = result
		}
		deadBool = r
	})
	b.Run("parallel without match", func(b *testing.B) {
		var r bool
		for n := 0; n < b.N; n++ {
			needle := ^int(0)
			r = par.All(collections, func(c Collection) bool {
				return c.NumbersSum() != needle
			})
		}
		deadBool = r
	})
}

func BenchmarkNone(b *testing.B) {
	rand.Seed(1)
	collections := CreateCollections(10000)

	b.Run("serial with match", func(b *testing.B) {
		var r bool
		for n := 0; n < b.N; n++ {
			needle := collections[len(collections)*n/b.N].NumbersSum()
			result := true
			for _, c := range collections {
				if c.NumbersSum() == needle {
					result = false
					break
				}
			}
			r = result
		}
		deadBool = r
	})
	b.Run("parallel with match", func(b *testing.B) {
		var r bool
		for n := 0; n < b.N; n++ {
			needle := collections[len(collections)*n/b.N].NumbersSum()
			r = par.None(collections, func(c Collection) bool {
				return c.NumbersSum() == needle
			})
		}
		deadBool = r
	})
	b.Run("serial without match", func(b *testing.B) {
		var r bool
		for n := 0; n < b.N; n++ {
			needle := ^int(0)
			result := true
			for _, c := range collections {
				if c.NumbersSum() == needle {
					result = false
					break
				}
			}
			r = result
		}
		deadBool = r
	})
	b.Run("parallel without match", func(b *testing.B) {
		var r bool
		for n := 0; n < b.N; n++ {
			needle := ^int(0)
			r = par.None(collections, func(c Collection) bool {
				return c.NumbersSum() == needle
			})
		}
		deadBool = r
	})
}

type Collection struct {
	Numbers []int
}

func CreateCollections(size int) []Collection {
	collections := make([]Collection, size)
	for i := range collections {
		collections[i].Numbers = make([]int, 2000+rand.Intn(20000))
		for n := range collections[i].Numbers {
			collections[i].Numbers[n] = 500000 - rand.Intn(1000000)
		}
	}
	return collections
}

func (c Collection) NumbersSum() int {
	var sum int
	for _, v := range c.Numbers {
		sum += v
	}
	return sum
}

func (c Collection) NumbersSumIsPositive() bool {
	return c.NumbersSum() >= 0
}

func (c Collection) JoinSums(c2 Collection) Collection {
	numbers := make([]int, 2)
	numbers[0] = c.NumbersSum()
	numbers[1] = c2.NumbersSum()
	return Collection{numbers}
}

func assertSliceEquals[T comparable](tb testing.TB, expected, received []T) {
	tb.Helper()
	if len(expected) != len(received) {
		tb.Fatalf("expected a slice of len %d, got %d", len(expected), len(received))
	}
	for i := range expected {
		if expected[i] != received[i] {
			tb.Fatalf("expected `%#v` at index %d, got `%#v`", expected[i], i, received[i])
		}
	}
}

func assertEquals[T comparable](tb testing.TB, expected, received T) {
	tb.Helper()
	if expected != received {
		tb.Fatalf("expected `%#v`, got `%#v`", expected, received)
	}
}

func assertPanics(tb testing.TB, fn func()) {
	tb.Helper()
	defer func() {
		if err := recover(); err == nil {
			tb.Fatal("expected a panic")
		}
	}()
	fn()
}
