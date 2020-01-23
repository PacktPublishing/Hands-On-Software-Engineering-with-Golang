package aggregator

import (
	"math"
	"sync/atomic"
	"unsafe"
)

// Float64Accumulator implements a concurrent-safe accumulator for float64 values.
type Float64Accumulator struct {
	prevSum float64
	curSum  float64
}

// Type implements bspgraph.Aggregator.
func (a *Float64Accumulator) Type() string {
	return "Float64Accumulator"
}

// Get returns the current value of the accumulator.
func (a *Float64Accumulator) Get() interface{} {
	return loadFloat64(&a.curSum)
}

// Set the current value of the accumulator.
func (a *Float64Accumulator) Set(v interface{}) {
	for v64 := v.(float64); ; {
		oldCur := loadFloat64(&a.curSum)
		oldPrev := loadFloat64(&a.prevSum)
		swappedCur := atomic.CompareAndSwapUint64(
			(*uint64)(unsafe.Pointer(&a.curSum)),
			math.Float64bits(oldCur),
			math.Float64bits(v64),
		)
		swappedPrev := atomic.CompareAndSwapUint64(
			(*uint64)(unsafe.Pointer(&a.prevSum)),
			math.Float64bits(oldPrev),
			math.Float64bits(v64),
		)
		if swappedCur && swappedPrev {
			return
		}
	}
}

// Aggregate adds a float64 value to the accumulator.
func (a *Float64Accumulator) Aggregate(v interface{}) {
	for v64 := v.(float64); ; {
		oldV := loadFloat64(&a.curSum)
		newV := oldV + v64
		if atomic.CompareAndSwapUint64(
			(*uint64)(unsafe.Pointer(&a.curSum)),
			math.Float64bits(oldV),
			math.Float64bits(newV),
		) {
			return
		}
	}
}

// Delta returns the delta change in the accumulator value since the last time
// it was invoked or the last time that Set was invoked.
func (a *Float64Accumulator) Delta() interface{} {
	for {
		curSum := loadFloat64(&a.curSum)
		prevSum := loadFloat64(&a.prevSum)
		if atomic.CompareAndSwapUint64(
			(*uint64)(unsafe.Pointer(&a.prevSum)),
			math.Float64bits(prevSum),
			math.Float64bits(curSum),
		) {
			return curSum - prevSum
		}
	}
}

func loadFloat64(v *float64) float64 {
	return math.Float64frombits(
		atomic.LoadUint64((*uint64)(unsafe.Pointer(v))),
	)
}

// IntAccumulator implements a concurrent-safe accumulator for int values.
type IntAccumulator struct {
	prevSum int64
	curSum  int64
}

// Type implements bspgraph.Aggregator.
func (a *IntAccumulator) Type() string {
	return "IntAccumulator"
}

// Get returns the current value of the accumulator.
func (a *IntAccumulator) Get() interface{} {
	return int(atomic.LoadInt64(&a.curSum))
}

// Set the current value of the accumulator.
func (a *IntAccumulator) Set(v interface{}) {
	for v64 := int64(v.(int)); ; {
		oldCur := a.curSum
		oldPrev := a.prevSum
		swappedCur := atomic.CompareAndSwapInt64(&a.curSum, oldCur, v64)
		swappedPrev := atomic.CompareAndSwapInt64(&a.prevSum, oldPrev, v64)
		if swappedCur && swappedPrev {
			return
		}
	}
}

// Aggregate adds a int value to the accumulator.
func (a *IntAccumulator) Aggregate(v interface{}) {
	_ = atomic.AddInt64(&a.curSum, int64(v.(int)))
}

// Delta returns the delta change in the accumulator value since the last time
// it was invoked or the last time that Set was invoked.
func (a *IntAccumulator) Delta() interface{} {
	for {
		curSum := atomic.LoadInt64(&a.curSum)
		prevSum := atomic.LoadInt64(&a.prevSum)
		if atomic.CompareAndSwapInt64(&a.prevSum, prevSum, curSum) {
			return int(curSum - prevSum)
		}
	}
}
