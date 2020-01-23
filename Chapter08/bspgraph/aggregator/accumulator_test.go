package aggregator

import (
	"math"
	"math/rand"
	"testing"

	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(AccumulatorTestSuite))

type aggregator interface {
	Set(interface{})
	Get() interface{}
	Aggregate(interface{})
	Delta() interface{}
}

func Test(t *testing.T) {
	// Run all gocheck test-suites
	gc.TestingT(t)
}

type AccumulatorTestSuite struct {
}

func (s *AccumulatorTestSuite) TestFloat64Accumulator(c *gc.C) {
	numValues := 100
	values := make([]interface{}, numValues)
	var exp float64
	for i := 0; i < numValues; i++ {
		next := rand.Float64()
		values[i] = next
		exp += next
	}

	got := s.testConcurrentAccess(new(Float64Accumulator), values).(float64)
	absDelta := math.Abs(exp - got)
	c.Assert(absDelta < 1e-6, gc.Equals, true, gc.Commentf("expected to get %f; got %f; |delta| %f > 1e-6", exp, got, absDelta))
}

func (s *AccumulatorTestSuite) TestIntAccumulator(c *gc.C) {
	numValues := 100
	values := make([]interface{}, numValues)
	var exp int
	for i := 0; i < numValues; i++ {
		next := rand.Int()
		values[i] = next
		exp += next
	}

	got := s.testConcurrentAccess(new(IntAccumulator), values).(int)
	c.Assert(got, gc.Equals, exp)
}

func (s *AccumulatorTestSuite) testConcurrentAccess(a aggregator, values []interface{}) interface{} {
	startedCh := make(chan struct{})
	syncCh := make(chan struct{})
	doneCh := make(chan struct{})
	for i := 0; i < len(values); i++ {
		go func(i int) {
			startedCh <- struct{}{}
			<-syncCh
			a.Aggregate(values[i])
			doneCh <- struct{}{}
		}(i)
	}

	// Wait for all go-routines to start
	for i := 0; i < len(values); i++ {
		<-startedCh
	}

	// Allow each go-routine to update theh accumulator
	close(syncCh)

	// Wait for all go-routines to exit
	for i := 0; i < len(values); i++ {
		<-doneCh
	}

	return a.Get()
}
