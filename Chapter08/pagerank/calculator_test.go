package pagerank_test

import (
	"context"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/pagerank"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(CalculatorTestSuite))

func Test(t *testing.T) {
	// Run all gocheck test-suites
	gc.TestingT(t)
}

type edge struct {
	src, dst string
}

type spec struct {
	descr     string
	vertices  []string
	edges     []edge
	expScores map[string]float64
}

type CalculatorTestSuite struct {
}

func (s *CalculatorTestSuite) TestSimpleGraphCase1(c *gc.C) {
	spec := spec{
		descr: `
 (A) -> (B) -> (C)
  ^             |
  |             |
  +-------------+

Expect PageRank score to be distributed evenly across the three nodes.
`,
		vertices: []string{"A", "B", "C"},
		edges: []edge{
			{"A", "B"},
			{"B", "C"},
			{"C", "A"},
		},
		expScores: map[string]float64{
			"A": 1.0 / 3.0,
			"B": 1.0 / 3.0,
			"C": 1.0 / 3.0,
		},
	}

	s.assertPageRankScores(c, spec)
}

func (s *CalculatorTestSuite) TestSimpleGraphCase2(c *gc.C) {
	spec := spec{
		descr: `
  +--(A)<-+
  |       |
  V       |
 (B) <-> (C)

Expect B and C to get better score than A due to the back-link between them.
Also, B should get slightly better score than C as there are two links pointing
to it.
`,
		vertices: []string{"A", "B", "C"},
		edges: []edge{
			{"A", "B"},
			{"B", "C"},
			{"C", "A"},
			{"C", "B"},
		},
		expScores: map[string]float64{
			"A": 0.2145,
			"B": 0.3937,
			"C": 0.3879,
		},
	}

	s.assertPageRankScores(c, spec)
}

func (s *CalculatorTestSuite) TestSimpleGraphCase3(c *gc.C) {
	spec := spec{
		descr: `
 (A) <-> (B) <-> (C)

Expect A and C to get the same score and B to get the largest score since there 
are two links pointing to it.
`,
		vertices: []string{"A", "B", "C"},
		edges: []edge{
			{"A", "B"},
			{"B", "A"},
			{"B", "C"},
			{"C", "B"},
		},
		expScores: map[string]float64{
			"A": 0.2569,
			"B": 0.4860,
			"C": 0.2569,
		},
	}

	s.assertPageRankScores(c, spec)
}

func (s *CalculatorTestSuite) TestDeadEnd(c *gc.C) {
	spec := spec{
		descr: `
 (A) -> (B) -> (C)

Expect that S(C) < S(A) < S(B). C is a dead-end as it has no outgoing links.
The algorithm deals with such cases by transferring C's score to a random node
in the graph; essentially, it's like C is connected to all other nodes in the
graph. As a result, A and C get a backlink from C; B now has two links pointing
at it (from A and C's backlink) and hence has the biggest score. Due to the 
random teleportation from C, C will get a slightly lower score than A.
`,
		vertices: []string{"A", "B", "C"},
		edges: []edge{
			{"A", "B"},
			{"B", "C"},
		},
		expScores: map[string]float64{
			"A": 0.1842,
			"B": 0.3411,
			"C": 0.4745,
		},
	}

	s.assertPageRankScores(c, spec)
}

func (s *CalculatorTestSuite) TestConvergenceForLargeGraphs(c *gc.C) {
	s.assertConvergence(c, 100000, 7)
}

func (s *CalculatorTestSuite) assertConvergence(c *gc.C, numLinks, maxOutLinks int) {
	calc, err := pagerank.NewCalculator(pagerank.Config{ComputeWorkers: 32, MinSADForConvergence: 0.001})
	c.Assert(err, gc.IsNil)
	defer func() { _ = calc.Close() }()

	// Make the graph generation and teleports deterministic for each test.
	rand.Seed(42)

	names := make([]string, numLinks)
	for i := 0; i < numLinks; i++ {
		names[i] = strconv.FormatInt(int64(i), 10)
	}

	start := time.Now()
	for i := 0; i < numLinks; i++ {
		calc.AddVertex(names[i])

		outLinks := rand.Intn(maxOutLinks)
		for j := 0; j < outLinks; j++ {
			dst := rand.Intn(numLinks)
			c.Assert(calc.AddEdge(names[i], names[dst]), gc.IsNil)
		}
	}
	c.Logf("constructed %d nodes in %v", numLinks, time.Since(start).Truncate(time.Millisecond).String())

	start = time.Now()
	ex := calc.Executor()
	err = ex.RunToCompletion(context.TODO())
	c.Assert(err, gc.IsNil)
	c.Logf("converged %d nodes after %d steps in %v", numLinks, ex.Superstep(), time.Since(start).Truncate(time.Millisecond).String())

	var prSum float64
	err = calc.Scores(func(id string, score float64) error {
		prSum += score
		return nil
	})
	c.Assert(err, gc.IsNil)

	c.Assert((1.0-prSum) <= 0.001, gc.Equals, true, gc.Commentf("expected all pagerank scores to add up to 1.0; got %f", prSum))
}

func (s *CalculatorTestSuite) assertPageRankScores(c *gc.C, spec spec) {
	c.Log(spec.descr)

	// Make teleports deterministic for each test.
	rand.Seed(42)

	calc, err := pagerank.NewCalculator(pagerank.Config{
		ComputeWorkers: 2,
		DampingFactor:  0.85,
	})
	c.Assert(err, gc.IsNil)
	defer func() { _ = calc.Close() }()

	for _, id := range spec.vertices {
		calc.AddVertex(id)
	}
	for _, e := range spec.edges {
		c.Assert(calc.AddEdge(e.src, e.dst), gc.IsNil)
	}

	ex := calc.Executor()
	err = ex.RunToCompletion(context.TODO())
	c.Assert(err, gc.IsNil)
	c.Logf("converged after %d steps", ex.Superstep())

	var prSum float64
	err = calc.Scores(func(id string, score float64) error {
		prSum += score
		absDelta := math.Abs(score - spec.expScores[id])
		c.Assert(absDelta <= 0.01, gc.Equals, true, gc.Commentf("expected score for %v to be %f ± 0.01; got %f (abs. delta %f)", id, spec.expScores[id], score, absDelta))
		return nil
	})
	c.Assert(err, gc.IsNil)

	c.Assert((1.0-prSum) <= 0.001, gc.Equals, true, gc.Commentf("expected all pagerank scores to add up to 1.0; got %f", prSum))
}
