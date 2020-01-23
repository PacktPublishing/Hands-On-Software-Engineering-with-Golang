package color_test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/color"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(ColorGraphTestSuite))

type ColorGraphTestSuite struct {
	assigner *color.Assigner
}

func (s *ColorGraphTestSuite) SetUpTest(c *gc.C) {
	assigner, err := color.NewColorAssigner(16)
	c.Assert(err, gc.IsNil)
	s.assigner = assigner
}

func (s *ColorGraphTestSuite) TearDownTest(c *gc.C) {
	c.Assert(s.assigner.Close(), gc.IsNil)
}

func (s *ColorGraphTestSuite) TestUnColoredGraph(c *gc.C) {
	// Ensure we always use the same seed to make the test deterministic
	rand.Seed(42)

	adjMap := map[string][]string{
		"0": {"1", "2"},
		"1": {"2", "3"},
		"2": {"3"},
		"3": {"4"},
	}
	outDeg := s.setupGraph(c, adjMap, nil)

	colorMap := make(map[string]int)
	numColors, err := s.assigner.AssignColors(context.TODO(), func(id string, color int) {
		colorMap[id] = color
	})
	c.Assert(err, gc.IsNil)

	maxColors := outDeg + 1
	c.Assert(numColors <= maxColors, gc.Equals, true, gc.Commentf("number of colors should not exceed (max vertex out degree + 1)"))
	assertNoColorConflictWithNeighbors(c, adjMap, colorMap)
}

func (s *ColorGraphTestSuite) TestPartiallyPrecoloredColoredGraph(c *gc.C) {
	// Ensure we always use the same seed to make the test deterministic
	rand.Seed(101)

	preColoredVerts := map[string]int{
		"0": 1,
		"3": 1,
	}
	adjMap := map[string][]string{
		"0": {"1", "2"},
		"1": {"2", "3"},
		"2": {"3"},
		"3": {"4"},
	}
	outDeg := s.setupGraph(c, adjMap, preColoredVerts)

	colorMap := make(map[string]int)
	numColors, err := s.assigner.AssignColors(context.TODO(), func(id string, color int) {
		colorMap[id] = color
		if fixedColor := preColoredVerts[id]; fixedColor != 0 {
			c.Assert(color, gc.Equals, fixedColor, gc.Commentf("pre-colored vertex %v color was overwritten from %d to %d", id, fixedColor, color))
		}
	})
	c.Assert(err, gc.IsNil)

	maxColors := outDeg + 1
	c.Assert(numColors <= maxColors, gc.Equals, true, gc.Commentf("number of colors should not exceed (max vertex out degree + 1)"))
	assertNoColorConflictWithNeighbors(c, adjMap, colorMap)
}

func (s *ColorGraphTestSuite) setupGraph(c *gc.C, adjMap map[string][]string, preColoredVerts map[string]int) int {
	uniqueVerts := make(map[string]struct{})
	for src, dsts := range adjMap {
		uniqueVerts[src] = struct{}{}
		for _, dst := range dsts {
			uniqueVerts[dst] = struct{}{}
		}
	}

	if preColoredVerts == nil {
		preColoredVerts = make(map[string]int)
	}
	for id := range uniqueVerts {
		if fixedColor := preColoredVerts[id]; fixedColor != 0 {
			s.assigner.AddPreColoredVertex(id, fixedColor)
		} else {
			s.assigner.AddVertex(id)
		}
	}

	for src, dsts := range adjMap {
		for _, dst := range dsts {
			c.Assert(s.assigner.AddUndirectedEdge(src, dst), gc.IsNil)
		}
	}

	var maxOutDeg int
	for _, v := range s.assigner.Graph().Vertices() {
		if deg := len(v.Edges()); deg > maxOutDeg {
			maxOutDeg = deg
		}
	}
	return maxOutDeg
}

func assertNoColorConflictWithNeighbors(c *gc.C, adjMap map[string][]string, colorMap map[string]int) {
	for srcID, srcColor := range colorMap {
		c.Assert(srcColor, gc.Not(gc.Equals), 0, gc.Commentf("no color assigned to vertex %v", srcID))

		for _, dstID := range adjMap[srcID] {
			c.Assert(colorMap[dstID], gc.Not(gc.Equals), srcColor, gc.Commentf("neighbor vertex %d assigned same color %d as vertex %d", dstID, srcColor, srcID))
		}
	}
}

func Test(t *testing.T) {
	// Run all gocheck test-suites
	gc.TestingT(t)
}
