package shortestpath_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/shortestpath"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(ShortestPathTestSuite))

type ShortestPathTestSuite struct{}

func (s *ShortestPathTestSuite) TestShortestPathCostTo(c *gc.C) {
	calc, err := shortestpath.NewCalculator(4)
	c.Assert(err, gc.IsNil)

	for i := 0; i < 9; i++ {
		calc.AddVertex(fmt.Sprint(i))
	}

	// costMat[i][j] is the cost of an edge (if non zero) from i -> j. The
	// matrix is symmetric as the edges are un-directed.
	costMat := [][]int{
		{0, 4, 0, 0, 0, 0, 0, 8, 0},
		{4, 0, 8, 0, 0, 0, 0, 11, 0},
		{0, 8, 0, 7, 0, 4, 0, 0, 2},
		{0, 0, 7, 0, 9, 14, 0, 0, 0},
		{0, 0, 0, 9, 0, 10, 0, 0, 0},
		{0, 0, 4, 0, 10, 0, 2, 0, 0},
		{0, 0, 0, 14, 0, 2, 0, 1, 6},
		{8, 11, 0, 0, 0, 0, 1, 0, 7},
		{0, 0, 2, 0, 0, 0, 6, 7, 0},
	}

	for src, dstWeights := range costMat {
		for dst, weight := range dstWeights {
			if weight == 0 {
				continue
			}

			err = calc.AddEdge(fmt.Sprint(src), fmt.Sprint(dst), weight)
			c.Assert(err, gc.IsNil)
		}
	}

	pathSrc := 0
	err = calc.CalculateShortestPaths(context.TODO(), fmt.Sprint(pathSrc))
	c.Assert(err, gc.IsNil)

	expPaths := []struct {
		path []string
		cost int
	}{
		{
			path: []string{"0"},
			cost: 0,
		},
		{
			path: []string{"0", "1"},
			cost: 4,
		},
		{
			path: []string{"0", "1", "2"},
			cost: 12,
		},
		{
			path: []string{"0", "1", "2", "3"},
			cost: 19,
		},
		{
			path: []string{"0", "7", "6", "5", "4"},
			cost: 21,
		},
		{
			path: []string{"0", "7", "6", "5"},
			cost: 11,
		},
		{
			path: []string{"0", "7", "6"},
			cost: 9,
		},
		{
			path: []string{"0", "7"},
			cost: 8,
		},
		{
			path: []string{"0", "1", "2", "8"},
			cost: 14,
		},
	}
	for dst, exp := range expPaths {
		gotPath, gotCost, err := calc.ShortestPathTo(fmt.Sprint(dst))
		c.Assert(err, gc.IsNil)
		c.Assert(gotPath, gc.DeepEquals, exp.path)
		c.Assert(gotCost, gc.Equals, exp.cost, gc.Commentf("path from %d -> %d", pathSrc, dst))
	}
}

func Test(t *testing.T) {
	// Run all gocheck test-suites
	gc.TestingT(t)
}
