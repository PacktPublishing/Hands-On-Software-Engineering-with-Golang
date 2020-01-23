package memory

import (
	"testing"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/graph/graphtest"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(InMemoryGraphTestSuite))

func Test(t *testing.T) { gc.TestingT(t) }

type InMemoryGraphTestSuite struct {
	graphtest.SuiteBase
}

func (s *InMemoryGraphTestSuite) SetUpTest(c *gc.C) {
	s.SetGraph(NewInMemoryGraph())
}
