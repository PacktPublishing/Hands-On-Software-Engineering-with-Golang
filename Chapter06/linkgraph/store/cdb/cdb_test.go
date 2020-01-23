package cdb

import (
	"database/sql"
	"os"
	"testing"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/graph/graphtest"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(CockroachDbGraphTestSuite))

func Test(t *testing.T) { gc.TestingT(t) }

type CockroachDbGraphTestSuite struct {
	graphtest.SuiteBase
	db *sql.DB
}

func (s *CockroachDbGraphTestSuite) SetUpSuite(c *gc.C) {
	dsn := os.Getenv("CDB_DSN")
	if dsn == "" {
		c.Skip("Missing CDB_DSN envvar; skipping cockroachdb-backed graph test suite")
	}

	g, err := NewCockroachDbGraph(dsn)
	c.Assert(err, gc.IsNil)
	s.SetGraph(g)
	s.db = g.db
}

func (s *CockroachDbGraphTestSuite) SetUpTest(c *gc.C) {
	s.flushDB(c)
}

func (s *CockroachDbGraphTestSuite) TearDownSuite(c *gc.C) {
	if s.db != nil {
		s.flushDB(c)
		c.Assert(s.db.Close(), gc.IsNil)
	}
}

func (s *CockroachDbGraphTestSuite) flushDB(c *gc.C) {
	_, err := s.db.Exec("DELETE FROM links")
	c.Assert(err, gc.IsNil)
	_, err = s.db.Exec("DELETE FROM edges")
	c.Assert(err, gc.IsNil)
}
