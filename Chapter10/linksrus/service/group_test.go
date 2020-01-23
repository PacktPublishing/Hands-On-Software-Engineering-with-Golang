package service

import (
	"context"
	"testing"
	"time"

	"golang.org/x/xerrors"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(GroupTestSuite))

type GroupTestSuite struct {
}

func (s *GroupTestSuite) TestGroupTerminatesWithOneError(c *gc.C) {
	grp := Group{
		dummyService{id: "0"},
		dummyService{id: "1", err: xerrors.Errorf("cannot connect to API")},
		dummyService{id: "2"},
	}

	err := grp.Run(context.TODO())
	c.Assert(err, gc.Not(gc.IsNil))
	c.Assert(err, gc.ErrorMatches, "(?ms).*1: cannot connect to API.*")
}

func (s *GroupTestSuite) TestGroupTerminatesWithMultipleErrors(c *gc.C) {
	grp := Group{
		dummyService{id: "0"},
		dummyService{id: "1", err: xerrors.Errorf("cannot connect to API")},
		dummyService{id: "2", err: xerrors.Errorf("cannot connect to API")},
	}

	err := grp.Run(context.TODO())
	c.Assert(err, gc.ErrorMatches, "(?ms).*1: cannot connect to API.*")
	c.Assert(err, gc.ErrorMatches, "(?ms).*2: cannot connect to API.*")
}

func (s *GroupTestSuite) TestGroupTerminatesFromContext(c *gc.C) {
	grp := Group{
		dummyService{id: "0"},
		dummyService{id: "1"},
		dummyService{id: "2"},
	}

	ctx, cancelFn := context.WithTimeout(context.TODO(), 200*time.Millisecond)
	defer cancelFn()
	err := grp.Run(ctx)
	c.Assert(err, gc.IsNil)
}

type dummyService struct {
	id  string
	err error
}

func (s dummyService) Name() string { return s.id }
func (s dummyService) Run(ctx context.Context) error {
	if s.err != nil {
		return s.err
	}

	<-ctx.Done()
	return nil
}

func Test(t *testing.T) {
	// Run all gocheck test-suites
	gc.TestingT(t)
}
