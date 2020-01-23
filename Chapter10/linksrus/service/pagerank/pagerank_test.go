package pagerank

import (
	"context"
	"testing"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/graph"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter10/linksrus/partition"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter10/linksrus/service/pagerank/mocks"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/juju/clock/testclock"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(ConfigTestSuite))
var _ = gc.Suite(new(PagerankTestSuite))

type ConfigTestSuite struct{}

func (s *ConfigTestSuite) TestConfigValidation(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	origCfg := Config{
		GraphAPI:          mocks.NewMockGraphAPI(ctrl),
		IndexAPI:          mocks.NewMockIndexAPI(ctrl),
		PartitionDetector: partition.Fixed{},
		ComputeWorkers:    4,
		UpdateInterval:    time.Minute,
	}

	cfg := origCfg
	c.Assert(cfg.validate(), gc.IsNil)
	c.Assert(cfg.Clock, gc.Not(gc.IsNil), gc.Commentf("default clock was not assigned"))
	c.Assert(cfg.Logger, gc.Not(gc.IsNil), gc.Commentf("default logger was not assigned"))

	cfg = origCfg
	cfg.GraphAPI = nil
	c.Assert(cfg.validate(), gc.ErrorMatches, "(?ms).*graph API has not been provided.*")

	cfg = origCfg
	cfg.IndexAPI = nil
	c.Assert(cfg.validate(), gc.ErrorMatches, "(?ms).*index API has not been provided.*")

	cfg = origCfg
	cfg.PartitionDetector = nil
	c.Assert(cfg.validate(), gc.ErrorMatches, "(?ms).*partition detector has not been provided.*")

	cfg = origCfg
	cfg.ComputeWorkers = 0
	c.Assert(cfg.validate(), gc.ErrorMatches, "(?ms).*invalid value for compute workers.*")

	cfg = origCfg
	cfg.UpdateInterval = 0
	c.Assert(cfg.validate(), gc.ErrorMatches, "(?ms).*invalid value for update interval.*")
}

type PagerankTestSuite struct{}

func (s *PagerankTestSuite) TestFullRun(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	mockGraph := mocks.NewMockGraphAPI(ctrl)
	mockIndex := mocks.NewMockIndexAPI(ctrl)
	clk := testclock.NewClock(time.Now())

	cfg := Config{
		GraphAPI:          mockGraph,
		IndexAPI:          mockIndex,
		PartitionDetector: partition.Fixed{Partition: 0, NumPartitions: 1},
		Clock:             clk,
		ComputeWorkers:    1,
		UpdateInterval:    time.Minute,
	}
	svc, err := NewService(cfg)
	c.Assert(err, gc.IsNil)

	ctx, cancelFn := context.WithCancel(context.TODO())
	defer cancelFn()

	uuid1, uuid2 := uuid.New(), uuid.New()

	mockLinkIt := mocks.NewMockLinkIterator(ctrl)
	gomock.InOrder(
		mockLinkIt.EXPECT().Next().Return(true),
		mockLinkIt.EXPECT().Link().Return(&graph.Link{ID: uuid1}),
		mockLinkIt.EXPECT().Next().Return(true),
		mockLinkIt.EXPECT().Link().Return(&graph.Link{ID: uuid2}),
		mockLinkIt.EXPECT().Next().Return(false),
	)
	mockLinkIt.EXPECT().Error().Return(nil)
	mockLinkIt.EXPECT().Close().Return(nil)

	mockEdgeIt := mocks.NewMockEdgeIterator(ctrl)
	gomock.InOrder(
		mockEdgeIt.EXPECT().Next().Return(true),
		mockEdgeIt.EXPECT().Edge().Return(&graph.Edge{Src: uuid1, Dst: uuid2}),
		mockEdgeIt.EXPECT().Next().Return(true),
		mockEdgeIt.EXPECT().Edge().Return(&graph.Edge{Src: uuid2, Dst: uuid1}),
		mockEdgeIt.EXPECT().Next().Return(false),
	)
	mockEdgeIt.EXPECT().Error().Return(nil)
	mockEdgeIt.EXPECT().Close().Return(nil)

	expLinkFilterTime := clk.Now().Add(cfg.UpdateInterval)
	maxUUID := uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	mockGraph.EXPECT().Links(uuid.Nil, maxUUID, expLinkFilterTime).Return(mockLinkIt, nil)
	mockGraph.EXPECT().Edges(uuid.Nil, maxUUID, expLinkFilterTime).Return(mockEdgeIt, nil)

	mockIndex.EXPECT().UpdateScore(uuid1, 0.5)
	mockIndex.EXPECT().UpdateScore(uuid2, 0.5)

	go func() {
		// Wait until the main loop calls time.After (or timeout if
		// 10 sec elapse) and advance the time to trigger a new pagerank
		// pass.
		c.Assert(clk.WaitAdvance(time.Minute, 10*time.Second, 1), gc.IsNil)

		// Wait until the main loop calls time.After again and cancel
		// the context.
		c.Assert(clk.WaitAdvance(time.Millisecond, 10*time.Second, 1), gc.IsNil)
		cancelFn()
	}()

	// Enter the blocking main loop
	err = svc.Run(ctx)
	c.Assert(err, gc.IsNil)
}

func (s *PagerankTestSuite) TestRunWhileInNonZeroPartition(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	clk := testclock.NewClock(time.Now())

	cfg := Config{
		GraphAPI:          mocks.NewMockGraphAPI(ctrl),
		IndexAPI:          mocks.NewMockIndexAPI(ctrl),
		PartitionDetector: partition.Fixed{Partition: 1, NumPartitions: 2},
		Clock:             clk,
		ComputeWorkers:    1,
		UpdateInterval:    time.Minute,
	}
	svc, err := NewService(cfg)
	c.Assert(err, gc.IsNil)

	go func() {
		// Wait until the main loop calls time.After and advance the time.
		// The service will check the partition information, see that
		// it is not assigned to partition 0 and exit the main loop.
		c.Assert(clk.WaitAdvance(time.Minute, 10*time.Second, 1), gc.IsNil)
	}()

	// Enter the blocking main loop
	err = svc.Run(context.TODO())
	c.Assert(err, gc.IsNil)
}

func Test(t *testing.T) {
	// Run all gocheck test-suites
	gc.TestingT(t)
}
