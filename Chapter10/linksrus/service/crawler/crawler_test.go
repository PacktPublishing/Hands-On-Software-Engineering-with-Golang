package crawler

import (
	"context"
	"testing"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter10/linksrus/partition"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter10/linksrus/service/crawler/mocks"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/juju/clock/testclock"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(ConfigTestSuite))
var _ = gc.Suite(new(CrawlerTestSuite))

type ConfigTestSuite struct{}

func (s *ConfigTestSuite) TestConfigValidation(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	origCfg := Config{
		GraphAPI:          mocks.NewMockGraphAPI(ctrl),
		IndexAPI:          mocks.NewMockIndexAPI(ctrl),
		PartitionDetector: partition.Fixed{},
		FetchWorkers:      4,
		UpdateInterval:    time.Minute,
		ReIndexThreshold:  time.Minute,
	}

	cfg := origCfg
	c.Assert(cfg.validate(), gc.IsNil)
	c.Assert(cfg.PrivateNetworkDetector, gc.Not(gc.IsNil), gc.Commentf("default private network detector was not assigned"))
	c.Assert(cfg.URLGetter, gc.Not(gc.IsNil), gc.Commentf("default URL getter was not assigned"))
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
	cfg.FetchWorkers = 0
	c.Assert(cfg.validate(), gc.ErrorMatches, "(?ms).*invalid value for fetch workers.*")

	cfg = origCfg
	cfg.UpdateInterval = 0
	c.Assert(cfg.validate(), gc.ErrorMatches, "(?ms).*invalid value for update interval.*")

	cfg = origCfg
	cfg.ReIndexThreshold = 0
	c.Assert(cfg.validate(), gc.ErrorMatches, "(?ms).*invalid value for re-index threshold.*")
}

type CrawlerTestSuite struct {
}

func (s *CrawlerTestSuite) TestFullRun(c *gc.C) {
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
		FetchWorkers:      1,
		UpdateInterval:    time.Minute,
		ReIndexThreshold:  12 * time.Hour,
	}
	svc, err := NewService(cfg)
	c.Assert(err, gc.IsNil)

	ctx, cancelFn := context.WithCancel(context.TODO())
	defer cancelFn()

	mockIt := mocks.NewMockLinkIterator(ctrl)
	mockIt.EXPECT().Next().Return(false)
	mockIt.EXPECT().Error().Return(nil)
	mockIt.EXPECT().Close().Return(nil)
	expLinkFilterTime := clk.Now().Add(cfg.UpdateInterval).Add(-cfg.ReIndexThreshold)
	mockGraph.EXPECT().Links(
		uuid.Nil,
		uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"),
		expLinkFilterTime,
	).Return(mockIt, nil)

	go func() {
		// Wait until the main loop calls time.After (or timeout if 10
		// sec elapse) and advance the time to trigger a new crawler
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

func Test(t *testing.T) {
	// Run all gocheck test-suites
	gc.TestingT(t)
}
