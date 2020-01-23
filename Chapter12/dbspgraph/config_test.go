package dbspgraph

import (
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/dbspgraph/mocks"
	"github.com/golang/mock/gomock"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(ConfigTestSuite))

type ConfigTestSuite struct {
}

func (s *ConfigTestSuite) TestMasterConfigValidation(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	origCfg := MasterConfig{
		ListenAddress: ":0",
		JobRunner:     mocks.NewMockRunner(ctrl),
		Serializer:    mocks.NewMockSerializer(ctrl),
	}

	cfg := origCfg
	c.Assert(cfg.Validate(), gc.IsNil)
	c.Assert(cfg.Logger, gc.Not(gc.IsNil), gc.Commentf("default logger was not assigned"))

	cfg = origCfg
	cfg.ListenAddress = ""
	c.Assert(cfg.Validate(), gc.ErrorMatches, "(?ms).*listen address not specified.*")

	cfg = origCfg
	cfg.JobRunner = nil
	c.Assert(cfg.Validate(), gc.ErrorMatches, "(?ms).*job runner not specified.*")

	cfg = origCfg
	cfg.Serializer = nil
	c.Assert(cfg.Validate(), gc.ErrorMatches, "(?ms).*serializer not specified.*")
}

func (s *ConfigTestSuite) TestWorkerConfigValidation(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	origCfg := WorkerConfig{
		JobRunner:  mocks.NewMockRunner(ctrl),
		Serializer: mocks.NewMockSerializer(ctrl),
	}

	cfg := origCfg
	c.Assert(cfg.Validate(), gc.IsNil)
	c.Assert(cfg.Logger, gc.Not(gc.IsNil), gc.Commentf("default logger was not assigned"))

	cfg = origCfg
	cfg.JobRunner = nil
	c.Assert(cfg.Validate(), gc.ErrorMatches, "(?ms).*job runner not specified.*")

	cfg = origCfg
	cfg.Serializer = nil
	c.Assert(cfg.Validate(), gc.ErrorMatches, "(?ms).*serializer not specified.*")
}
