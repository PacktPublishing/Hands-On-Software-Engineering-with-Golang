package service

import (
	"context"
	"io/ioutil"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/bspgraph"
	pr "github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/pagerank"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/dbspgraph"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/dbspgraph/job"
	"github.com/hashicorp/go-multierror"
	"github.com/juju/clock"
	"github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

// MasterConfig encapsulates the settings for configuring the master node for
// the PageRank calculator service.
type MasterConfig struct {
	// The address to listen for incoming worker connections.
	ListenAddress string

	// The minimum required number of connected workers for starting a new
	// PageRank pass. If not specified, a new pass will start when at least
	// one worker has connected.
	MinWorkers int

	// The timeout for the required number of workers to connect before
	// aborting a new pass attempt. If not specified, the master will wait
	// indefinitely.
	WorkerAcquireTimeout time.Duration

	// A clock instance for generating time-related events. If not specified,
	// the default wall-clock will be used instead.
	Clock clock.Clock

	// The time between subsequent pagerank updates.
	UpdateInterval time.Duration

	// The logger to use. If not defined an output-discarding logger will
	// be used instead.
	Logger *logrus.Entry
}

func (cfg *MasterConfig) validate() error {
	var err error
	if cfg.ListenAddress == "" {
		err = multierror.Append(err, xerrors.Errorf("invalid value for listen address"))
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.WallClock
	}
	if cfg.UpdateInterval == 0 {
		err = multierror.Append(err, xerrors.Errorf("invalid value for update interval"))
	}
	if cfg.Logger == nil {
		cfg.Logger = logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard})
	}
	return err
}

// MasterNode implements a master node for calculating PageRank scores in a
// distributed fashion.
type MasterNode struct {
	cfg        MasterConfig
	calculator *pr.Calculator

	masterFacade *dbspgraph.Master

	// Stats
	jobStartedAt time.Time
}

// NewMasterNode creates a new master node for the PageRank calculator service.
func NewMasterNode(cfg MasterConfig) (*MasterNode, error) {
	if err := cfg.validate(); err != nil {
		return nil, xerrors.Errorf("pagerank service: config validation failed: %w", err)
	}

	calculator, err := pr.NewCalculator(pr.Config{ComputeWorkers: 1})
	if err != nil {
		return nil, xerrors.Errorf("pagerank service: config validation failed: %w", err)
	}

	masterNode := &MasterNode{
		cfg:        cfg,
		calculator: calculator,
	}

	if masterNode.masterFacade, err = dbspgraph.NewMaster(dbspgraph.MasterConfig{
		ListenAddress: cfg.ListenAddress,
		JobRunner:     masterNode,
		Serializer:    serializer{},
		Logger:        cfg.Logger,
	}); err != nil {
		_ = calculator.Close()
		return nil, err
	}

	if err = masterNode.masterFacade.Start(); err != nil {
		_ = calculator.Close()
		return nil, err
	}

	return masterNode, nil
}

// Run implements the main loop of the master node for the distributed PageRank
// calculator. It periodically wakes up and orchestrates the execution of a new
// PageRank update pass across all connected workers.
//
// Run blocks until the provided context expires.
func (n *MasterNode) Run(ctx context.Context) error {
	n.cfg.Logger.WithField("update_interval", n.cfg.UpdateInterval.String()).Info("starting service")
	defer func() {
		_ = n.masterFacade.Close()
		_ = n.calculator.Close()
		n.cfg.Logger.Info("stopped service")
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-n.cfg.Clock.After(n.cfg.UpdateInterval):
			if err := n.masterFacade.RunJob(ctx, n.cfg.MinWorkers, n.cfg.WorkerAcquireTimeout); err != nil {
				if err == dbspgraph.ErrUnableToReserveWorkers {
					n.cfg.Logger.WithFields(logrus.Fields{
						"min_workers":     n.cfg.MinWorkers,
						"acquire_timeout": n.cfg.WorkerAcquireTimeout.String(),
					}).Error("unable to acquire the requested number of workers")
					continue
				}
				n.cfg.Logger.WithField("err", err).Error("PageRank update job failed")
			}
		}
	}
}

// StartJob implements job.Runner. It initializes the underlying bspgraph.Graph
// instance and invokes the provided ExecutorFactory to create an executor for
// the graph supersteps.
func (n *MasterNode) StartJob(_ job.Details, execFactory bspgraph.ExecutorFactory) (*bspgraph.Executor, error) {
	if err := n.calculator.Graph().Reset(); err != nil {
		return nil, err
	}

	n.jobStartedAt = n.cfg.Clock.Now()
	n.calculator.SetExecutorFactory(execFactory)
	return n.calculator.Executor(), nil
}

// CompleteJob implements job.Runner.
func (n *MasterNode) CompleteJob(_ job.Details) error {
	n.cfg.Logger.WithFields(logrus.Fields{
		"total_link_count": n.calculator.Graph().Aggregator("page_count").Get(),
		"total_pass_time":  n.cfg.Clock.Now().Sub(n.jobStartedAt).String(),
	}).Info("completed PageRank update pass")
	return nil
}

// AbortJob implements job.Runner.
func (n *MasterNode) AbortJob(_ job.Details) {}
