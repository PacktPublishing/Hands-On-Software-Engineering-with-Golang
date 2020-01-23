package pagerank

import (
	"context"
	"io/ioutil"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/graph"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/bspgraph"
	pr "github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/pagerank"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter10/linksrus/partition"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/juju/clock"
	"github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

//go:generate mockgen -package mocks -destination mocks/mocks.go github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter10/linksrus/service/pagerank GraphAPI,IndexAPI
//go:generate mockgen -package mocks -destination mocks/mock_iterator.go github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/graph LinkIterator,EdgeIterator

// GraphAPI defines as set of API methods for fetching the links and edges from
// the link graph.
type GraphAPI interface {
	Links(fromID, toID uuid.UUID, retrievedBefore time.Time) (graph.LinkIterator, error)
	Edges(fromID, toID uuid.UUID, updatedBefore time.Time) (graph.EdgeIterator, error)
}

// IndexAPI defines a set of API methods for updating PageRank scores for
// indexed documents.
type IndexAPI interface {
	UpdateScore(linkID uuid.UUID, score float64) error
}

// Config encapsulates the settings for configuring the PageRank calculator
// service.
type Config struct {
	// An API for interating links and edges from the link graph.
	GraphAPI GraphAPI

	// An API for updating the PageRank score for indexed documents.
	IndexAPI IndexAPI

	// An API for detecting the partition assignments for this service.
	PartitionDetector partition.Detector

	// A clock instance for generating time-related events. If not specified,
	// the default wall-clock will be used instead.
	Clock clock.Clock

	// The number of workers to spin up for computing PageRank scores. If
	// not specified, a default value of 1 will be used instead.
	ComputeWorkers int

	// The time between subsequent crawler passes.
	UpdateInterval time.Duration

	// The logger to use. If not defined an output-discarding logger will
	// be used instead.
	Logger *logrus.Entry
}

func (cfg *Config) validate() error {
	var err error
	if cfg.GraphAPI == nil {
		err = multierror.Append(err, xerrors.Errorf("graph API has not been provided"))
	}
	if cfg.IndexAPI == nil {
		err = multierror.Append(err, xerrors.Errorf("index API has not been provided"))
	}
	if cfg.PartitionDetector == nil {
		err = multierror.Append(err, xerrors.Errorf("partition detector has not been provided"))
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.WallClock
	}
	if cfg.ComputeWorkers <= 0 {
		err = multierror.Append(err, xerrors.Errorf("invalid value for compute workers"))
	}
	if cfg.UpdateInterval == 0 {
		err = multierror.Append(err, xerrors.Errorf("invalid value for update interval"))
	}
	if cfg.Logger == nil {
		cfg.Logger = logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard})
	}
	return err
}

// Service implements the PageRank calculator component for the Links 'R' Us project.
type Service struct {
	cfg        Config
	calculator *pr.Calculator
}

// NewService creates a new PageRank calculator service instance with the specified config.
func NewService(cfg Config) (*Service, error) {
	if err := cfg.validate(); err != nil {
		return nil, xerrors.Errorf("pagerank service: config validation failed: %w", err)
	}

	calculator, err := pr.NewCalculator(pr.Config{ComputeWorkers: cfg.ComputeWorkers})
	if err != nil {
		return nil, xerrors.Errorf("pagerank service: config validation failed: %w", err)
	}

	return &Service{
		cfg:        cfg,
		calculator: calculator,
	}, nil
}

// Name implements service.Service
func (svc *Service) Name() string { return "PageRank calculator" }

// Run implements service.Service
func (svc *Service) Run(ctx context.Context) error {
	svc.cfg.Logger.WithField("update_interval", svc.cfg.UpdateInterval.String()).Info("starting service")
	defer svc.cfg.Logger.Info("stopped service")

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-svc.cfg.Clock.After(svc.cfg.UpdateInterval):
			curPartition, _, err := svc.cfg.PartitionDetector.PartitionInfo()
			if err != nil {
				if xerrors.Is(err, partition.ErrNoPartitionDataAvailableYet) {
					svc.cfg.Logger.Warn("deferring PageRank update pass: partition data not yet available")
					continue
				}
				return err
			}

			if curPartition != 0 {
				svc.cfg.Logger.Info("service can only run on the leader of the application cluster")
				return nil
			}

			if err := svc.updateGraphScores(ctx); err != nil {
				return err
			}
		}
	}
}

func (svc *Service) updateGraphScores(ctx context.Context) error {
	svc.cfg.Logger.Info("starting PageRank update pass")
	startAt := svc.cfg.Clock.Now()

	maxUUID := uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	tick := startAt
	if err := svc.calculator.Graph().Reset(); err != nil {
		return err
	} else if err := svc.loadLinks(uuid.Nil, maxUUID, startAt); err != nil {
		return err
	} else if err := svc.loadEdges(uuid.Nil, maxUUID, startAt); err != nil {
		return err
	}
	graphPopulateTime := svc.cfg.Clock.Now().Sub(tick)

	tick = svc.cfg.Clock.Now()
	if err := svc.calculator.Executor().RunToCompletion(ctx); err != nil {
		return err
	}
	scoreCalculationTime := svc.cfg.Clock.Now().Sub(tick)

	tick = svc.cfg.Clock.Now()
	if err := svc.calculator.Scores(svc.persistScore); err != nil {
		return err
	}
	scorePersistTime := svc.cfg.Clock.Now().Sub(tick)

	svc.cfg.Logger.WithFields(logrus.Fields{
		"processed_links":        len(svc.calculator.Graph().Vertices()),
		"graph_populate_time":    graphPopulateTime.String(),
		"score_calculation_time": scoreCalculationTime.String(),
		"score_persist_time":     scorePersistTime.String(),
		"total_pass_time":        svc.cfg.Clock.Now().Sub(startAt).String(),
	}).Info("completed PageRank update pass")
	return nil
}

func (svc *Service) persistScore(vertexID string, score float64) error {
	linkID, err := uuid.Parse(vertexID)
	if err != nil {
		return err
	}

	return svc.cfg.IndexAPI.UpdateScore(linkID, score)
}

func (svc *Service) loadLinks(fromID, toID uuid.UUID, filter time.Time) error {
	linkIt, err := svc.cfg.GraphAPI.Links(fromID, toID, filter)
	if err != nil {
		return err
	}

	for linkIt.Next() {
		link := linkIt.Link()
		svc.calculator.AddVertex(link.ID.String())
	}
	if err = linkIt.Error(); err != nil {
		_ = linkIt.Close()
		return err
	}

	return linkIt.Close()
}

func (svc *Service) loadEdges(fromID, toID uuid.UUID, filter time.Time) error {
	edgeIt, err := svc.cfg.GraphAPI.Edges(fromID, toID, filter)
	if err != nil {
		return err
	}

	for edgeIt.Next() {
		edge := edgeIt.Edge()
		// As new edges may have been created since the links were loaded be
		// tolerant to UnknownEdgeSource errors.
		if err = svc.calculator.AddEdge(edge.Src.String(), edge.Dst.String()); err != nil && !xerrors.Is(err, bspgraph.ErrUnknownEdgeSource) {
			_ = edgeIt.Close()
			return err
		}
	}
	if err = edgeIt.Error(); err != nil {
		_ = edgeIt.Close()
		return err
	}
	return edgeIt.Close()
}
