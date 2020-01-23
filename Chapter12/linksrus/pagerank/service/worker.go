package service

import (
	"context"
	"io/ioutil"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/graph"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/bspgraph"
	pr "github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/pagerank"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/dbspgraph"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/dbspgraph/job"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

// GraphAPI defines as set of API methods for fetching the links and edges from
// the link graph.
type GraphAPI interface {
	Links(fromID, toID uuid.UUID, accessedBefore time.Time) (graph.LinkIterator, error)
	Edges(fromID, toID uuid.UUID, updatedBefore time.Time) (graph.EdgeIterator, error)
}

// IndexAPI defines a set of API methods for updating PageRank scores for
// indexed documents.
type IndexAPI interface {
	UpdateScore(linkID uuid.UUID, score float64) error
}

// WorkerConfig encapsulates the settings for configuring a worker node for the
// PageRank calculator service.
type WorkerConfig struct {
	// The master node endpoint.
	MasterEndpoint string

	// The timeout for establishing a connection to the master node.
	MasterDialTimeout time.Duration

	// An API for interating links and edges from the link graph.
	GraphAPI GraphAPI

	// An API for updating the PageRank score for indexed documents.
	IndexAPI IndexAPI

	// The number of workers to spin up for computing PageRank scores. If
	// not specified, a default value of 1 will be used instead.
	ComputeWorkers int

	// The logger to use. If not defined an output-discarding logger will
	// be used instead.
	Logger *logrus.Entry
}

func (cfg *WorkerConfig) validate() error {
	var err error
	if cfg.MasterEndpoint == "" {
		err = multierror.Append(err, xerrors.Errorf("invalid value for master endpoint"))
	}
	if cfg.GraphAPI == nil {
		err = multierror.Append(err, xerrors.Errorf("graph API has not been provided"))
	}
	if cfg.IndexAPI == nil {
		err = multierror.Append(err, xerrors.Errorf("index API has not been provided"))
	}
	if cfg.ComputeWorkers <= 0 {
		err = multierror.Append(err, xerrors.Errorf("invalid value for compute workers"))
	}
	if cfg.Logger == nil {
		cfg.Logger = logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard})
	}
	return err
}

// WorkerNode implements a master node for calculating PageRank scores in a
// distributed fashion.
type WorkerNode struct {
	cfg          WorkerConfig
	calculator   *pr.Calculator
	workerFacade *dbspgraph.Worker

	// Stats
	jobStartedAt              time.Time
	graphPopulateTime         time.Duration
	scoreCalculationStartedAt time.Time
}

// NewWorkerNode creates a new worker node for the PageRank calculator service.
func NewWorkerNode(cfg WorkerConfig) (*WorkerNode, error) {
	if err := cfg.validate(); err != nil {
		return nil, xerrors.Errorf("pagerank service: config validation failed: %w", err)
	}
	calculator, err := pr.NewCalculator(pr.Config{ComputeWorkers: cfg.ComputeWorkers})
	if err != nil {
		return nil, xerrors.Errorf("pagerank service: config validation failed: %w", err)
	}

	workerNode := &WorkerNode{
		cfg:        cfg,
		calculator: calculator,
	}

	if workerNode.workerFacade, err = dbspgraph.NewWorker(dbspgraph.WorkerConfig{
		JobRunner:  workerNode,
		Serializer: serializer{},
		Logger:     cfg.Logger,
	}); err != nil {
		_ = calculator.Close()
		return nil, err
	}

	if err = workerNode.workerFacade.Dial(cfg.MasterEndpoint, cfg.MasterDialTimeout); err != nil {
		_ = calculator.Close()
		return nil, err
	}

	return workerNode, nil
}

// Run implements the main loop of a worker that executes the PageRank
// algorithm on a subset of the link graph. The worker waits for the master
// node to publish a new PageRank job and then begins the algorithm execution
// constrained to the assigned partition range.
//
// Run blocks until the provided context expires.
func (n *WorkerNode) Run(ctx context.Context) error {
	n.cfg.Logger.Info("starting service")
	defer func() {
		_ = n.workerFacade.Close()
		_ = n.calculator.Close()
		n.cfg.Logger.Info("stopped service")
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err := n.workerFacade.RunJob(ctx); err != nil {
			n.cfg.Logger.WithField("err", err).Error("PageRank update job failed")
		}
	}
}

// StartJob implements job.Runner. It initializes the underlying bspgraph.Graph
// instance and invokes the provided ExecutorFactory to create an executor for
// the graph supersteps.
func (n *WorkerNode) StartJob(jobDetails job.Details, execFactory bspgraph.ExecutorFactory) (*bspgraph.Executor, error) {
	n.jobStartedAt = time.Now()
	if err := n.calculator.Graph().Reset(); err != nil {
		return nil, err
	} else if err := n.loadLinks(jobDetails.PartitionFromID, jobDetails.PartitionToID, jobDetails.CreatedAt); err != nil {
		return nil, err
	} else if err := n.loadEdges(jobDetails.PartitionFromID, jobDetails.PartitionToID, jobDetails.CreatedAt); err != nil {
		return nil, err
	}
	n.graphPopulateTime = time.Since(n.jobStartedAt)

	n.scoreCalculationStartedAt = time.Now()
	n.calculator.SetExecutorFactory(execFactory)
	return n.calculator.Executor(), nil
}

func (n *WorkerNode) loadLinks(fromID, toID uuid.UUID, filter time.Time) error {
	linkIt, err := n.cfg.GraphAPI.Links(fromID, toID, filter)
	if err != nil {
		return err
	}

	for linkIt.Next() {
		link := linkIt.Link()
		n.calculator.AddVertex(link.ID.String())
	}
	if err = linkIt.Error(); err != nil {
		_ = linkIt.Close()
		return err
	}

	return linkIt.Close()
}

func (n *WorkerNode) loadEdges(fromID, toID uuid.UUID, filter time.Time) error {
	edgeIt, err := n.cfg.GraphAPI.Edges(fromID, toID, filter)
	if err != nil {
		return err
	}

	for edgeIt.Next() {
		edge := edgeIt.Edge()
		// As new edges may have been created since the links were loaded be
		// tolerant to UnknownEdgeSource errors.
		if err = n.calculator.AddEdge(edge.Src.String(), edge.Dst.String()); err != nil && !xerrors.Is(err, bspgraph.ErrUnknownEdgeSource) {
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

// CompleteJob implements job.Runner. It persists the locally computed PageRank
// scores after a successful execution of a distributed PageRank run.
func (n *WorkerNode) CompleteJob(_ job.Details) error {
	scoreCalculationTime := time.Since(n.scoreCalculationStartedAt)

	tick := time.Now()
	if err := n.calculator.Scores(n.persistScore); err != nil {
		return err
	}
	scorePersistTime := time.Since(tick)

	n.cfg.Logger.WithFields(logrus.Fields{
		"local_link_count":       len(n.calculator.Graph().Vertices()),
		"total_link_count":       n.calculator.Graph().Aggregator("page_count").Get(),
		"graph_populate_time":    n.graphPopulateTime.String(),
		"score_calculation_time": scoreCalculationTime.String(),
		"score_persist_time":     scorePersistTime.String(),
		"total_pass_time":        time.Since(n.jobStartedAt).String(),
	}).Info("completed PageRank update pass")
	return nil
}

func (n *WorkerNode) persistScore(vertexID string, score float64) error {
	linkID, err := uuid.Parse(vertexID)
	if err != nil {
		return err
	}

	return n.cfg.IndexAPI.UpdateScore(linkID, score)
}

// AbortJob implements job.Runner.
func (n *WorkerNode) AbortJob(_ job.Details) {}
