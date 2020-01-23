package pagerank

import (
	"context"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/bspgraph"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/bspgraph/aggregator"
	"golang.org/x/xerrors"
)

// Calculator executes the iterative version of the PageRank algorithm
// on a graph until the desired level of convergence is reached.
type Calculator struct {
	g   *bspgraph.Graph
	cfg Config

	executorFactory bspgraph.ExecutorFactory
}

// NewCalculator returns a new Calculator instance using the provided config
// options.
func NewCalculator(cfg Config) (*Calculator, error) {
	if err := cfg.validate(); err != nil {
		return nil, xerrors.Errorf("PageRank calculator config validation failed: %w", err)
	}

	g, err := bspgraph.NewGraph(bspgraph.GraphConfig{
		ComputeWorkers: cfg.ComputeWorkers,
		ComputeFn:      makeComputeFunc(cfg.DampingFactor),
	})
	if err != nil {
		return nil, err
	}

	return &Calculator{
		cfg:             cfg,
		g:               g,
		executorFactory: bspgraph.NewExecutor,
	}, nil
}

// Close releases any resources allocated by this PageRank calculator instance.
func (c *Calculator) Close() error {
	return c.g.Close()
}

// SetExecutorFactory configures the calculator to use the a custom executor
// factory when the Executor method is invoked.
func (c *Calculator) SetExecutorFactory(factory bspgraph.ExecutorFactory) {
	c.executorFactory = factory
}

// AddVertex inserts a new vertex to the graph with the given id.
func (c *Calculator) AddVertex(id string) {
	c.g.AddVertex(id, 0.0)
}

// AddEdge inserts a directed edge from src to dst. If both src and dst refer
// to the same vertex then this is a no-op.
func (c *Calculator) AddEdge(src, dst string) error {
	// Don't allow self-links
	if src == dst {
		return nil
	}
	return c.g.AddEdge(src, dst, nil)
}

// Graph returns the underlying bspgraph.Graph instance.
func (c *Calculator) Graph() *bspgraph.Graph {
	return c.g
}

// Executor creates and return a bspgraph.Executor for running the PageRank
// algorithm once the graph layout has been properly set up.
func (c *Calculator) Executor() *bspgraph.Executor {
	c.registerAggregators()
	cb := bspgraph.ExecutorCallbacks{
		PreStep: func(_ context.Context, g *bspgraph.Graph) error {
			// Reset sum of abs differences aggregator and residual
			// aggregator for next step.
			g.Aggregator("SAD").Set(0.0)
			g.Aggregator(residualOutputAccName(g.Superstep())).Set(0.0)
			return nil
		},
		PostStepKeepRunning: func(_ context.Context, g *bspgraph.Graph, _ int) (bool, error) {
			// Supersteps 0 and 1 are part of the algorithm initialization;
			// the predicate should only be evaluated for supersteps > 1
			sad := c.g.Aggregator("SAD").Get().(float64)
			return !(g.Superstep() > 1 && sad < c.cfg.MinSADForConvergence), nil
		},
	}

	return c.executorFactory(c.g, cb)
}

// registerAggregators creates and registers the aggregator instances that we
// need to run the PageRank calculation algorithm.
func (c *Calculator) registerAggregators() {
	c.g.RegisterAggregator("page_count", new(aggregator.IntAccumulator))
	c.g.RegisterAggregator("residual_0", new(aggregator.Float64Accumulator))
	c.g.RegisterAggregator("residual_1", new(aggregator.Float64Accumulator))
	c.g.RegisterAggregator("SAD", new(aggregator.Float64Accumulator))
}

// Scores invokes the provided visitor function for each vertex in the graph.
func (c *Calculator) Scores(visitFn func(id string, score float64) error) error {
	for id, v := range c.g.Vertices() {
		if err := visitFn(id, v.Value().(float64)); err != nil {
			return err
		}
	}

	return nil
}

// residualOutputAccName returns the name of the accumulator where the
// residual PageRank scores for the specified superstep are to be written to.
func residualOutputAccName(superstep int) string {
	if superstep%2 == 0 {
		return "residual_0"
	}
	return "residual_1"
}

// residualInputAccName returns the name of the accumulator where the
// residual PageRank scores for the specified superstep are to be read from.
func residualInputAccName(superstep int) string {
	if (superstep+1)%2 == 0 {
		return "residual_0"
	}
	return "residual_1"
}
