package shortestpath

import (
	"context"
	"math"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/bspgraph"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/bspgraph/message"
	"golang.org/x/xerrors"
)

// Calculator implements a shortest path calculator from a single vertex to
// all other vertices in a connected graph.
type Calculator struct {
	g     *bspgraph.Graph
	srcID string

	executorFactory bspgraph.ExecutorFactory
}

// NewCalculator returns a new shortest path calculator instance.
func NewCalculator(numWorkers int) (*Calculator, error) {
	c := &Calculator{
		executorFactory: bspgraph.NewExecutor,
	}

	var err error
	if c.g, err = bspgraph.NewGraph(bspgraph.GraphConfig{
		ComputeFn:      c.findShortestPath,
		ComputeWorkers: numWorkers,
	}); err != nil {
		return nil, err
	}

	return c, nil
}

// Close cleans up any allocated graph resources.
func (c *Calculator) Close() error {
	return c.g.Close()
}

// SetExecutorFactory configures the calculator to use the a custom executor
// factory when CalculateShortestPaths is invoked.
func (c *Calculator) SetExecutorFactory(factory bspgraph.ExecutorFactory) {
	c.executorFactory = factory
}

// AddVertex inserts a new vertex with the specified ID into the graph.
func (c *Calculator) AddVertex(id string) {
	c.g.AddVertex(id, nil)
}

// AddEdge creates a directed edge from srcID to dstID with the specified cost.
// An error will be returned if a negative cost value is specified.
func (c *Calculator) AddEdge(srcID, dstID string, cost int) error {
	if cost < 0 {
		return xerrors.Errorf("negative edge costs not supported")
	}
	return c.g.AddEdge(srcID, dstID, cost)
}

// CalculateShortestPaths finds the shortest path costs from srcID to all other
// vertices in the graph.
func (c *Calculator) CalculateShortestPaths(ctx context.Context, srcID string) error {
	c.srcID = srcID
	exec := c.executorFactory(c.g, bspgraph.ExecutorCallbacks{
		PostStepKeepRunning: func(_ context.Context, _ *bspgraph.Graph, activeInStep int) (bool, error) {
			return activeInStep != 0, nil
		},
	})
	return exec.RunToCompletion(ctx)
}

// ShortestPathTo returns the shortest path from the source vertex to the
// specified destination together with its cost.
func (c *Calculator) ShortestPathTo(dstID string) ([]string, int, error) {
	vertMap := c.g.Vertices()
	v, exists := vertMap[dstID]
	if !exists {
		return nil, 0, xerrors.Errorf("unknown vertex with ID %q", dstID)
	}

	var (
		minDist = v.Value().(*pathState).minDist
		path    []string
	)

	for ; v.ID() != c.srcID; v = vertMap[v.Value().(*pathState).prevInPath] {
		path = append(path, v.ID())
	}
	path = append(path, c.srcID)

	// Reverse in place to get path from src->dst
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}
	return path, minDist, nil
}

// PathCostMessage is used to advertise the cost of a path through a vertex.
type PathCostMessage struct {
	// The ID of the vertex this cost announcement originates from.
	FromID string

	// The cost of the path from this vertex to the source vertex via FromID.
	Cost int
}

// Type returns the type of this message.
func (pc PathCostMessage) Type() string { return "cost" }

type pathState struct {
	minDist    int
	prevInPath string
}

func (c *Calculator) findShortestPath(g *bspgraph.Graph, v *bspgraph.Vertex, msgIt message.Iterator) error {
	if g.Superstep() == 0 {
		v.SetValue(&pathState{
			minDist: int(math.MaxInt64),
		})
	}

	minDist := int(math.MaxInt64)
	if v.ID() == c.srcID {
		minDist = 0
	}

	// Process cost messages from neighbors and update minDist if
	// we receive a better path announcement.
	var via string
	for msgIt.Next() {
		m := msgIt.Message().(*PathCostMessage)
		if m.Cost < minDist {
			minDist = m.Cost
			via = m.FromID
		}
	}

	// If a better path was found through this vertex, announce it
	// to all neighbors so they can update their own scores.
	st := v.Value().(*pathState)
	if minDist < st.minDist {
		st.minDist = minDist
		st.prevInPath = via
		for _, e := range v.Edges() {
			costMsg := &PathCostMessage{
				FromID: v.ID(),
				Cost:   minDist + e.Value().(int),
			}
			if err := g.SendMessage(e.DstID(), costMsg); err != nil {
				return err
			}
		}
	}

	// We are done unless we receive a better path announcement.
	v.Freeze()
	return nil
}
