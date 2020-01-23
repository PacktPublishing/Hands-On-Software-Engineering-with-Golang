package color

import (
	"context"
	"math/rand"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/bspgraph"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/bspgraph/message"
)

// Assigner implements the greedy Jones/Plassmann algorithm for coloring graphs.
type Assigner struct {
	g *bspgraph.Graph

	executorFactory bspgraph.ExecutorFactory
}

// NewColorAssigner returns a new color Assigner instance.
func NewColorAssigner(numWorkers int) (*Assigner, error) {
	g, err := bspgraph.NewGraph(bspgraph.GraphConfig{
		ComputeFn:      assignColorsToGraph,
		ComputeWorkers: numWorkers,
	})
	if err != nil {
		return nil, err
	}

	return &Assigner{
		g:               g,
		executorFactory: bspgraph.NewExecutor,
	}, nil
}

// Close cleans up any allocated graph resources.
func (c *Assigner) Close() error {
	return c.g.Close()
}

// Graph returns the underlying bspgraph.Graph instance.
func (c *Assigner) Graph() *bspgraph.Graph {
	return c.g
}

// SetExecutorFactory configures the calculator to use the a custom executor
// factory when AssignColors is invoked.
func (c *Assigner) SetExecutorFactory(factory bspgraph.ExecutorFactory) {
	c.executorFactory = factory
}

// AddVertex inserts a new vertex with the specified ID into the graph.
func (c *Assigner) AddVertex(id string) {
	c.AddPreColoredVertex(id, 0)
}

// AddPreColoredVertex inserts a new vertex with a pre-assigned color.
func (c *Assigner) AddPreColoredVertex(id string, color int) {
	c.g.AddVertex(id, &vertexState{color: color})
}

// AddUndirectedEdge creates an un-directed edge from srcID to dstID.
func (c *Assigner) AddUndirectedEdge(srcID, dstID string) error {
	if err := c.g.AddEdge(srcID, dstID, nil); err != nil {
		return err
	}
	return c.g.AddEdge(dstID, srcID, nil)
}

// AssignColors executes the Jones/Plassmann algorithm on the graph and invokes
// the user-defined visitor function for each vertex in the graph.
func (c *Assigner) AssignColors(ctx context.Context, visitor func(vertexID string, color int)) (int, error) {
	exec := c.executorFactory(c.g, bspgraph.ExecutorCallbacks{
		PostStepKeepRunning: func(_ context.Context, _ *bspgraph.Graph, activeInStep int) (bool, error) {
			// Stop when all vertices have been colored.
			return activeInStep != 0, nil
		},
	})
	if err := exec.RunToCompletion(ctx); err != nil {
		return 0, err
	}

	var numColors int
	for vertID, v := range c.g.Vertices() {
		state := v.Value().(*vertexState)
		if state.color > numColors {
			numColors = state.color
		}
		visitor(vertID, state.color)
	}
	return numColors, nil
}

// VertexStateMessage is used to advertise the state of a vertex to its neighbors.
type VertexStateMessage struct {
	ID    string
	Token int
	Color int
}

// Type returns the type of this message.
func (m *VertexStateMessage) Type() string { return "VertexStateMessage" }

type vertexState struct {
	token      int
	color      int
	usedColors map[int]bool
}

func (s *vertexState) asMessage(id string) *VertexStateMessage {
	return &VertexStateMessage{
		ID:    id,
		Token: s.token,
		Color: s.color,
	}
}

func assignColorsToGraph(g *bspgraph.Graph, v *bspgraph.Vertex, msgIt message.Iterator) error {
	v.Freeze()
	state := v.Value().(*vertexState)

	// Initialization. If this is an unconnected vertex without a color
	// assign the first possible color.
	if g.Superstep() == 0 {
		if state.color == 0 && len(v.Edges()) == 0 {
			state.color = 1
			return nil
		}
		state.token = rand.Int()
		state.usedColors = make(map[int]bool)
		return g.BroadcastToNeighbors(v, state.asMessage(v.ID()))
	}

	// Color already assigned; no extra work required
	if state.color != 0 {
		return nil
	}

	// Process neighbor updates and update edge color assignments. Also,
	// figure out if we have the highest token number from un-colored
	// neighbors so we get to pick a color next.
	//
	// If our token is also assigned to a neighor (highly unlikely) compare
	// the vertex IDs to break the thie.
	pickNextColor := true
	myID := v.ID()
	for msgIt.Next() {
		m := msgIt.Message().(*VertexStateMessage)
		if m.Color != 0 {
			state.usedColors[m.Color] = true
		} else if state.token < m.Token || (state.token == m.Token && myID < m.ID) {
			pickNextColor = false
		}
	}

	// If it's not yet our turn to pick a color keep broadcasting our token
	// to each one of our neighbors.
	if !pickNextColor {
		return g.BroadcastToNeighbors(v, state.asMessage(v.ID()))
	}

	// Find the minimum unused color, assign it to us and announce it to neighbors
	for nextColor := 1; ; nextColor++ {
		if state.usedColors[nextColor] {
			continue
		}

		state.color = nextColor
		return g.BroadcastToNeighbors(v, state.asMessage(myID))
	}
}
