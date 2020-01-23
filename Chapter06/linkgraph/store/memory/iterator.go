package memory

import "github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/graph"

// linkIterator is a graph.LinkIterator implementation for the in-memory graph.
type linkIterator struct {
	s *InMemoryGraph

	links    []*graph.Link
	curIndex int
}

// Next implements graph.LinkIterator.
func (i *linkIterator) Next() bool {
	if i.curIndex >= len(i.links) {
		return false
	}
	i.curIndex++
	return true
}

// Error implements graph.LinkIterator.
func (i *linkIterator) Error() error {
	return nil
}

// Close implements graph.LinkIterator.
func (i *linkIterator) Close() error {
	return nil
}

// Link implements graph.LinkIterator.
func (i *linkIterator) Link() *graph.Link {
	// The link pointer contents may be overwritten by a graph update; to
	// avoid data-races we acquire the read lock first and clone the link
	i.s.mu.RLock()
	link := new(graph.Link)
	*link = *i.links[i.curIndex-1]
	i.s.mu.RUnlock()
	return link
}

// edgeIterator is a graph.EdgeIterator implementation for the in-memory graph.
type edgeIterator struct {
	s *InMemoryGraph

	edges    []*graph.Edge
	curIndex int
}

// Next implements graph.LinkIterator.
func (i *edgeIterator) Next() bool {
	if i.curIndex >= len(i.edges) {
		return false
	}
	i.curIndex++
	return true
}

// Error implements graph.LinkIterator.
func (i *edgeIterator) Error() error {
	return nil
}

// Close implements graph.LinkIterator.
func (i *edgeIterator) Close() error {
	return nil
}

// Link implements graph.LinkIterator.
func (i *edgeIterator) Edge() *graph.Edge {
	// The edge pointer contents may be overwritten by a graph update; to
	// avoid data-races we acquire the read lock first and clone the edge
	i.s.mu.RLock()
	edge := new(graph.Edge)
	*edge = *i.edges[i.curIndex-1]
	i.s.mu.RUnlock()
	return edge
}
