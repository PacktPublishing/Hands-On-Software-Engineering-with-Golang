package graph

import (
	"time"

	"github.com/google/uuid"
)

// Iterator is implemented by graph objects that can be iterated.
type Iterator interface {
	// Next advances the iterator. If no more items are available or an
	// error occurs, calls to Next() return false.
	Next() bool

	// Error returns the last error encountered by the iterator.
	Error() error

	// Close releases any resources associated with an iterator.
	Close() error
}

// LinkIterator is implemented by objects that can iterate the graph links.
type LinkIterator interface {
	Iterator

	// Link returns the currently fetched link object.
	Link() *Link
}

// EdgeIterator is implemented by objects that can iterate the graph edges.
type EdgeIterator interface {
	Iterator

	// Edge returns the currently fetched edge objects.
	Edge() *Edge
}

// Link encapsulates all information about a link discovered by the Links 'R'
// Us crawler.
type Link struct {
	// A unique identifier for the link.
	ID uuid.UUID

	// The link target.
	URL string

	// The timestamp when the link was last retrieved.
	RetrievedAt time.Time
}

// Edge describes a graph edge that originates from Src and terminates
// at Dst.
type Edge struct {
	// A unique identifier for the edge.
	ID uuid.UUID

	// The origin link.
	Src uuid.UUID

	// The destination link.
	Dst uuid.UUID

	// The timestamp when the link was last updated.
	UpdatedAt time.Time
}

// Graph is implemented by objects that can mutate or query a link graph.
type Graph interface {
	// UpsertLink creates a new link or updates an existing link.
	UpsertLink(link *Link) error

	// FindLink looks up a link by its ID.
	FindLink(id uuid.UUID) (*Link, error)

	// Links returns an iterator for the set of links whose IDs belong to the
	// [fromID, toID) range and were retrieved before the provided timestamp.
	Links(fromID, toID uuid.UUID, retrievedBefore time.Time) (LinkIterator, error)

	// UpsertEdge creates a new edge or updates an existing edge.
	UpsertEdge(edge *Edge) error

	// Edges returns an iterator for the set of edges whose source vertex IDs
	// belong to the [fromID, toID) range and were updated before the provided
	// timestamp.
	Edges(fromID, toID uuid.UUID, updatedBefore time.Time) (EdgeIterator, error)

	// RemoveStaleEdges removes any edge that originates from the specified
	// link ID and was updated before the specified timestamp.
	RemoveStaleEdges(fromID uuid.UUID, updatedBefore time.Time) error
}
