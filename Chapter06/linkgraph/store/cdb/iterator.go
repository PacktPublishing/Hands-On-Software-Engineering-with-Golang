package cdb

import (
	"database/sql"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/graph"
	"golang.org/x/xerrors"
)

// linkIterator is a graph.LinkIterator implementation for the cdb graph.
type linkIterator struct {
	rows        *sql.Rows
	lastErr     error
	latchedLink *graph.Link
}

// Next implements graph.LinkIterator.
func (i *linkIterator) Next() bool {
	if i.lastErr != nil || !i.rows.Next() {
		return false
	}

	l := new(graph.Link)
	i.lastErr = i.rows.Scan(&l.ID, &l.URL, &l.RetrievedAt)
	if i.lastErr != nil {
		return false
	}
	l.RetrievedAt = l.RetrievedAt.UTC()

	i.latchedLink = l
	return true
}

// Error implements graph.LinkIterator.
func (i *linkIterator) Error() error {
	return i.lastErr
}

// Close implements graph.LinkIterator.
func (i *linkIterator) Close() error {
	err := i.rows.Close()
	if err != nil {
		return xerrors.Errorf("link iterator: %w", err)
	}
	return nil
}

// Link implements graph.LinkIterator.
func (i *linkIterator) Link() *graph.Link {
	return i.latchedLink
}

// edgeIterator is a graph.EdgeIterator implementation for the cdb graph.
type edgeIterator struct {
	rows        *sql.Rows
	lastErr     error
	latchedEdge *graph.Edge
}

// Next implements graph.EdgeIterator.
func (i *edgeIterator) Next() bool {
	if i.lastErr != nil || !i.rows.Next() {
		return false
	}

	e := new(graph.Edge)
	i.lastErr = i.rows.Scan(&e.ID, &e.Src, &e.Dst, &e.UpdatedAt)
	if i.lastErr != nil {
		return false
	}
	e.UpdatedAt = e.UpdatedAt.UTC()

	i.latchedEdge = e
	return true
}

// Error implements graph.EdgeIterator.
func (i *edgeIterator) Error() error {
	return i.lastErr
}

// Close implements graph.EdgeIterator.
func (i *edgeIterator) Close() error {
	err := i.rows.Close()
	if err != nil {
		return xerrors.Errorf("edge iterator: %w", err)
	}
	return nil
}

// Edge implements graph.EdgeIterator.
func (i *edgeIterator) Edge() *graph.Edge {
	return i.latchedEdge
}
