package graph

import "golang.org/x/xerrors"

var (
	// ErrNotFound is returned when a link or edge lookup fails.
	ErrNotFound = xerrors.New("not found")

	// ErrUnknownEdgeLinks is returned when attempting to create an edge
	// with an invalid source and/or destination ID
	ErrUnknownEdgeLinks = xerrors.New("unknown source and/or destination for edge")
)
