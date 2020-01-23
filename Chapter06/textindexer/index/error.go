package index

import "golang.org/x/xerrors"

var (
	// ErrNotFound is returned by the indexer when attempting to look up
	// a document that does not exist.
	ErrNotFound = xerrors.New("not found")

	// ErrMissingLinkID is returned when attempting to index a document
	// that does not specify a valid link ID.
	ErrMissingLinkID = xerrors.New("document does not provide a valid linkID")
)
