package textindexerapi

import (
	"context"
	"io"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/textindexer/index"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/textindexerapi/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
)

//go:generate mockgen -package mocks -destination mocks/mock.go github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/textindexerapi/proto TextIndexerClient,TextIndexer_SearchClient

// TextIndexerClient provides an API compatible with the index.Indexer interface
// for accessing a text indexer instances exposed by a remote gRPC server.
type TextIndexerClient struct {
	ctx context.Context
	cli proto.TextIndexerClient
}

// NewTextIndexerClient returns a new client instance that implements a subset
// of the index.Indexer interface by delegating methods to an indexer instance
// exposed by a remote gRPC sever.
func NewTextIndexerClient(ctx context.Context, rpcClient proto.TextIndexerClient) *TextIndexerClient {
	return &TextIndexerClient{ctx: ctx, cli: rpcClient}
}

// Index inserts a new document to the index or updates the index entry for and
// existing document.
func (c *TextIndexerClient) Index(doc *index.Document) error {
	req := &proto.Document{
		LinkId:  doc.LinkID[:],
		Url:     doc.URL,
		Title:   doc.Title,
		Content: doc.Content,
	}
	res, err := c.cli.Index(c.ctx, req)
	if err != nil {
		return err
	}

	t, err := ptypes.Timestamp(res.IndexedAt)
	if err != nil {
		return xerrors.Errorf("unable to decode indexedAt attribute of document %q: %w", doc.LinkID, err)
	}

	doc.IndexedAt = t
	return nil
}

// UpdateScore updates the PageRank score for a document with the specified
// link ID.
func (c *TextIndexerClient) UpdateScore(linkID uuid.UUID, score float64) error {
	req := &proto.UpdateScoreRequest{
		LinkId:        linkID[:],
		PageRankScore: score,
	}
	_, err := c.cli.UpdateScore(c.ctx, req)
	return err
}

// Search the index for a particular query and return back a result iterator.
func (c *TextIndexerClient) Search(query index.Query) (index.Iterator, error) {
	ctx, cancelFn := context.WithCancel(c.ctx)
	req := &proto.Query{
		Type:       proto.Query_Type(query.Type),
		Expression: query.Expression,
		Offset:     query.Offset,
	}
	stream, err := c.cli.Search(ctx, req)
	if err != nil {
		cancelFn()
		return nil, err
	}

	// Read result count
	res, err := stream.Recv()
	if err != nil {
		cancelFn()
		return nil, err
	} else if res.GetDoc() != nil {
		cancelFn()
		return nil, xerrors.Errorf("expected server to report the result count before sending any documents")
	}

	return &resultIterator{
		total:    res.GetDocCount(),
		stream:   stream,
		cancelFn: cancelFn,
	}, nil
}

type resultIterator struct {
	total   uint64
	stream  proto.TextIndexer_SearchClient
	next    *index.Document
	lastErr error

	// A function to cancel the context used to perform the streaming RPC. It
	// allows us to abort server-streaming calls from the client side.
	cancelFn func()
}

// Next advances the iterator. If no more items are available or an
// error occurs, calls to Next() return false.
func (it *resultIterator) Next() bool {
	res, err := it.stream.Recv()
	if err != nil {
		if err != io.EOF {
			it.lastErr = err
		}
		it.cancelFn()
		return false
	}

	resDoc := res.GetDoc()
	if resDoc == nil {
		it.cancelFn()
		it.lastErr = xerrors.Errorf("received nil document in search result list")
		return false
	}

	linkID := uuidFromBytes(resDoc.LinkId)

	t, err := ptypes.Timestamp(resDoc.IndexedAt)
	if err != nil {
		it.cancelFn()
		it.lastErr = xerrors.Errorf("unable to decode indexedAt attribute of document %q: %w", linkID, err)
		return false
	}

	it.next = &index.Document{
		LinkID:    linkID,
		URL:       resDoc.Url,
		Title:     resDoc.Title,
		Content:   resDoc.Content,
		IndexedAt: t,
	}
	return true
}

// Error returns the last error encountered by the iterator.
func (it *resultIterator) Error() error { return it.lastErr }

// Document returns the currently fetched edge object.
func (it *resultIterator) Document() *index.Document { return it.next }

// TotalCount returns the approximate number of search results.
func (it *resultIterator) TotalCount() uint64 { return it.total }

// Close releases any resources associated with an iterator.
func (it *resultIterator) Close() error {
	it.cancelFn()
	return nil
}
