package linkgraphapi

import (
	"context"
	"io"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/graph"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/linkgraphapi/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
)

//go:generate mockgen -package mocks -destination mocks/mock.go github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/linkgraphapi/proto LinkGraphClient,LinkGraph_LinksClient,LinkGraph_EdgesClient

// LinkGraphClient provides an API compatible with the graph.Graph interface
// for accessing graph instances exposed by a remote gRPC server.
type LinkGraphClient struct {
	ctx context.Context
	cli proto.LinkGraphClient
}

// NewLinkGraphClient returns a new client instance that implements a subset
// of the graph.Graph interface by delegating methods to a graph instance
// exposed by a remote gRPC sever.
func NewLinkGraphClient(ctx context.Context, rpcClient proto.LinkGraphClient) *LinkGraphClient {
	return &LinkGraphClient{ctx: ctx, cli: rpcClient}
}

// UpsertLink creates a new link or updates an existing link.
func (c *LinkGraphClient) UpsertLink(link *graph.Link) error {
	req := &proto.Link{
		Uuid:        link.ID[:],
		Url:         link.URL,
		RetrievedAt: timeToProto(link.RetrievedAt),
	}
	res, err := c.cli.UpsertLink(c.ctx, req)
	if err != nil {
		return err
	}

	link.ID = uuidFromBytes(res.Uuid)
	link.URL = res.Url
	if link.RetrievedAt, err = ptypes.Timestamp(res.RetrievedAt); err != nil {
		return err
	}

	return nil
}

// UpsertEdge creates a new edge or updates an existing edge.
func (c *LinkGraphClient) UpsertEdge(edge *graph.Edge) error {
	req := &proto.Edge{
		Uuid:    edge.ID[:],
		SrcUuid: edge.Src[:],
		DstUuid: edge.Dst[:],
	}
	res, err := c.cli.UpsertEdge(c.ctx, req)
	if err != nil {
		return err
	}

	edge.ID = uuidFromBytes(res.Uuid)
	if edge.UpdatedAt, err = ptypes.Timestamp(res.UpdatedAt); err != nil {
		return err
	}

	return nil
}

// Links returns an iterator for the set of links whose IDs belong to the
// [fromID, toID) range and were last accessed before the provided value.
func (c *LinkGraphClient) Links(fromID, toID uuid.UUID, accessedBefore time.Time) (graph.LinkIterator, error) {
	filter, err := ptypes.TimestampProto(accessedBefore)
	if err != nil {
		return nil, err
	}

	req := &proto.Range{
		FromUuid: fromID[:],
		ToUuid:   toID[:],
		Filter:   filter,
	}

	ctx, cancelFn := context.WithCancel(c.ctx)
	stream, err := c.cli.Links(ctx, req)
	if err != nil {
		cancelFn()
		return nil, err
	}

	return &linkIterator{stream: stream, cancelFn: cancelFn}, nil
}

// Edges returns an iterator for the set of edges whose source vertex IDs
// belong to the [fromID, toID) range and were last updated before the provided
// value.
func (c *LinkGraphClient) Edges(fromID, toID uuid.UUID, updatedBefore time.Time) (graph.EdgeIterator, error) {
	filter, err := ptypes.TimestampProto(updatedBefore)
	if err != nil {
		return nil, err
	}

	req := &proto.Range{
		FromUuid: fromID[:],
		ToUuid:   toID[:],
		Filter:   filter,
	}

	ctx, cancelFn := context.WithCancel(c.ctx)
	stream, err := c.cli.Edges(ctx, req)
	if err != nil {
		cancelFn()
		return nil, err
	}

	return &edgeIterator{stream: stream, cancelFn: cancelFn}, nil
}

// RemoveStaleEdges removes any edge that originates from the specified link ID
// and was updated before the specified timestamp.
func (c *LinkGraphClient) RemoveStaleEdges(from uuid.UUID, updatedBefore time.Time) error {
	req := &proto.RemoveStaleEdgesQuery{
		FromUuid:      from[:],
		UpdatedBefore: timeToProto(updatedBefore),
	}

	_, err := c.cli.RemoveStaleEdges(c.ctx, req)
	return err
}

type linkIterator struct {
	stream  proto.LinkGraph_LinksClient
	next    *graph.Link
	lastErr error

	// A function to cancel the context used to perform the streaming RPC. It
	// allows us to abort server-streaming calls from the client side.
	cancelFn func()
}

// Next advances the iterator. If no more items are available or an
// error occurs, calls to Next() return false.
func (it *linkIterator) Next() bool {
	res, err := it.stream.Recv()
	if err != nil {
		if err != io.EOF {
			it.lastErr = err
		}
		it.cancelFn()
		return false
	}

	lastAccessed, err := ptypes.Timestamp(res.RetrievedAt)
	if err != nil {
		it.lastErr = err
		it.cancelFn()
		return false
	}

	it.next = &graph.Link{
		ID:          uuidFromBytes(res.Uuid),
		URL:         res.Url,
		RetrievedAt: lastAccessed,
	}
	return true
}

// Error returns the last error encountered by the iterator.
func (it *linkIterator) Error() error { return it.lastErr }

// Link returns the currently fetched link object.
func (it *linkIterator) Link() *graph.Link { return it.next }

// Close releases any resources associated with an iterator.
func (it *linkIterator) Close() error {
	it.cancelFn()
	return nil
}

type edgeIterator struct {
	stream  proto.LinkGraph_EdgesClient
	next    *graph.Edge
	lastErr error

	// A function to cancel the context used to perform the streaming RPC. It
	// allows us to abort server-streaming calls from the client side.
	cancelFn func()
}

// Next advances the iterator. If no more items are available or an
// error occurs, calls to Next() return false.
func (it *edgeIterator) Next() bool {
	res, err := it.stream.Recv()
	if err != nil {
		if err != io.EOF {
			it.lastErr = err
		}
		it.cancelFn()
		return false
	}

	updatedAt, err := ptypes.Timestamp(res.UpdatedAt)
	if err != nil {
		it.lastErr = err
		it.cancelFn()
		return false
	}

	it.next = &graph.Edge{
		ID:        uuidFromBytes(res.Uuid),
		Src:       uuidFromBytes(res.SrcUuid),
		Dst:       uuidFromBytes(res.DstUuid),
		UpdatedAt: updatedAt,
	}
	return true
}

// Error returns the last error encountered by the iterator.
func (it *edgeIterator) Error() error { return it.lastErr }

// Edge returns the currently fetched edge object.
func (it *edgeIterator) Edge() *graph.Edge { return it.next }

// Close releases any resources associated with an iterator.
func (it *edgeIterator) Close() error {
	it.cancelFn()
	return nil
}
