package linkgraphapi

import (
	"context"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/graph"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/linkgraphapi/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/google/uuid"
)

var _ proto.LinkGraphServer = (*LinkGraphServer)(nil)

// LinkGraphServer provides a gRPC layer for accessing a link graph.
type LinkGraphServer struct {
	g graph.Graph
}

// NewLinkGraphServer returns a new server instance that uses the provided
// graph as its backing store.
func NewLinkGraphServer(g graph.Graph) *LinkGraphServer {
	return &LinkGraphServer{g: g}
}

// UpsertLink inserts or updates a link.
func (s *LinkGraphServer) UpsertLink(_ context.Context, req *proto.Link) (*proto.Link, error) {
	var (
		err  error
		link = graph.Link{
			ID:  uuidFromBytes(req.Uuid),
			URL: req.Url,
		}
	)

	if link.RetrievedAt, err = ptypes.Timestamp(req.RetrievedAt); err != nil {
		return nil, err
	}

	if err = s.g.UpsertLink(&link); err != nil {
		return nil, err
	}

	req.RetrievedAt = timeToProto(link.RetrievedAt)
	req.Url = link.URL
	req.Uuid = link.ID[:]
	return req, nil
}

// UpsertEdge inserts or updates an edge.
func (s *LinkGraphServer) UpsertEdge(_ context.Context, req *proto.Edge) (*proto.Edge, error) {
	edge := graph.Edge{
		ID:  uuidFromBytes(req.Uuid),
		Src: uuidFromBytes(req.SrcUuid),
		Dst: uuidFromBytes(req.DstUuid),
	}

	if err := s.g.UpsertEdge(&edge); err != nil {
		return nil, err
	}

	req.Uuid = edge.ID[:]
	req.SrcUuid = edge.Src[:]
	req.DstUuid = edge.Dst[:]
	req.UpdatedAt = timeToProto(edge.UpdatedAt)
	return req, nil
}

// Links streams the set of links whose IDs belong to the specified partition
// range and were accessed before the specified timestamp.
func (s *LinkGraphServer) Links(idRange *proto.Range, w proto.LinkGraph_LinksServer) error {
	accessedBefore, err := ptypes.Timestamp(idRange.Filter)
	if err != nil && idRange.Filter != nil {
		return err
	}

	fromID, err := uuid.FromBytes(idRange.FromUuid)
	if err != nil {
		return err
	}
	toID, err := uuid.FromBytes(idRange.ToUuid)
	if err != nil {
		return err
	}

	it, err := s.g.Links(fromID, toID, accessedBefore)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()

	for it.Next() {
		link := it.Link()
		msg := &proto.Link{
			Uuid:        link.ID[:],
			Url:         link.URL,
			RetrievedAt: timeToProto(link.RetrievedAt),
		}
		if err := w.Send(msg); err != nil {
			_ = it.Close()
			return err
		}
	}

	if err := it.Error(); err != nil {
		return err
	}

	return it.Close()
}

// Edges streams the set of edges whose IDs belong to the specified partition
// range and were updated before the specified timestamp.
func (s *LinkGraphServer) Edges(idRange *proto.Range, w proto.LinkGraph_EdgesServer) error {
	updatedBefore, err := ptypes.Timestamp(idRange.Filter)
	if err != nil && idRange.Filter != nil {
		return err
	}

	fromID, err := uuid.FromBytes(idRange.FromUuid)
	if err != nil {
		return err
	}
	toID, err := uuid.FromBytes(idRange.ToUuid)
	if err != nil {
		return err
	}

	it, err := s.g.Edges(fromID, toID, updatedBefore)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()

	for it.Next() {
		edge := it.Edge()
		msg := &proto.Edge{
			Uuid:      edge.ID[:],
			SrcUuid:   edge.Src[:],
			DstUuid:   edge.Dst[:],
			UpdatedAt: timeToProto(edge.UpdatedAt),
		}
		if err := w.Send(msg); err != nil {
			_ = it.Close()
			return err
		}
	}

	if err := it.Error(); err != nil {
		return err
	}

	return it.Close()
}

// RemoveStaleEdges removes any edge that originates from the specified
// link ID and was updated before the specified timestamp.
func (s *LinkGraphServer) RemoveStaleEdges(_ context.Context, req *proto.RemoveStaleEdgesQuery) (*empty.Empty, error) {
	updatedBefore, err := ptypes.Timestamp(req.UpdatedBefore)
	if err != nil {
		return nil, err
	}

	err = s.g.RemoveStaleEdges(
		uuidFromBytes(req.FromUuid),
		updatedBefore,
	)

	return new(empty.Empty), err
}

func uuidFromBytes(b []byte) uuid.UUID {
	if len(b) != 16 {
		return uuid.Nil
	}

	var dst uuid.UUID
	copy(dst[:], b)
	return dst
}

func timeToProto(t time.Time) *timestamp.Timestamp {
	ts, _ := ptypes.TimestampProto(t)
	return ts
}
