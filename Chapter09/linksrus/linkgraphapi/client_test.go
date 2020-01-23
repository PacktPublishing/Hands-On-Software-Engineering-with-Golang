package linkgraphapi_test

import (
	"context"
	"io"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/graph"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/linkgraphapi"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/linkgraphapi/mocks"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/linkgraphapi/proto"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/uuid"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(ClientTestSuite))

type ClientTestSuite struct{}

func (s *ClientTestSuite) TestUpsertLink(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	rpcCli := mocks.NewMockLinkGraphClient(ctrl)

	now := time.Now().Truncate(time.Second).UTC()
	link := &graph.Link{
		URL:         "http://www.example.com",
		RetrievedAt: now,
	}

	assignedID := uuid.New()
	rpcCli.EXPECT().UpsertLink(
		gomock.AssignableToTypeOf(context.TODO()),
		&proto.Link{
			Uuid:        uuid.Nil[:],
			Url:         link.URL,
			RetrievedAt: mustEncodeTimestamp(c, link.RetrievedAt),
		},
	).Return(
		&proto.Link{
			Uuid:        assignedID[:],
			Url:         link.URL,
			RetrievedAt: mustEncodeTimestamp(c, link.RetrievedAt),
		},
		nil,
	)

	cli := linkgraphapi.NewLinkGraphClient(context.TODO(), rpcCli)
	err := cli.UpsertLink(link)
	c.Assert(err, gc.IsNil)
	c.Assert(link.ID, gc.DeepEquals, assignedID)
	c.Assert(link.RetrievedAt, gc.Equals, now)
}

func (s *ClientTestSuite) TestUpsertEdge(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	rpcCli := mocks.NewMockLinkGraphClient(ctrl)

	edge := &graph.Edge{
		Src: uuid.New(),
		Dst: uuid.New(),
	}

	assignedID := uuid.New()
	rpcCli.EXPECT().UpsertEdge(
		gomock.AssignableToTypeOf(context.TODO()),
		&proto.Edge{
			Uuid:    uuid.Nil[:],
			SrcUuid: edge.Src[:],
			DstUuid: edge.Dst[:],
		},
	).Return(
		&proto.Edge{
			Uuid:      assignedID[:],
			SrcUuid:   edge.Src[:],
			DstUuid:   edge.Dst[:],
			UpdatedAt: ptypes.TimestampNow(),
		},
		nil,
	)

	cli := linkgraphapi.NewLinkGraphClient(context.TODO(), rpcCli)
	err := cli.UpsertEdge(edge)
	c.Assert(err, gc.IsNil)
	c.Assert(edge.ID, gc.DeepEquals, assignedID)
}

func (s *ClientTestSuite) TestLinks(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	rpcCli := mocks.NewMockLinkGraphClient(ctrl)
	linkStream := mocks.NewMockLinkGraph_LinksClient(ctrl)

	ctxWithCancel, cancelFn := context.WithCancel(context.TODO())
	defer cancelFn()

	now := time.Now().Truncate(time.Second).UTC()
	rpcCli.EXPECT().Links(
		gomock.AssignableToTypeOf(ctxWithCancel),
		&proto.Range{FromUuid: minUUID[:], ToUuid: maxUUID[:], Filter: mustEncodeTimestamp(c, now)},
	).Return(linkStream, nil)

	uuid1 := uuid.New()
	uuid2 := uuid.New()
	lastAccessed := mustEncodeTimestamp(c, now)
	returns := [][]interface{}{
		{&proto.Link{Uuid: uuid1[:], Url: "http://example.com", RetrievedAt: lastAccessed}, nil},
		{&proto.Link{Uuid: uuid2[:], Url: "http://example.com", RetrievedAt: lastAccessed}, nil},
		{nil, io.EOF},
	}
	linkStream.EXPECT().Recv().DoAndReturn(
		func() (interface{}, interface{}) {
			next := returns[0]
			returns = returns[1:]
			return next[0], next[1]
		},
	).Times(len(returns))

	cli := linkgraphapi.NewLinkGraphClient(context.TODO(), rpcCli)
	it, err := cli.Links(minUUID, maxUUID, now)
	c.Assert(err, gc.IsNil)

	var linkCount int
	for it.Next() {
		linkCount++
		next := it.Link()
		if next.ID != uuid1 && next.ID != uuid2 {
			c.Fatalf("unexpected link with ID %q", next.ID)
		}
		c.Assert(next.URL, gc.Equals, "http://example.com")
		c.Assert(next.RetrievedAt, gc.Equals, now)
	}
	c.Assert(it.Error(), gc.IsNil)
	c.Assert(it.Close(), gc.IsNil)
	c.Assert(linkCount, gc.Equals, 2)
}

func (s *ClientTestSuite) TestEdges(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	rpcCli := mocks.NewMockLinkGraphClient(ctrl)
	edgeStream := mocks.NewMockLinkGraph_EdgesClient(ctrl)

	ctxWithCancel, cancelFn := context.WithCancel(context.TODO())
	defer cancelFn()

	now := time.Now().Truncate(time.Second).UTC()
	rpcCli.EXPECT().Edges(
		gomock.AssignableToTypeOf(ctxWithCancel),
		&proto.Range{FromUuid: minUUID[:], ToUuid: maxUUID[:], Filter: mustEncodeTimestamp(c, now)},
	).Return(edgeStream, nil)

	uuid1 := uuid.New()
	uuid2 := uuid.New()
	srcID := uuid.New()
	dstID := uuid.New()
	updatedAt := time.Now().UTC()

	returns := [][]interface{}{
		{&proto.Edge{Uuid: uuid1[:], SrcUuid: srcID[:], DstUuid: dstID[:], UpdatedAt: mustEncodeTimestamp(c, updatedAt)}, nil},
		{&proto.Edge{Uuid: uuid2[:], SrcUuid: srcID[:], DstUuid: dstID[:], UpdatedAt: mustEncodeTimestamp(c, updatedAt)}, nil},
		{nil, io.EOF},
	}
	edgeStream.EXPECT().Recv().DoAndReturn(
		func() (interface{}, interface{}) {
			next := returns[0]
			returns = returns[1:]
			return next[0], next[1]
		},
	).Times(len(returns))

	cli := linkgraphapi.NewLinkGraphClient(context.TODO(), rpcCli)
	it, err := cli.Edges(minUUID, maxUUID, now)
	c.Assert(err, gc.IsNil)

	var edgeCount int
	for it.Next() {
		edgeCount++
		next := it.Edge()
		if next.ID != uuid1 && next.ID != uuid2 {
			c.Fatalf("unexpected link with ID %q", next.ID)
		}
		c.Assert(next.Src, gc.DeepEquals, srcID)
		c.Assert(next.Dst, gc.DeepEquals, dstID)
		c.Assert(next.UpdatedAt, gc.DeepEquals, updatedAt)
	}
	c.Assert(it.Error(), gc.IsNil)
	c.Assert(it.Close(), gc.IsNil)
	c.Assert(edgeCount, gc.Equals, 2)
}

func (s *ClientTestSuite) TestRetainVersionedEdges(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	rpcCli := mocks.NewMockLinkGraphClient(ctrl)

	from := uuid.New()
	now := time.Now()

	rpcCli.EXPECT().RemoveStaleEdges(
		gomock.AssignableToTypeOf(context.TODO()),
		&proto.RemoveStaleEdgesQuery{FromUuid: from[:], UpdatedBefore: mustEncodeTimestamp(c, now)},
	).Return(new(empty.Empty), nil)

	cli := linkgraphapi.NewLinkGraphClient(context.TODO(), rpcCli)
	err := cli.RemoveStaleEdges(from, now)
	c.Assert(err, gc.IsNil)
}
