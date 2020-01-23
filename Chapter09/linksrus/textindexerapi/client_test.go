package textindexerapi_test

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/textindexer/index"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/textindexerapi"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/textindexerapi/mocks"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/textindexerapi/proto"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/uuid"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(ClientTestSuite))

type ClientTestSuite struct{}

func (s *ClientTestSuite) TestIndex(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	rpcCli := mocks.NewMockTextIndexerClient(ctrl)

	now := time.Now().Truncate(time.Second).UTC()
	doc := &index.Document{
		LinkID:  uuid.New(),
		URL:     "http://example.com",
		Title:   "Title",
		Content: "Lorem Ipsum",
	}

	rpcCli.EXPECT().Index(
		gomock.AssignableToTypeOf(context.TODO()),
		&proto.Document{
			LinkId:  doc.LinkID[:],
			Url:     doc.URL,
			Title:   doc.Title,
			Content: doc.Content,
		},
	).Return(
		&proto.Document{
			LinkId:    doc.LinkID[:],
			Url:       doc.URL,
			Title:     doc.Title,
			Content:   doc.Content,
			IndexedAt: mustEncodeTimestamp(c, now),
		},
		nil,
	)

	cli := textindexerapi.NewTextIndexerClient(context.TODO(), rpcCli)
	err := cli.Index(doc)
	c.Assert(err, gc.IsNil)
	c.Assert(doc.IndexedAt, gc.Equals, now)
}

func (s *ClientTestSuite) TestUpdateScore(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	rpcCli := mocks.NewMockTextIndexerClient(ctrl)

	linkID := uuid.New()

	rpcCli.EXPECT().UpdateScore(
		gomock.AssignableToTypeOf(context.TODO()),
		&proto.UpdateScoreRequest{
			LinkId:        linkID[:],
			PageRankScore: 0.5,
		},
	).Return(new(empty.Empty), nil)

	cli := textindexerapi.NewTextIndexerClient(context.TODO(), rpcCli)
	err := cli.UpdateScore(linkID, 0.5)
	c.Assert(err, gc.IsNil)
}

func (s *ClientTestSuite) TestSearch(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	rpcCli := mocks.NewMockTextIndexerClient(ctrl)
	resultStream := mocks.NewMockTextIndexer_SearchClient(ctrl)

	ctxWithCancel, cancelFn := context.WithCancel(context.TODO())
	defer cancelFn()

	rpcCli.EXPECT().Search(
		gomock.AssignableToTypeOf(ctxWithCancel),
		&proto.Query{Type: proto.Query_MATCH, Expression: "foo"},
	).Return(resultStream, nil)

	now := time.Now().Truncate(time.Second).UTC()
	linkIDs := [2]uuid.UUID{uuid.New(), uuid.New()}
	returns := [][]interface{}{
		{&proto.QueryResult{Result: &proto.QueryResult_DocCount{DocCount: 2}}, nil},
		{&proto.QueryResult{Result: &proto.QueryResult_Doc{
			Doc: &proto.Document{
				LinkId:    linkIDs[0][:],
				Url:       "url-0",
				Title:     "title-0",
				Content:   "content-0",
				IndexedAt: mustEncodeTimestamp(c, now),
			},
		}}, nil},
		{&proto.QueryResult{Result: &proto.QueryResult_Doc{
			Doc: &proto.Document{
				LinkId:    linkIDs[1][:],
				Url:       "url-1",
				Title:     "title-1",
				Content:   "content-1",
				IndexedAt: mustEncodeTimestamp(c, now),
			},
		}}, nil},
		{nil, io.EOF},
	}
	resultStream.EXPECT().Recv().DoAndReturn(
		func() (interface{}, interface{}) {
			next := returns[0]
			returns = returns[1:]
			return next[0], next[1]
		},
	).Times(len(returns))

	cli := textindexerapi.NewTextIndexerClient(context.TODO(), rpcCli)
	it, err := cli.Search(index.Query{Type: index.QueryTypeMatch, Expression: "foo"})
	c.Assert(err, gc.IsNil)

	c.Assert(it.TotalCount(), gc.Equals, uint64(2))

	var docCount int
	for it.Next() {
		next := it.Document()
		c.Assert(next.LinkID, gc.DeepEquals, linkIDs[docCount])
		c.Assert(next.URL, gc.Equals, fmt.Sprintf("url-%d", docCount))
		c.Assert(next.Title, gc.Equals, fmt.Sprintf("title-%d", docCount))
		c.Assert(next.Content, gc.Equals, fmt.Sprintf("content-%d", docCount))
		c.Assert(next.IndexedAt, gc.Equals, now)

		docCount++
	}
	c.Assert(it.Error(), gc.IsNil)
	c.Assert(it.Close(), gc.IsNil)
	c.Assert(docCount, gc.Equals, 2)
}
