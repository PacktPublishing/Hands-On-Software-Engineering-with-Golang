package textindexerapi_test

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/textindexer/index"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/textindexer/store/memory"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/textindexerapi"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/textindexerapi/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(ServerTestSuite))

type indexerCloser interface {
	io.Closer
	index.Indexer
}

type ServerTestSuite struct {
	i indexerCloser

	netListener *bufconn.Listener
	grpcSrv     *grpc.Server

	cliConn *grpc.ClientConn
	cli     proto.TextIndexerClient
}

func (s *ServerTestSuite) SetUpTest(c *gc.C) {
	var err error
	s.i, err = memory.NewInMemoryBleveIndexer()
	c.Assert(err, gc.IsNil)

	s.netListener = bufconn.Listen(1024)
	s.grpcSrv = grpc.NewServer()
	proto.RegisterTextIndexerServer(s.grpcSrv, textindexerapi.NewTextIndexerServer(s.i))
	go func() {
		err := s.grpcSrv.Serve(s.netListener)
		c.Assert(err, gc.IsNil)
	}()

	s.cliConn, err = grpc.Dial(
		"bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return s.netListener.Dial() }),
		grpc.WithInsecure(),
	)
	c.Assert(err, gc.IsNil)
	s.cli = proto.NewTextIndexerClient(s.cliConn)
}

func (s *ServerTestSuite) TearDownTest(c *gc.C) {
	_ = s.cliConn.Close()
	s.grpcSrv.Stop()
	_ = s.netListener.Close()
	_ = s.i.Close()
}

func (s *ServerTestSuite) TestIndex(c *gc.C) {
	linkID := uuid.New()
	doc := &proto.Document{
		LinkId:  linkID[:],
		Url:     "http://example.com",
		Title:   "Test",
		Content: "Lorem Ipsum",
	}
	res, err := s.cli.Index(context.TODO(), doc)
	c.Assert(err, gc.IsNil)
	c.Assert(res.Url, gc.Equals, doc.Url)
	c.Assert(res.Title, gc.Equals, doc.Title)
	c.Assert(res.Content, gc.Equals, doc.Content)

	// Check that document has been correctly indexed
	indexedDoc, err := s.i.FindByID(linkID)
	c.Assert(err, gc.IsNil)
	c.Assert(indexedDoc.URL, gc.Equals, doc.Url)
	c.Assert(indexedDoc.Title, gc.Equals, doc.Title)
	c.Assert(indexedDoc.Content, gc.Equals, doc.Content)
	c.Assert(indexedDoc.IndexedAt.Unix(), gc.Not(gc.Equals), 0)
}

func (s *ServerTestSuite) TestReindex(c *gc.C) {
	// Manually index test document
	linkID := uuid.New()
	doc := &index.Document{
		LinkID:  linkID,
		URL:     "http://example.com",
		Title:   "Test",
		Content: "Lorem Ipsum",
	}
	c.Assert(s.i.Index(doc), gc.IsNil)

	// Re-index existing document
	req := &proto.Document{
		LinkId:  doc.LinkID[:],
		Url:     "http://foo.com",
		Title:   "Bar",
		Content: "Baz",
	}

	res, err := s.cli.Index(context.TODO(), req)
	c.Assert(err, gc.IsNil)
	c.Assert(res.LinkId, gc.DeepEquals, doc.LinkID[:])
	c.Assert(res.Url, gc.Equals, req.Url)
	c.Assert(res.Title, gc.Equals, req.Title)
	c.Assert(res.Content, gc.Equals, req.Content)

	// Check that document has been corre\ctly re-indexed
	indexedDoc, err := s.i.FindByID(linkID)
	c.Assert(err, gc.IsNil)
	c.Assert(indexedDoc.URL, gc.Equals, res.Url)
	c.Assert(indexedDoc.Title, gc.Equals, res.Title)
	c.Assert(indexedDoc.Content, gc.Equals, res.Content)
}

func (s *ServerTestSuite) TestUpdateScore(c *gc.C) {
	// Manually index test document
	linkID := uuid.New()
	doc := &index.Document{
		LinkID:  linkID,
		URL:     "http://example.com",
		Title:   "Test",
		Content: "Lorem Ipsum",
	}
	c.Assert(s.i.Index(doc), gc.IsNil)

	// Update PageRank score and check that the document has been updated
	req := &proto.UpdateScoreRequest{
		LinkId:        linkID[:],
		PageRankScore: 0.5,
	}
	_, err := s.cli.UpdateScore(context.TODO(), req)
	c.Assert(err, gc.IsNil)

	indexedDoc, err := s.i.FindByID(linkID)
	c.Assert(err, gc.IsNil)
	c.Assert(indexedDoc.PageRank, gc.Equals, 0.5)
}

func (s *ServerTestSuite) TestSearch(c *gc.C) {
	idList := s.indexDocs(c, 100)

	stream, err := s.cli.Search(context.TODO(), &proto.Query{
		Type:       proto.Query_MATCH,
		Expression: "Test",
	})
	c.Assert(err, gc.IsNil)

	s.assertSearchResultsMatchList(c, stream, 100, idList)
}

func (s *ServerTestSuite) TestSearchWithOffset(c *gc.C) {
	idList := s.indexDocs(c, 100)

	stream, err := s.cli.Search(context.TODO(), &proto.Query{
		Type:       proto.Query_MATCH,
		Expression: "Test",
		Offset:     50,
	})
	c.Assert(err, gc.IsNil)

	s.assertSearchResultsMatchList(c, stream, 100, idList[50:])
}

func (s *ServerTestSuite) TestSearchWithOffsetAfterEndOfResultset(c *gc.C) {
	_ = s.indexDocs(c, 100)

	stream, err := s.cli.Search(context.TODO(), &proto.Query{
		Type:       proto.Query_MATCH,
		Expression: "Test",
		Offset:     101,
	})
	c.Assert(err, gc.IsNil)

	s.assertSearchResultsMatchList(c, stream, 100, nil)
}

func (s *ServerTestSuite) assertSearchResultsMatchList(c *gc.C, stream proto.TextIndexer_SearchClient, expTotalCount int, expIDList []uuid.UUID) {
	// First message should be the result count
	next, err := stream.Recv()
	c.Assert(err, gc.IsNil)
	c.Assert(next.GetDoc(), gc.IsNil, gc.Commentf("expected first message to contain result count only"))
	c.Assert(int(next.GetDocCount()), gc.Equals, expTotalCount)

	var docCount int
	for {
		next, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			c.Fatal(err)
		}

		doc := next.GetDoc()
		linkID, err := uuid.FromBytes(doc.LinkId)
		c.Assert(err, gc.IsNil)
		c.Assert(expIDList[docCount], gc.Equals, linkID)
		docCount++
	}

	c.Assert(docCount, gc.Equals, len(expIDList))
}

func (s *ServerTestSuite) indexDocs(c *gc.C, count int) []uuid.UUID {
	idList := make([]uuid.UUID, count)
	for i := 0; i < count; i++ {
		linkID := uuid.New()
		idList[i] = linkID
		err := s.i.Index(&index.Document{
			LinkID:  linkID,
			URL:     fmt.Sprintf("http://example.com/%d", i),
			Title:   fmt.Sprintf("Test-%d", i),
			Content: "Lorem Ipsum",
		})
		c.Assert(err, gc.IsNil)

		// Assign decending scores so documents sort in correct order
		// in search results.
		c.Assert(s.i.UpdateScore(linkID, float64(count-i)), gc.IsNil)
	}

	return idList
}
