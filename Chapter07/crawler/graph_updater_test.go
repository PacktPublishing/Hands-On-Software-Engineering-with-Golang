package crawler

import (
	"context"
	"fmt"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/graph"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter07/crawler/mocks"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(GraphUpdaterTestSuite))

type GraphUpdaterTestSuite struct {
	graph *mocks.MockGraph
}

func (s *GraphUpdaterTestSuite) TestGraphUpdater(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	s.graph = mocks.NewMockGraph(ctrl)

	payload := &crawlerPayload{
		LinkID: uuid.New(),
		URL:    "http://example.com",
		NoFollowLinks: []string{
			"http://forum.com",
		},
		Links: []string{
			"http://example.com/foo",
			"http://example.com/bar",
		},
	}

	exp := s.graph.EXPECT()

	// We expect the original link to be upserted with a new timestamp and
	// two additional insert calls for the discovered links.
	exp.UpsertLink(linkMatcher{id: payload.LinkID, url: payload.URL, notBefore: time.Now()}).Return(nil)

	id0, id1, id2 := uuid.New(), uuid.New(), uuid.New()
	exp.UpsertLink(linkMatcher{url: "http://forum.com", notBefore: time.Time{}}).DoAndReturn(setLinkID(id0))
	exp.UpsertLink(linkMatcher{url: "http://example.com/foo", notBefore: time.Time{}}).DoAndReturn(setLinkID(id1))
	exp.UpsertLink(linkMatcher{url: "http://example.com/bar", notBefore: time.Time{}}).DoAndReturn(setLinkID(id2))

	// We then expect two edges to be created from the origin link to the
	// two links we just created.
	exp.UpsertEdge(edgeMatcher{src: payload.LinkID, dst: id1}).Return(nil)
	exp.UpsertEdge(edgeMatcher{src: payload.LinkID, dst: id2}).Return(nil)

	// Finally we expect a call to drop stale edges whose source is the origin link.
	exp.RemoveStaleEdges(payload.LinkID, gomock.Any()).Return(nil)

	p := s.updateGraph(c, payload)
	c.Assert(p, gc.Not(gc.IsNil))
}

func (s *GraphUpdaterTestSuite) updateGraph(c *gc.C, p *crawlerPayload) *crawlerPayload {
	out, err := newGraphUpdater(s.graph).Process(context.TODO(), p)
	c.Assert(err, gc.IsNil)
	if out != nil {
		c.Assert(out, gc.FitsTypeOf, p)
		return out.(*crawlerPayload)
	}

	return nil
}

func setLinkID(id uuid.UUID) func(*graph.Link) error {
	return func(link *graph.Link) error {
		link.ID = id
		return nil
	}
}

type linkMatcher struct {
	id        uuid.UUID
	url       string
	notBefore time.Time
}

func (lm linkMatcher) Matches(x interface{}) bool {
	link := x.(*graph.Link)
	return lm.id == link.ID &&
		lm.url == link.URL &&
		!link.RetrievedAt.Before(lm.notBefore)
}

func (lm linkMatcher) String() string {
	return fmt.Sprintf("has ID=%q, URL=%q and LastAccessed not before %v", lm.id, lm.url, lm.notBefore)
}

type edgeMatcher struct {
	src uuid.UUID
	dst uuid.UUID
}

func (em edgeMatcher) Matches(x interface{}) bool {
	edge := x.(*graph.Edge)
	return em.src == edge.Src && em.dst == edge.Dst
}

func (em edgeMatcher) String() string {
	return fmt.Sprintf("has Src=%q and Dst=%q", em.src, em.dst)
}
