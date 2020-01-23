package crawler

import (
	"context"
	"fmt"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/textindexer/index"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter07/crawler/mocks"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(TextIndexerTestSuite))

type TextIndexerTestSuite struct {
	indexer *mocks.MockIndexer
}

func (s *TextIndexerTestSuite) TestTextIndexer(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	s.indexer = mocks.NewMockIndexer(ctrl)

	payload := &crawlerPayload{
		LinkID:      uuid.New(),
		URL:         "http://example.com",
		Title:       "some title",
		TextContent: "Lorem ipsum dolor",
	}

	exp := s.indexer.EXPECT()
	exp.Index(docMatcher{
		linkID:    payload.LinkID,
		url:       payload.URL,
		title:     payload.Title,
		content:   payload.TextContent,
		notBefore: time.Now(),
	}).Return(nil)

	p := s.updateIndex(c, payload)
	c.Assert(p, gc.Not(gc.IsNil))
}

func (s *TextIndexerTestSuite) updateIndex(c *gc.C, p *crawlerPayload) *crawlerPayload {
	out, err := newTextIndexer(s.indexer).Process(context.TODO(), p)
	c.Assert(err, gc.IsNil)
	if out != nil {
		c.Assert(out, gc.FitsTypeOf, p)
		return out.(*crawlerPayload)
	}

	return nil
}

type docMatcher struct {
	linkID    uuid.UUID
	url       string
	title     string
	content   string
	notBefore time.Time
}

func (dm docMatcher) Matches(x interface{}) bool {
	doc := x.(*index.Document)
	return dm.linkID == doc.LinkID &&
		dm.url == doc.URL &&
		dm.title == doc.Title &&
		dm.content == doc.Content &&
		!doc.IndexedAt.Before(dm.notBefore)
}

func (dm docMatcher) String() string {
	return fmt.Sprintf("has LinkID=%q, URL=%q, Title=%q, Content=%q and IndexedAt not before %v", dm.linkID, dm.url, dm.title, dm.content, dm.notBefore)
}
