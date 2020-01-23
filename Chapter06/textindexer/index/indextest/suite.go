package indextest

import (
	"fmt"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/textindexer/index"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
	gc "gopkg.in/check.v1"
)

// SuiteBase defines a re-usable set of index-related tests that can
// be executed against any type that implements index.Indexer.
type SuiteBase struct {
	idx index.Indexer
}

// SetIndexer configures the test-suite to run all tests against idx.
func (s *SuiteBase) SetIndexer(idx index.Indexer) {
	s.idx = idx
}

// TestIndexDocument verifies the indexing logic for new and existing documents.
func (s *SuiteBase) TestIndexDocument(c *gc.C) {
	// Insert new Document
	doc := &index.Document{
		LinkID:    uuid.New(),
		URL:       "http://example.com",
		Title:     "Illustrious examples",
		Content:   "Lorem ipsum dolor",
		IndexedAt: time.Now().Add(-12 * time.Hour).UTC(),
	}

	err := s.idx.Index(doc)
	c.Assert(err, gc.IsNil)

	// Update existing Document
	updatedDoc := &index.Document{
		LinkID:    doc.LinkID,
		URL:       "http://example.com",
		Title:     "A more exciting title",
		Content:   "Ovidius poeta in terra pontica",
		IndexedAt: time.Now().UTC(),
	}

	err = s.idx.Index(updatedDoc)
	c.Assert(err, gc.IsNil)

	// Insert document without an ID
	incompleteDoc := &index.Document{
		URL: "http://example.com",
	}

	err = s.idx.Index(incompleteDoc)
	c.Assert(xerrors.Is(err, index.ErrMissingLinkID), gc.Equals, true)
}

// TestIndexDoesNotOverridePageRank verifies the indexing logic for new and
// existing documents.
func (s *SuiteBase) TestIndexDoesNotOverridePageRank(c *gc.C) {
	// Insert new Document
	doc := &index.Document{
		LinkID:    uuid.New(),
		URL:       "http://example.com",
		Title:     "Illustrious examples",
		Content:   "Lorem ipsum dolor",
		IndexedAt: time.Now().Add(-12 * time.Hour).UTC(),
	}

	err := s.idx.Index(doc)
	c.Assert(err, gc.IsNil)

	// Update its score
	expScore := 0.5
	err = s.idx.UpdateScore(doc.LinkID, expScore)
	c.Assert(err, gc.IsNil)

	// Update document
	updatedDoc := &index.Document{
		LinkID:    doc.LinkID,
		URL:       "http://example.com",
		Title:     "A more exciting title",
		Content:   "Ovidius poeta in terra pontica",
		IndexedAt: time.Now().UTC(),
	}

	err = s.idx.Index(updatedDoc)
	c.Assert(err, gc.IsNil)

	// Lookup document and verify that PageRank score has not been changed.
	got, err := s.idx.FindByID(doc.LinkID)
	c.Assert(err, gc.IsNil)
	c.Assert(got.PageRank, gc.Equals, expScore)
}

// TestFindByID verifies the document lookup logic.
func (s *SuiteBase) TestFindByID(c *gc.C) {
	doc := &index.Document{
		LinkID:    uuid.New(),
		URL:       "http://example.com",
		Title:     "Illustrious examples",
		Content:   "Lorem ipsum dolor",
		IndexedAt: time.Now().Add(-12 * time.Hour).UTC(),
	}

	err := s.idx.Index(doc)
	c.Assert(err, gc.IsNil)

	// Look up doc
	got, err := s.idx.FindByID(doc.LinkID)
	c.Assert(err, gc.IsNil)
	c.Assert(got, gc.DeepEquals, doc, gc.Commentf("document returned by FindByID does not match inserted document"))

	// Look up unknown
	_, err = s.idx.FindByID(uuid.New())
	c.Assert(xerrors.Is(err, index.ErrNotFound), gc.Equals, true)
}

// TestPhraseSearch verifies the document search logic when searching for
// exact phrases.
func (s *SuiteBase) TestPhraseSearch(c *gc.C) {
	var (
		numDocs = 50
		expIDs  []uuid.UUID
	)
	for i := 0; i < numDocs; i++ {
		id := uuid.New()
		doc := &index.Document{
			LinkID:  id,
			Title:   fmt.Sprintf("doc with ID %s", id.String()),
			Content: "Lorem Ipsum Dolor",
		}

		if i%5 == 0 {
			doc.Content = "Lorem Dolor Ipsum"
			expIDs = append(expIDs, id)
		}

		err := s.idx.Index(doc)
		c.Assert(err, gc.IsNil)

		err = s.idx.UpdateScore(id, float64(numDocs-i))
		c.Assert(err, gc.IsNil)
	}

	it, err := s.idx.Search(index.Query{
		Type:       index.QueryTypePhrase,
		Expression: "lorem dolor ipsum",
	})
	c.Assert(err, gc.IsNil)
	c.Assert(iterateDocs(c, it), gc.DeepEquals, expIDs)
}

// TestMatchSearch verifies the document search logic when searching for
// keyword matches.
func (s *SuiteBase) TestMatchSearch(c *gc.C) {
	var (
		numDocs = 50
		expIDs  []uuid.UUID
	)
	for i := 0; i < numDocs; i++ {
		id := uuid.New()
		doc := &index.Document{
			LinkID:  id,
			Title:   fmt.Sprintf("doc with ID %s", id.String()),
			Content: "Ovidius poeta in terra pontica",
		}

		if i%5 == 0 {
			doc.Content = "Lorem Dolor Ipsum"
			expIDs = append(expIDs, id)
		}

		err := s.idx.Index(doc)
		c.Assert(err, gc.IsNil)

		err = s.idx.UpdateScore(id, float64(numDocs-i))
		c.Assert(err, gc.IsNil)
	}

	it, err := s.idx.Search(index.Query{
		Type:       index.QueryTypeMatch,
		Expression: "lorem ipsum",
	})
	c.Assert(err, gc.IsNil)
	c.Assert(iterateDocs(c, it), gc.DeepEquals, expIDs)
}

// TestMatchSearchWithOffset verifies the document search logic when searching
// for keyword matches and skipping some results.
func (s *SuiteBase) TestMatchSearchWithOffset(c *gc.C) {
	var (
		numDocs = 50
		expIDs  []uuid.UUID
	)
	for i := 0; i < numDocs; i++ {
		id := uuid.New()
		expIDs = append(expIDs, id)
		doc := &index.Document{
			LinkID:  id,
			Title:   fmt.Sprintf("doc with ID %s", id.String()),
			Content: "Ovidius poeta in terra pontica",
		}

		err := s.idx.Index(doc)
		c.Assert(err, gc.IsNil)

		err = s.idx.UpdateScore(id, float64(numDocs-i))
		c.Assert(err, gc.IsNil)
	}

	it, err := s.idx.Search(index.Query{
		Type:       index.QueryTypeMatch,
		Expression: "poeta",
		Offset:     20,
	})
	c.Assert(err, gc.IsNil)
	c.Assert(iterateDocs(c, it), gc.DeepEquals, expIDs[20:])

	// Search with offset beyon the total number of results
	it, err = s.idx.Search(index.Query{
		Type:       index.QueryTypeMatch,
		Expression: "poeta",
		Offset:     200,
	})
	c.Assert(err, gc.IsNil)
	c.Assert(iterateDocs(c, it), gc.HasLen, 0)
}

// TestUpdateScore checks that PageRank score updates work as expected.
func (s *SuiteBase) TestUpdateScore(c *gc.C) {
	var (
		numDocs = 100
		expIDs  []uuid.UUID
	)
	for i := 0; i < numDocs; i++ {
		id := uuid.New()
		expIDs = append(expIDs, id)
		doc := &index.Document{
			LinkID:  id,
			Title:   fmt.Sprintf("doc with ID %s", id.String()),
			Content: "Ovidius poeta in terra pontica",
		}

		err := s.idx.Index(doc)
		c.Assert(err, gc.IsNil)

		err = s.idx.UpdateScore(id, float64(numDocs-i))
		c.Assert(err, gc.IsNil)
	}

	it, err := s.idx.Search(index.Query{
		Type:       index.QueryTypeMatch,
		Expression: "poeta",
	})
	c.Assert(err, gc.IsNil)
	c.Assert(iterateDocs(c, it), gc.DeepEquals, expIDs)

	// Update the pagerank scores so that results are sorted in the
	// reverse order.
	for i := 0; i < numDocs; i++ {
		err = s.idx.UpdateScore(expIDs[i], float64(i))
		c.Assert(err, gc.IsNil, gc.Commentf(expIDs[i].String()))
	}

	it, err = s.idx.Search(index.Query{
		Type:       index.QueryTypeMatch,
		Expression: "poeta",
	})
	c.Assert(err, gc.IsNil)
	c.Assert(iterateDocs(c, it), gc.DeepEquals, reverse(expIDs))
}

// TestUpdateScoreForUnknownDocument checks that a placeholder document will
// be created when setting the PageRank score for an unknown document.
func (s *SuiteBase) TestUpdateScoreForUnknownDocument(c *gc.C) {
	linkID := uuid.New()
	err := s.idx.UpdateScore(linkID, 0.5)
	c.Assert(err, gc.IsNil)

	doc, err := s.idx.FindByID(linkID)
	c.Assert(err, gc.IsNil)

	c.Assert(doc.URL, gc.Equals, "")
	c.Assert(doc.Title, gc.Equals, "")
	c.Assert(doc.Content, gc.Equals, "")
	c.Assert(doc.IndexedAt.IsZero(), gc.Equals, true)
	c.Assert(doc.PageRank, gc.Equals, 0.5)
}

func iterateDocs(c *gc.C, it index.Iterator) []uuid.UUID {
	var seen []uuid.UUID
	for it.Next() {
		seen = append(seen, it.Document().LinkID)
	}
	c.Assert(it.Error(), gc.IsNil)
	c.Assert(it.Close(), gc.IsNil)
	return seen
}

func reverse(in []uuid.UUID) []uuid.UUID {
	for left, right := 0, len(in)-1; left < right; left, right = left+1, right-1 {
		in[left], in[right] = in[right], in[left]
	}

	return in
}
