package graphtest

import (
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/graph"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
	gc "gopkg.in/check.v1"
)

// SuiteBase defines a re-usable set of graph-related tests that can
// be executed against any type that implements graph.Graph.
type SuiteBase struct {
	g graph.Graph
}

// SetGraph configures the test-suite to run all tests against g.
func (s *SuiteBase) SetGraph(g graph.Graph) {
	s.g = g
}

// TestUpsertLink verifies the link upsert logic.
func (s *SuiteBase) TestUpsertLink(c *gc.C) {
	// Create a new link
	original := &graph.Link{
		URL:         "https://example.com",
		RetrievedAt: time.Now().Add(-10 * time.Hour),
	}

	err := s.g.UpsertLink(original)
	c.Assert(err, gc.IsNil)
	c.Assert(original.ID, gc.Not(gc.Equals), uuid.Nil, gc.Commentf("expected a linkID to be assigned to the new link"))

	// Update existing link with a newer timestamp and different URL
	accessedAt := time.Now().Truncate(time.Second).UTC()
	existing := &graph.Link{
		ID:          original.ID,
		URL:         "https://example.com",
		RetrievedAt: accessedAt,
	}
	err = s.g.UpsertLink(existing)
	c.Assert(err, gc.IsNil)
	c.Assert(existing.ID, gc.Equals, original.ID, gc.Commentf("link ID changed while upserting"))

	stored, err := s.g.FindLink(existing.ID)
	c.Assert(err, gc.IsNil)
	c.Assert(stored.RetrievedAt, gc.Equals, accessedAt, gc.Commentf("last accessed timestamp was not updated"))

	// Attempt to insert a new link whose URL matches an existing link with
	// and provide an older accessedAt value
	sameURL := &graph.Link{
		URL:         existing.URL,
		RetrievedAt: time.Now().Add(-10 * time.Hour).UTC(),
	}
	err = s.g.UpsertLink(sameURL)
	c.Assert(err, gc.IsNil)
	c.Assert(sameURL.ID, gc.Equals, existing.ID)

	stored, err = s.g.FindLink(existing.ID)
	c.Assert(err, gc.IsNil)
	c.Assert(stored.RetrievedAt, gc.Equals, accessedAt, gc.Commentf("last accessed timestamp was overwritten with an older value"))

	// Create a new link and then attempt to update its URL to the same as
	// an existing link.
	dup := &graph.Link{
		URL: "foo",
	}
	err = s.g.UpsertLink(dup)
	c.Assert(err, gc.IsNil)
	c.Assert(dup.ID, gc.Not(gc.Equals), uuid.Nil, gc.Commentf("expected a linkID to be assigned to the new link"))
}

// TestFindLink verifies the link lookup logic.
func (s *SuiteBase) TestFindLink(c *gc.C) {
	// Create a new link
	link := &graph.Link{
		URL:         "https://example.com",
		RetrievedAt: time.Now().Truncate(time.Second).UTC(),
	}

	err := s.g.UpsertLink(link)
	c.Assert(err, gc.IsNil)
	c.Assert(link.ID, gc.Not(gc.Equals), uuid.Nil, gc.Commentf("expected a linkID to be assigned to the new link"))

	// Lookup link by ID
	other, err := s.g.FindLink(link.ID)
	c.Assert(err, gc.IsNil)
	c.Assert(other, gc.DeepEquals, link, gc.Commentf("lookup by ID returned the wrong link"))

	// Lookup link by unknown ID
	_, err = s.g.FindLink(uuid.Nil)
	c.Assert(xerrors.Is(err, graph.ErrNotFound), gc.Equals, true)
}

// TestConcurrentLinkIterators verifies that multiple clients can concurrently
// access the store.
func (s *SuiteBase) TestConcurrentLinkIterators(c *gc.C) {
	var (
		wg           sync.WaitGroup
		numIterators = 10
		numLinks     = 100
	)

	for i := 0; i < numLinks; i++ {
		link := &graph.Link{URL: fmt.Sprint(i)}
		c.Assert(s.g.UpsertLink(link), gc.IsNil)
	}

	wg.Add(numIterators)
	for i := 0; i < numIterators; i++ {
		go func(id int) {
			defer wg.Done()

			itTagComment := gc.Commentf("iterator %d", id)
			seen := make(map[string]bool)
			it, err := s.partitionedLinkIterator(c, 0, 1, time.Now())
			c.Assert(err, gc.IsNil, itTagComment)
			defer func() {
				c.Assert(it.Close(), gc.IsNil, itTagComment)
			}()

			for i := 0; it.Next(); i++ {
				link := it.Link()
				linkID := link.ID.String()
				c.Assert(seen[linkID], gc.Equals, false, gc.Commentf("iterator %d saw same link twice", id))
				seen[linkID] = true
			}

			c.Assert(seen, gc.HasLen, numLinks, itTagComment)
			c.Assert(it.Error(), gc.IsNil, itTagComment)
			c.Assert(it.Close(), gc.IsNil, itTagComment)
		}(i)
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
	// test completed successfully
	case <-time.After(10 * time.Second):
		c.Fatal("timed out waiting for test to complete")
	}
}

// TestLinkIteratorTimeFilter verifies that the time-based filtering of the
// link iterator works as expected.
func (s *SuiteBase) TestLinkIteratorTimeFilter(c *gc.C) {
	linkUUIDs := make([]uuid.UUID, 3)
	linkInsertTimes := make([]time.Time, len(linkUUIDs))
	for i := 0; i < len(linkUUIDs); i++ {
		link := &graph.Link{URL: fmt.Sprint(i), RetrievedAt: time.Now()}
		c.Assert(s.g.UpsertLink(link), gc.IsNil)
		linkUUIDs[i] = link.ID
		linkInsertTimes[i] = time.Now()
	}

	for i, t := range linkInsertTimes {
		c.Logf("fetching links created before edge %d", i)
		s.assertIteratedLinkIDsMatch(c, t, linkUUIDs[:i+1])
	}
}

func (s *SuiteBase) assertIteratedLinkIDsMatch(c *gc.C, updatedBefore time.Time, exp []uuid.UUID) {
	it, err := s.partitionedLinkIterator(c, 0, 1, updatedBefore)
	c.Assert(err, gc.IsNil)

	var got []uuid.UUID
	for it.Next() {
		got = append(got, it.Link().ID)
	}
	c.Assert(it.Error(), gc.IsNil)
	c.Assert(it.Close(), gc.IsNil)

	sort.Slice(got, func(l, r int) bool { return got[l].String() < got[r].String() })
	sort.Slice(exp, func(l, r int) bool { return exp[l].String() < exp[r].String() })
	c.Assert(got, gc.DeepEquals, exp)
}

// TestPartitionedLinkIterators verifies that the graph partitioning logic
// works as expected even when partitions contain an uneven number of items.
func (s *SuiteBase) TestPartitionedLinkIterators(c *gc.C) {
	numLinks := 100
	numPartitions := 10
	for i := 0; i < numLinks; i++ {
		c.Assert(s.g.UpsertLink(&graph.Link{URL: fmt.Sprint(i)}), gc.IsNil)
	}

	// Check with both odd and even partition counts to check for rounding-related bugs.
	c.Assert(s.iteratePartitionedLinks(c, numPartitions), gc.Equals, numLinks)
	c.Assert(s.iteratePartitionedLinks(c, numPartitions+1), gc.Equals, numLinks)
}

func (s *SuiteBase) iteratePartitionedLinks(c *gc.C, numPartitions int) int {
	seen := make(map[string]bool)
	for partition := 0; partition < numPartitions; partition++ {
		it, err := s.partitionedLinkIterator(c, partition, numPartitions, time.Now())
		c.Assert(err, gc.IsNil)
		defer func() {
			c.Assert(it.Close(), gc.IsNil)
		}()

		for it.Next() {
			link := it.Link()
			linkID := link.ID.String()
			c.Assert(seen[linkID], gc.Equals, false, gc.Commentf("iterator returned same link in different partitions"))
			seen[linkID] = true
		}

		c.Assert(it.Error(), gc.IsNil)
		c.Assert(it.Close(), gc.IsNil)
	}

	return len(seen)
}

// TestUpsertEdge verifies the edge upsert logic.
func (s *SuiteBase) TestUpsertEdge(c *gc.C) {
	// Create links
	linkUUIDs := make([]uuid.UUID, 3)
	for i := 0; i < 3; i++ {
		link := &graph.Link{URL: fmt.Sprint(i)}
		c.Assert(s.g.UpsertLink(link), gc.IsNil)
		linkUUIDs[i] = link.ID
	}

	// Create a edge
	edge := &graph.Edge{
		Src: linkUUIDs[0],
		Dst: linkUUIDs[1],
	}

	err := s.g.UpsertEdge(edge)
	c.Assert(err, gc.IsNil)
	c.Assert(edge.ID, gc.Not(gc.Equals), uuid.Nil, gc.Commentf("expected an edgeID to be assigned to the new edge"))
	c.Assert(edge.UpdatedAt.IsZero(), gc.Equals, false, gc.Commentf("UpdatedAt field not set"))

	// Update existing edge
	other := &graph.Edge{
		ID:  edge.ID,
		Src: linkUUIDs[0],
		Dst: linkUUIDs[1],
	}
	err = s.g.UpsertEdge(other)
	c.Assert(err, gc.IsNil)
	c.Assert(other.ID, gc.Equals, edge.ID, gc.Commentf("edge ID changed while upserting"))
	c.Assert(other.UpdatedAt, gc.Not(gc.Equals), edge.UpdatedAt, gc.Commentf("UpdatedAt field not modified"))

	// Create edge with unknown link IDs
	bogus := &graph.Edge{
		Src: linkUUIDs[0],
		Dst: uuid.New(),
	}
	err = s.g.UpsertEdge(bogus)
	c.Assert(xerrors.Is(err, graph.ErrUnknownEdgeLinks), gc.Equals, true)
}

// TestConcurrentEdgeIterators verifies that multiple clients can concurrently
// access the store.
func (s *SuiteBase) TestConcurrentEdgeIterators(c *gc.C) {
	var (
		wg           sync.WaitGroup
		numIterators = 10
		numEdges     = 100
		linkUUIDs    = make([]uuid.UUID, numEdges*2)
	)

	for i := 0; i < numEdges*2; i++ {
		link := &graph.Link{URL: fmt.Sprint(i)}
		c.Assert(s.g.UpsertLink(link), gc.IsNil)
		linkUUIDs[i] = link.ID
	}
	for i := 0; i < numEdges; i++ {
		c.Assert(s.g.UpsertEdge(&graph.Edge{
			Src: linkUUIDs[0],
			Dst: linkUUIDs[i],
		}), gc.IsNil)
	}

	wg.Add(numIterators)
	for i := 0; i < numIterators; i++ {
		go func(id int) {
			defer wg.Done()

			itTagComment := gc.Commentf("iterator %d", id)
			seen := make(map[string]bool)
			it, err := s.partitionedEdgeIterator(c, 0, 1, time.Now())
			c.Assert(err, gc.IsNil, itTagComment)
			defer func() {
				c.Assert(it.Close(), gc.IsNil, itTagComment)
			}()

			for i := 0; it.Next(); i++ {
				edge := it.Edge()
				edgeID := edge.ID.String()
				c.Assert(seen[edgeID], gc.Equals, false, gc.Commentf("iterator %d saw same edge twice", id))
				seen[edgeID] = true
			}

			c.Assert(seen, gc.HasLen, numEdges, itTagComment)
			c.Assert(it.Error(), gc.IsNil, itTagComment)
			c.Assert(it.Close(), gc.IsNil, itTagComment)
		}(i)
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
	// test completed successfully
	case <-time.After(10 * time.Second):
		c.Fatal("timed out waiting for test to complete")
	}
}

// TestEdgeIteratorTimeFilter verifies that the time-based filtering of the
// edge iterator works as expected.
func (s *SuiteBase) TestEdgeIteratorTimeFilter(c *gc.C) {
	linkUUIDs := make([]uuid.UUID, 3)
	linkInsertTimes := make([]time.Time, len(linkUUIDs))
	for i := 0; i < len(linkUUIDs); i++ {
		link := &graph.Link{URL: fmt.Sprint(i)}
		c.Assert(s.g.UpsertLink(link), gc.IsNil)
		linkUUIDs[i] = link.ID
		linkInsertTimes[i] = time.Now()
	}

	edgeUUIDs := make([]uuid.UUID, len(linkUUIDs))
	edgeInsertTimes := make([]time.Time, len(linkUUIDs))
	for i := 0; i < len(linkUUIDs); i++ {
		edge := &graph.Edge{Src: linkUUIDs[0], Dst: linkUUIDs[i]}
		c.Assert(s.g.UpsertEdge(edge), gc.IsNil)
		edgeUUIDs[i] = edge.ID
		edgeInsertTimes[i] = time.Now()
	}

	for i, t := range edgeInsertTimes {
		c.Logf("fetching edges created before edge %d", i)
		s.assertIteratedEdgeIDsMatch(c, t, edgeUUIDs[:i+1])
	}
}

func (s *SuiteBase) assertIteratedEdgeIDsMatch(c *gc.C, updatedBefore time.Time, exp []uuid.UUID) {
	it, err := s.partitionedEdgeIterator(c, 0, 1, updatedBefore)
	c.Assert(err, gc.IsNil)

	var got []uuid.UUID
	for it.Next() {
		got = append(got, it.Edge().ID)
	}
	c.Assert(it.Error(), gc.IsNil)
	c.Assert(it.Close(), gc.IsNil)

	sort.Slice(got, func(l, r int) bool { return got[l].String() < got[r].String() })
	sort.Slice(exp, func(l, r int) bool { return exp[l].String() < exp[r].String() })
	c.Assert(got, gc.DeepEquals, exp)
}

// TestPartitionedEdgeIterators verifies that the graph partitioning logic
// works as expected even when partitions contain an uneven number of items.
func (s *SuiteBase) TestPartitionedEdgeIterators(c *gc.C) {
	numEdges := 100
	numPartitions := 10
	linkUUIDs := make([]uuid.UUID, numEdges*2)
	for i := 0; i < numEdges*2; i++ {
		link := &graph.Link{URL: fmt.Sprint(i)}
		c.Assert(s.g.UpsertLink(link), gc.IsNil)
		linkUUIDs[i] = link.ID
	}
	for i := 0; i < numEdges; i++ {
		c.Assert(s.g.UpsertEdge(&graph.Edge{
			Src: linkUUIDs[0],
			Dst: linkUUIDs[i],
		}), gc.IsNil)
	}

	// Check with both odd and even partition counts to check for rounding-related bugs.
	c.Assert(s.iteratePartitionedEdges(c, numPartitions), gc.Equals, numEdges)
	c.Assert(s.iteratePartitionedEdges(c, numPartitions+1), gc.Equals, numEdges)
}

func (s *SuiteBase) iteratePartitionedEdges(c *gc.C, numPartitions int) int {
	seen := make(map[string]bool)
	for partition := 0; partition < numPartitions; partition++ {
		// Build list of expected edges per partition. An edge belongs to a
		// partition if its origin link also belongs to the same partition.
		linksInPartition := make(map[uuid.UUID]struct{})
		linkIt, err := s.partitionedLinkIterator(c, partition, numPartitions, time.Now())
		c.Assert(err, gc.IsNil)
		for linkIt.Next() {
			linkID := linkIt.Link().ID
			linksInPartition[linkID] = struct{}{}
		}

		it, err := s.partitionedEdgeIterator(c, partition, numPartitions, time.Now())
		c.Assert(err, gc.IsNil)
		defer func() {
			c.Assert(it.Close(), gc.IsNil)
		}()

		for it.Next() {
			edge := it.Edge()
			edgeID := edge.ID.String()
			c.Assert(seen[edgeID], gc.Equals, false, gc.Commentf("iterator returned same edge in different partitions"))
			seen[edgeID] = true

			_, srcInPartition := linksInPartition[edge.Src]
			c.Assert(srcInPartition, gc.Equals, true, gc.Commentf("iterator returned an edge whose source link belongs to a different partition"))
		}

		c.Assert(it.Error(), gc.IsNil)
		c.Assert(it.Close(), gc.IsNil)
	}

	return len(seen)
}

// TestRemoveStaleEdges verifies that the edge deletion logic works as expected.
func (s *SuiteBase) TestRemoveStaleEdges(c *gc.C) {
	numEdges := 100
	linkUUIDs := make([]uuid.UUID, numEdges*4)
	goneUUIDs := make(map[uuid.UUID]struct{})
	for i := 0; i < numEdges*4; i++ {
		link := &graph.Link{URL: fmt.Sprint(i)}
		c.Assert(s.g.UpsertLink(link), gc.IsNil)
		linkUUIDs[i] = link.ID
	}

	var lastTs time.Time
	for i := 0; i < numEdges; i++ {
		e1 := &graph.Edge{
			Src: linkUUIDs[0],
			Dst: linkUUIDs[i],
		}
		c.Assert(s.g.UpsertEdge(e1), gc.IsNil)
		goneUUIDs[e1.ID] = struct{}{}
		lastTs = e1.UpdatedAt
	}

	deleteBefore := lastTs.Add(time.Millisecond)
	time.Sleep(250 * time.Millisecond)

	// The following edges will have an updated at value > lastTs
	for i := 0; i < numEdges; i++ {
		e2 := &graph.Edge{
			Src: linkUUIDs[0],
			Dst: linkUUIDs[numEdges+i+1],
		}
		c.Assert(s.g.UpsertEdge(e2), gc.IsNil)
	}
	c.Assert(s.g.RemoveStaleEdges(linkUUIDs[0], deleteBefore), gc.IsNil)

	it, err := s.partitionedEdgeIterator(c, 0, 1, time.Now())
	c.Assert(err, gc.IsNil)
	defer func() { c.Assert(it.Close(), gc.IsNil) }()

	var seen int
	for it.Next() {
		id := it.Edge().ID
		_, found := goneUUIDs[id]
		c.Assert(found, gc.Equals, false, gc.Commentf("expected edge %s to be removed from the edge list", id.String()))
		seen++
	}

	c.Assert(seen, gc.Equals, numEdges)
}

func (s *SuiteBase) partitionedLinkIterator(c *gc.C, partition, numPartitions int, accessedBefore time.Time) (graph.LinkIterator, error) {
	from, to := s.partitionRange(c, partition, numPartitions)
	return s.g.Links(from, to, accessedBefore)
}

func (s *SuiteBase) partitionedEdgeIterator(c *gc.C, partition, numPartitions int, updatedBefore time.Time) (graph.EdgeIterator, error) {
	from, to := s.partitionRange(c, partition, numPartitions)
	return s.g.Edges(from, to, updatedBefore)
}

func (s *SuiteBase) partitionRange(c *gc.C, partition, numPartitions int) (from, to uuid.UUID) {
	if partition < 0 || partition >= numPartitions {
		c.Fatal("invalid partition")
	}

	var minUUID = uuid.Nil
	var maxUUID = uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	var err error

	// Calculate the size of each partition as: (2^128 / numPartitions)
	tokenRange := big.NewInt(0)
	partSize := big.NewInt(0)
	partSize.SetBytes(maxUUID[:])
	partSize = partSize.Div(partSize, big.NewInt(int64(numPartitions)))

	// We model the partitions as a segment that begins at minUUID (all
	// bits set to zero) and ends at maxUUID (all bits set to 1). By
	// setting the end range for the *last* partition to maxUUID we ensure
	// that we always cover the full range of UUIDs even if the range
	// itself is not evenly divisible by numPartitions.
	if partition == 0 {
		from = minUUID
	} else {
		tokenRange.Mul(partSize, big.NewInt(int64(partition)))
		from, err = uuid.FromBytes(tokenRange.Bytes())
		c.Assert(err, gc.IsNil)
	}

	if partition == numPartitions-1 {
		to = maxUUID
	} else {
		tokenRange.Mul(partSize, big.NewInt(int64(partition+1)))
		to, err = uuid.FromBytes(tokenRange.Bytes())
		c.Assert(err, gc.IsNil)
	}

	return from, to
}
