package service_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/graph"
	memgraph "github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/store/memory"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/textindexer/index"
	memindex "github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/textindexer/store/memory"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/pagerank"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/linksrus/pagerank/service"
	"github.com/google/uuid"
	"github.com/juju/clock/testclock"
	"github.com/sirupsen/logrus"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(DistributedPageRankTestSuite))

type DistributedPageRankTestSuite struct {
	logger    *logrus.Entry
	logOutput bytes.Buffer
}

func (s *DistributedPageRankTestSuite) SetUpTest(c *gc.C) {
	s.logOutput.Reset()
	rootLogger := logrus.New()
	rootLogger.Level = logrus.DebugLevel
	rootLogger.Out = &s.logOutput

	s.logger = logrus.NewEntry(rootLogger)
}

func (s *DistributedPageRankTestSuite) TearDownTest(c *gc.C) {
	c.Log(s.logOutput.String())
}

func (s *DistributedPageRankTestSuite) TestVerifyDistributedCalculationsAreCorrect(c *gc.C) {
	graphInstance, indexInstance := s.generateGraph(c, 2048, 10)

	// Run the calculations on a single instance so we can get a baseline
	// for our comparisons.
	singleResults := s.runStandaloneCalculator(c, graphInstance)

	// Reset the scores and run in distributed mode
	s.resetScores(c, graphInstance, indexInstance)
	distributedResults := s.runDistributedCalculator(c, graphInstance, indexInstance, 42)

	// Compare results
	deltaTolerance := 0.0001
	sumTolerance := 0.001
	s.assertResultsMatch(c, singleResults, distributedResults, deltaTolerance, sumTolerance)
}

func (s *DistributedPageRankTestSuite) assertResultsMatch(c *gc.C, singleResults, distributedResults map[uuid.UUID]float64, deltaTolerance, sumTolerance float64) {
	c.Assert(len(singleResults), gc.Equals, len(distributedResults), gc.Commentf("result count mismatch"))
	c.Logf("checking single and distributed run results (count %d)", len(singleResults))

	var singleSum, distributedSum float64
	for vertexID, singleScore := range singleResults {
		distributedScore, found := distributedResults[vertexID]
		c.Assert(found, gc.Equals, true, gc.Commentf("vertex %s not found in distributed result set", vertexID))

		absDelta := math.Abs(singleScore - distributedScore)
		c.Assert(absDelta <= deltaTolerance, gc.Equals, true, gc.Commentf("vertex %s: single score %v, distr. score %v, absDelta %v > %v", vertexID, singleScore, distributedScore, absDelta, deltaTolerance))

		singleSum += singleScore
		distributedSum += distributedScore
	}

	absDelta := math.Abs(1.0 - singleSum)
	c.Assert(absDelta <= sumTolerance, gc.Equals, true, gc.Commentf("expected all single run pagerank scores to add up to ~1.0; got %v, absDelta %v > %v", singleSum, absDelta, sumTolerance))

	absDelta = math.Abs(1.0 - distributedSum)
	c.Assert(absDelta <= sumTolerance, gc.Equals, true, gc.Commentf("expected all distributed run pagerank scores to add up to ~1.0; got %v, absDelta %v > %v", distributedSum, absDelta, sumTolerance))
}

func (s *DistributedPageRankTestSuite) resetScores(c *gc.C, graphInstance graph.Graph, indexInstance index.Indexer) {
	linkIt, err := graphInstance.Links(uuid.Nil, uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"), time.Now())
	c.Assert(err, gc.IsNil)
	for linkIt.Next() {
		err := indexInstance.UpdateScore(linkIt.Link().ID, 0)
		c.Assert(err, gc.IsNil)
	}
	c.Assert(linkIt.Close(), gc.IsNil)
}

// runStandaloneCalculator processes the graph using a single calculator
// instance with only one worker and returns back the calculated scores as a
// map.
func (s *DistributedPageRankTestSuite) runStandaloneCalculator(c *gc.C, graphInstance graph.Graph) map[uuid.UUID]float64 {
	calc, err := pagerank.NewCalculator(pagerank.Config{ComputeWorkers: 1})
	c.Assert(err, gc.IsNil)

	// Load links
	linkIt, err := graphInstance.Links(uuid.Nil, uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"), time.Now())
	c.Assert(err, gc.IsNil)
	for linkIt.Next() {
		calc.AddVertex(linkIt.Link().ID.String())
	}
	c.Assert(linkIt.Close(), gc.IsNil)

	// Load edges
	edgeIt, err := graphInstance.Edges(uuid.Nil, uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"), time.Now())
	c.Assert(err, gc.IsNil)
	for edgeIt.Next() {
		edge := edgeIt.Edge()
		err := calc.AddEdge(edge.Src.String(), edge.Dst.String())
		c.Assert(err, gc.IsNil)
	}
	c.Assert(edgeIt.Close(), gc.IsNil)

	// Execute graph and collect results
	err = calc.Executor().RunToCompletion(context.TODO())
	c.Assert(err, gc.IsNil)

	resMap := make(map[uuid.UUID]float64)
	err = calc.Scores(func(id string, score float64) error {
		resMap[uuid.MustParse(id)] = score
		return nil
	})
	c.Assert(err, gc.IsNil)

	return resMap
}

func (s *DistributedPageRankTestSuite) generateGraph(c *gc.C, numLinks, maxOutEdges int) (graph.Graph, index.Indexer) {
	graphInstance := memgraph.NewInMemoryGraph()
	indexInstance, err := memindex.NewInMemoryBleveIndexer()
	c.Assert(err, gc.IsNil)

	// Setup links
	now := time.Now()
	linkIDs := make([]uuid.UUID, numLinks)
	for i := 0; i < numLinks; i++ {
		linkURL := fmt.Sprintf("http://example.com/%d", i)
		link := &graph.Link{
			URL:         linkURL,
			RetrievedAt: now,
		}
		err = graphInstance.UpsertLink(link)
		c.Assert(err, gc.IsNil)

		linkIDs[i] = link.ID
		err = indexInstance.Index(&index.Document{
			LinkID: linkIDs[i],
			URL:    linkURL,
		})
		c.Assert(err, gc.IsNil)
	}

	// Make the link configuration deterministic across test runs
	rand.Seed(42)
	for i := 0; i < numLinks; i++ {
		edgeCount := rand.Intn(maxOutEdges)
		for j := 0; j < edgeCount; j++ {
			dst := rand.Intn(numLinks)
			err = graphInstance.UpsertEdge(&graph.Edge{
				Src: linkIDs[i],
				Dst: linkIDs[dst],
			})
			c.Assert(err, gc.IsNil)
		}
	}

	return graphInstance, indexInstance
}

// runDistributedCalculator processes the graph using the distributed calculator
// and returns back the calculated scores as a map.
func (s *DistributedPageRankTestSuite) runDistributedCalculator(c *gc.C, graphInstance graph.Graph, indexInstance index.Indexer, numWorkers int) map[uuid.UUID]float64 {
	var (
		ctx, cancelFn = context.WithCancel(context.TODO())
		clk           = testclock.NewClock(time.Now())
		wg            sync.WaitGroup
	)
	defer cancelFn()

	master, err := service.NewMasterNode(service.MasterConfig{
		ListenAddress:  ":9998",
		Clock:          clk,
		UpdateInterval: time.Minute,
		MinWorkers:     numWorkers,
		Logger:         s.logger.WithField("master", true),
	})
	c.Assert(err, gc.IsNil)

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(master.Run(ctx), gc.IsNil)
	}()

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func(i int) {
			defer wg.Done()
			worker, err := service.NewWorkerNode(service.WorkerConfig{
				MasterEndpoint:    ":9998",
				MasterDialTimeout: 10 * time.Second,
				GraphAPI:          graphInstance,
				IndexAPI:          indexInstance,
				ComputeWorkers:    runtime.NumCPU(),
				Logger:            s.logger.WithField("worker_id", i),
			})
			c.Assert(err, gc.IsNil)
			c.Assert(worker.Run(ctx), gc.IsNil)
		}(i)
	}

	// Trigger an update on the master
	c.Assert(clk.WaitAdvance(time.Minute, 60*time.Second, 1), gc.IsNil)

	// Wait till the master goes back to the main loop and stop it
	c.Assert(clk.WaitAdvance(time.Second, 60*time.Second, 1), gc.IsNil)
	cancelFn()

	// Wait for master and workers to shut down.
	wg.Wait()

	return s.extractScores(c, graphInstance, indexInstance)
}

func (s *DistributedPageRankTestSuite) extractScores(c *gc.C, graphInstance graph.Graph, indexInstance index.Indexer) map[uuid.UUID]float64 {
	resMap := make(map[uuid.UUID]float64)

	linkIt, err := graphInstance.Links(uuid.Nil, uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"), time.Now())
	c.Assert(err, gc.IsNil)
	for linkIt.Next() {
		doc, err := indexInstance.FindByID(linkIt.Link().ID)
		c.Assert(err, gc.IsNil)
		resMap[doc.LinkID] = doc.PageRank
	}
	c.Assert(linkIt.Close(), gc.IsNil)

	return resMap
}

func Test(t *testing.T) {
	// Run all gocheck test-suites
	gc.TestingT(t)
}
