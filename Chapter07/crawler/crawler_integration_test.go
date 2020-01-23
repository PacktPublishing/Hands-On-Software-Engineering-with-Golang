// +build integration_tests all_tests

package crawler_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/graph"
	memgraph "github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/store/memory"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/textindexer/index"
	memidx "github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/textindexer/store/memory"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter07/crawler"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter07/crawler/privnet"
	"github.com/google/uuid"
	gc "gopkg.in/check.v1"
)

var (
	_ = gc.Suite(new(CrawlerIntegrationTestSuite))

	serverRes = `
	<html>
	<head>
	  <title>A title</title>
	  <base href="http://google.com/"/>
	</head>
	<body>
	  <a href="./relative">I am a link relative to base</a>
	  <a href="/absolute/path">I am an absolute link</a>
	  <a href="//images/cart.png">I am using the same URL scheme as this page</a>
	  
	  <!-- Link should be added to the index but without creating an edge to it -->
	  <a href="ignore-me" rel="nofollow"/>

	  <!-- The following links should be ignored -->
	  <a href="file:///etc/passwd"></a>
	  <a href="http://169.254.169.254/api/credentials">Link-local address</a>
	</body>
	</html>`
)

type CrawlerIntegrationTestSuite struct{}

func (s *CrawlerIntegrationTestSuite) TestCrawlerPipeline(c *gc.C) {
	linkGraph := memgraph.NewInMemoryGraph()
	searchIndex := mustCreateBleveIndex(c)

	cfg := crawler.Config{
		PrivateNetworkDetector: mustCreatePrivateNetworkDetector(c),
		Graph:                  linkGraph,
		Indexer:                searchIndex,
		URLGetter:              http.DefaultClient,
		FetchWorkers:           5,
	}

	// Start a TLS server and a regular server
	srv1 := mustCreateTestServer(c)
	srv2 := mustCreateTestServer(c)
	defer srv1.Close()
	defer srv2.Close()

	mustImportLinks(c, linkGraph, []string{
		srv1.URL,
		srv2.URL,
	})

	count, err := crawler.NewCrawler(cfg).Crawl(
		context.Background(),
		mustGetLinkIterator(c, linkGraph),
	)
	c.Assert(err, gc.IsNil)
	c.Assert(count, gc.Equals, 2)

	s.assertGraphLinksMatchList(c, linkGraph, []string{
		srv1.URL,
		srv2.URL,
		"http://google.com/absolute/path",
		"http://google.com/relative",
		"http://google.com/ignore-me",
	})

	s.assertLinksIndexed(c, linkGraph, searchIndex,
		[]string{
			srv1.URL,
			srv2.URL,
		},
		"A title",
		"I am a link relative to base I am an absolute link I am using the same URL scheme as this page Link-local address",
	)
}

func (s *CrawlerIntegrationTestSuite) assertGraphLinksMatchList(c *gc.C, g graph.Graph, exp []string) {
	var got []string
	for it := mustGetLinkIterator(c, g); it.Next(); {
		got = append(got, it.Link().URL)
	}
	sort.Strings(exp)
	sort.Strings(got)

	c.Assert(got, gc.DeepEquals, exp)
}

func (s *CrawlerIntegrationTestSuite) assertLinksIndexed(c *gc.C, g graph.Graph, i index.Indexer, links []string, expTitle, expContent string) {
	var urlToID = make(map[string]uuid.UUID)
	for it := mustGetLinkIterator(c, g); it.Next(); {
		link := it.Link()
		urlToID[link.URL] = link.ID
	}

	zeroTime := time.Time{}
	for _, link := range links {
		id, exists := urlToID[link]
		c.Assert(exists, gc.Equals, true, gc.Commentf("link %q was not retrieved", link))

		doc, err := i.FindByID(id)
		c.Assert(err, gc.IsNil, gc.Commentf("link %q was not added to the search index", link))

		c.Assert(doc.Title, gc.Equals, expTitle)
		c.Assert(doc.Content, gc.Equals, expContent)
		c.Assert(doc.IndexedAt.After(zeroTime), gc.Equals, true, gc.Commentf("indexed document with zero IndexAt timestamp"))
	}
}

func mustCreateTestServer(c *gc.C) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Logf("GET %q", r.URL)
		w.Header().Set("Content-Type", "application/xhtml")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(serverRes))
		c.Assert(err, gc.IsNil)
	}))

	return srv
}

func mustGetLinkIterator(c *gc.C, g graph.Graph) graph.LinkIterator {
	it, err := g.Links(uuid.Nil, uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"), time.Now())
	c.Assert(err, gc.IsNil)
	return it
}

func mustCreatePrivateNetworkDetector(c *gc.C) *privnet.Detector {
	// Use a custom CIDR list to temporarily allow calls to the HTTP servers
	// that are listening on loopback addresses.
	det, err := privnet.NewDetectorFromCIDRs("169.254.0.0/16")
	c.Assert(err, gc.IsNil)
	return det
}

func mustCreateBleveIndex(c *gc.C) *memidx.InMemoryBleveIndexer {
	idx, err := memidx.NewInMemoryBleveIndexer()
	c.Assert(err, gc.IsNil)
	return idx
}

func mustImportLinks(c *gc.C, g graph.Graph, links []string) {
	for _, l := range links {
		err := g.UpsertLink(&graph.Link{
			URL: l,
		})
		c.Logf("importing %q into the graph", l)
		c.Assert(err, gc.IsNil, gc.Commentf("inserting %q", l))
	}
}
