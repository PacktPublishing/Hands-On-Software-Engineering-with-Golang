package crawler

import (
	"context"
	"net/url"
	"sort"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter07/crawler/mocks"
	"github.com/golang/mock/gomock"
	gc "gopkg.in/check.v1"
)

var (
	_ = gc.Suite(new(ResolveURLTestSuite))
	_ = gc.Suite(new(LinkExtractorTestSuite))
)

type ResolveURLTestSuite struct{}

func (s *ResolveURLTestSuite) TestResolveAbsoluteURL(c *gc.C) {
	assertResolvedURL(c,
		"/bar/baz",
		"http://example.com/foo/",
		"http://example.com/bar/baz",
	)
}

func (s *ResolveURLTestSuite) TestResolveRelativeURL(c *gc.C) {
	assertResolvedURL(c,
		"bar/baz",
		"http://example.com/foo/",
		"http://example.com/foo/bar/baz",
	)

	assertResolvedURL(c,
		"./bar/baz",
		"http://example.com/foo/secret/",
		"http://example.com/foo/secret/bar/baz",
	)

	assertResolvedURL(c,
		"./bar/baz",
		// Lack of a trailing slash means we should treat "secret" as a
		// file and the path is relative to its parent path.
		"http://example.com/foo/secret",
		"http://example.com/foo/bar/baz",
	)

	assertResolvedURL(c,
		"../../bar/baz",
		"http://example.com/foo/secret/",
		"http://example.com/bar/baz",
	)
}

func (s *ResolveURLTestSuite) TestResolveDoubleSlashURL(c *gc.C) {
	assertResolvedURL(c,
		"//www.somewhere.com/foo",
		"http://example.com/bar/secret/",
		"http://www.somewhere.com/foo",
	)

	assertResolvedURL(c,
		"//www.somewhere.com/foo",
		"https://example.com/bar/secret/",
		"https://www.somewhere.com/foo",
	)
}

func assertResolvedURL(c *gc.C, target, base, exp string) {
	relBase, err := url.Parse(base)
	c.Assert(err, gc.IsNil)

	var gotURL string
	if got := resolveURL(relBase, target); got != nil {
		gotURL = got.String()
	}
	c.Assert(gotURL, gc.Equals, exp)
}

type LinkExtractorTestSuite struct {
	privNetDetector *mocks.MockPrivateNetworkDetector
}

func (s *LinkExtractorTestSuite) TestLinkExtractor(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	s.privNetDetector = mocks.NewMockPrivateNetworkDetector(ctrl)

	exp := s.privNetDetector.EXPECT()
	exp.IsPrivate("example.com").Return(false, nil).Times(2)
	exp.IsPrivate("foo.com").Return(false, nil).Times(2)

	content := `
<html>
<body>
<a href="https://example.com"/>
<a href="//foo.com"></a>
<a href="/absolute/link"></a>

<!-- the following link should be included in the no follow link list -->
<a href="./local" rel="nofollow"></a>

<!-- duplicates, even with fragments should be skipped -->
<a href="https://example.com#important"/>
<a href="//foo.com"></a>
<a href="/absolute/link#some-anchor"></a>

</body>
</html>
`
	s.assertExtractedLinks(c, "http://test.com", content, []string{
		"https://example.com",
		"http://foo.com",
		"http://test.com/absolute/link",
	}, []string{
		"http://test.com/local",
	})
}

func (s *LinkExtractorTestSuite) TestLinkExtractorWithNonHTTPLinks(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	s.privNetDetector = mocks.NewMockPrivateNetworkDetector(ctrl)

	content := `
<html>
<body>
<a href="ftp://example.com">An FTP site</a>
</body>
</html>
`
	s.assertExtractedLinks(c, "http://test.com", content, nil, nil)
}

func (s *LinkExtractorTestSuite) TestLinkExtractorWithRelativeLinksToFile(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	content := `
<html>
<body>
<a href="./foo.html">link to foo</a>
<a href="../private/data.html">login required</a>
</body>
</html>
`
	s.assertExtractedLinks(c, "https://test.com/content/intro.html", content, []string{
		"https://test.com/content/foo.html",
		"https://test.com/private/data.html",
	}, nil)
}

func (s *LinkExtractorTestSuite) TestLinkExtractorWithRelativeLinksToDir(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	content := `
<html>
<body>
<a href="./foo.html">link to foo</a>
<a href="../private/data.html">login required</a>
</body>
</html>
`
	s.assertExtractedLinks(c, "https://test.com/content/", content, []string{
		"https://test.com/content/foo.html",
		"https://test.com/private/data.html",
	}, nil)
}

func (s *LinkExtractorTestSuite) TestLinkExtractorWithBaseTag(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	content := `
<html>
<head>
<base href="https://test.com/base/"/>
</head>
<body>
<a href="./foo.html">link to foo</a>
<a href="../private/data.html">login required</a>
</body>
</html>
`
	s.assertExtractedLinks(c, "https://test.com/content/", content, []string{
		"https://test.com/base/foo.html",
		"https://test.com/private/data.html",
	}, nil)
}

func (s *LinkExtractorTestSuite) TestLinkExtractorWithPrivateNetworkLinks(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	s.privNetDetector = mocks.NewMockPrivateNetworkDetector(ctrl)

	exp := s.privNetDetector.EXPECT()
	exp.IsPrivate("example.com").Return(false, nil)
	exp.IsPrivate("169.254.169.254").Return(true, nil)

	content := `
<html>
<body>
<a href="https://example.com">link to foo</a>
<a href="http://169.254.169.254/api/credentials"/>
</body>
</html>
`
	s.assertExtractedLinks(c, "https://test.com/content/", content, []string{
		"https://example.com",
	}, nil)
}

func (s *LinkExtractorTestSuite) assertExtractedLinks(c *gc.C, url, content string, expLinks []string, expNoFollowLinks []string) {
	p := &crawlerPayload{URL: url}
	_, err := p.RawContent.WriteString(content)
	c.Assert(err, gc.IsNil)

	le := newLinkExtractor(s.privNetDetector)
	ret, err := le.Process(context.TODO(), p)
	c.Assert(err, gc.IsNil)
	c.Assert(ret, gc.DeepEquals, p)

	sort.Strings(expLinks)
	sort.Strings(p.Links)
	c.Assert(p.Links, gc.DeepEquals, expLinks)
	c.Assert(p.NoFollowLinks, gc.DeepEquals, expNoFollowLinks)
}
