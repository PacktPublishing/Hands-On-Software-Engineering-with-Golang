package crawler

import (
	"context"

	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(ContentExtractorTestSuite))

type ContentExtractorTestSuite struct{}

func (s *ContentExtractorTestSuite) TestContentExtractor(c *gc.C) {
	content := `<div>Some<span> content</span> rock &amp; roll</div>
<buttton>Search</button>
`
	assertExtractedContent(c, content, "", `Some content rock & roll Search`)
}

func (s *ContentExtractorTestSuite) TestContentExtractorWithTitle(c *gc.C) {
	content := `<html>
<head>
<title>Test title</title>
</head>
<body>
<div>Some<span> content</span></div>
</body>
</html>
`
	assertExtractedContent(c, content, "Test title", `Some content`)
}

func assertExtractedContent(c *gc.C, content, expTitle, expText string) {
	p := new(crawlerPayload)
	_, err := p.RawContent.WriteString(content)
	c.Assert(err, gc.IsNil)

	ret, err := newTextExtractor().Process(context.TODO(), p)
	c.Assert(err, gc.IsNil)
	c.Assert(ret, gc.DeepEquals, p)

	c.Assert(p.Title, gc.Equals, expTitle)
	c.Assert(p.TextContent, gc.Equals, expText)
}
