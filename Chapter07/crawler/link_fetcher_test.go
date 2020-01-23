package crawler

import (
	"context"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter07/crawler/mocks"
	"github.com/golang/mock/gomock"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(LinkFetcherTestSuite))

type LinkFetcherTestSuite struct {
	urlGetter       *mocks.MockURLGetter
	privNetDetector *mocks.MockPrivateNetworkDetector
}

func (s *LinkFetcherTestSuite) SetUpTest(c *gc.C) {
}

func (s *LinkFetcherTestSuite) TestLinkFetcherWithExcludedExtension(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	s.urlGetter = mocks.NewMockURLGetter(ctrl)
	s.privNetDetector = mocks.NewMockPrivateNetworkDetector(ctrl)

	p := s.fetchLink(c, "http://example.com/foo.png")
	c.Assert(p, gc.IsNil)
}

func (s *LinkFetcherTestSuite) TestLinkFetcher(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	s.urlGetter = mocks.NewMockURLGetter(ctrl)
	s.privNetDetector = mocks.NewMockPrivateNetworkDetector(ctrl)

	s.privNetDetector.EXPECT().IsPrivate("example.com").Return(false, nil)
	s.urlGetter.EXPECT().Get("http://example.com/index.html").Return(
		makeResponse(200, "hello", "application/xhtml"),
		nil,
	)

	p := s.fetchLink(c, "http://example.com/index.html")
	c.Assert(p.RawContent.String(), gc.Equals, "hello")
}

func (s *LinkFetcherTestSuite) TestLinkFetcherForLinkWithPortNumber(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	s.urlGetter = mocks.NewMockURLGetter(ctrl)
	s.privNetDetector = mocks.NewMockPrivateNetworkDetector(ctrl)

	s.privNetDetector.EXPECT().IsPrivate("example.com").Return(false, nil)
	s.urlGetter.EXPECT().Get("http://example.com:1234").Return(
		makeResponse(200, "hello", "application/xhtml"),
		nil,
	)

	p := s.fetchLink(c, "http://example.com:1234")
	c.Assert(p.RawContent.String(), gc.Equals, "hello")
}

func (s *LinkFetcherTestSuite) TestLinkFetcherWithWrongStatusCode(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	s.urlGetter = mocks.NewMockURLGetter(ctrl)
	s.privNetDetector = mocks.NewMockPrivateNetworkDetector(ctrl)

	s.privNetDetector.EXPECT().IsPrivate("example.com").Return(false, nil)
	s.urlGetter.EXPECT().Get("http://example.com/index.html").Return(
		makeResponse(400, `{"error":"something went wrong"}`, "application/json"),
		nil,
	)

	p := s.fetchLink(c, "http://example.com/index.html")
	c.Assert(p, gc.IsNil)
}

func (s *LinkFetcherTestSuite) TestLinkFetcherWithNonHTMLContentType(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	s.urlGetter = mocks.NewMockURLGetter(ctrl)
	s.privNetDetector = mocks.NewMockPrivateNetworkDetector(ctrl)

	s.privNetDetector.EXPECT().IsPrivate("example.com").Return(false, nil)
	s.urlGetter.EXPECT().Get("http://example.com/list/products").Return(
		makeResponse(200, `["a", "b", "c"]`, "application/json"),
		nil,
	)

	p := s.fetchLink(c, "http://example.com/list/products")
	c.Assert(p, gc.IsNil)
}

func (s *LinkFetcherTestSuite) TestLinkFetcherWithLinkThatResolvesToPrivateNetwork(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()
	s.urlGetter = mocks.NewMockURLGetter(ctrl)
	s.privNetDetector = mocks.NewMockPrivateNetworkDetector(ctrl)

	s.privNetDetector.EXPECT().IsPrivate("169.254.169.254").Return(true, nil)

	p := s.fetchLink(c, "http://169.254.169.254/api/credentials")
	c.Assert(p, gc.IsNil)
}

func (s *LinkFetcherTestSuite) fetchLink(c *gc.C, url string) *crawlerPayload {
	p := &crawlerPayload{URL: url}
	out, err := newLinkFetcher(s.urlGetter, s.privNetDetector).Process(context.TODO(), p)
	c.Assert(err, gc.IsNil)
	if out != nil {
		c.Assert(out, gc.FitsTypeOf, p)
		return out.(*crawlerPayload)
	}

	return nil
}

func makeResponse(status int, body, contentType string) *http.Response {
	res := new(http.Response)
	res.Body = ioutil.NopCloser(strings.NewReader(body))
	res.StatusCode = status
	if contentType != "" {
		res.Header = make(http.Header)
		res.Header.Set("Content-Type", contentType)
	}
	return res
}
