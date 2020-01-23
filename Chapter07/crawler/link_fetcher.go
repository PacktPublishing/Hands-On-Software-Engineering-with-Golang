package crawler

import (
	"context"
	"io"
	"net/url"
	"strings"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter07/pipeline"
)

var _ pipeline.Processor = (*linkFetcher)(nil)

type linkFetcher struct {
	urlGetter   URLGetter
	netDetector PrivateNetworkDetector
}

func newLinkFetcher(urlGetter URLGetter, netDetector PrivateNetworkDetector) *linkFetcher {
	return &linkFetcher{
		urlGetter:   urlGetter,
		netDetector: netDetector,
	}
}

func (lf *linkFetcher) Process(ctx context.Context, p pipeline.Payload) (pipeline.Payload, error) {
	payload := p.(*crawlerPayload)

	// Skip URLs that point to files that cannot contain html content.
	if exclusionRegex.MatchString(payload.URL) {
		return nil, nil
	}

	// Never crawl links in private networks (e.g. link-local addresses).
	// This is a security risk!
	if isPrivate, err := lf.isPrivate(payload.URL); err != nil || isPrivate {
		return nil, nil
	}

	res, err := lf.urlGetter.Get(payload.URL)
	if err != nil {
		return nil, nil
	}
	_, err = io.Copy(&payload.RawContent, res.Body)
	_ = res.Body.Close()
	if err != nil {
		return nil, err
	}

	// Skip payloads for invalid http status codes.
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return nil, nil
	}

	// Skip payloads for non-html payloads
	if contentType := res.Header.Get("Content-Type"); !strings.Contains(contentType, "html") {
		return nil, nil
	}

	return payload, nil
}

func (lf *linkFetcher) isPrivate(URL string) (bool, error) {
	u, err := url.Parse(URL)
	if err != nil {
		return false, err
	}
	return lf.netDetector.IsPrivate(u.Hostname())
}
