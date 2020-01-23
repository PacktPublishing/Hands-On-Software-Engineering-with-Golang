package dialer_test

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/pincert/dialer"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(DialerTestSuite))

func Test(t *testing.T) {
	// Run all gocheck test-suites
	gc.TestingT(t)
}

type DialerTestSuite struct {
	srv *httptest.Server
}

func (s *DialerTestSuite) SetUpTest(c *gc.C) {
	s.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, "success")
	}))
}

func (s *DialerTestSuite) TearDownTest(c *gc.C) {
	s.srv.Close()
}

func (s *DialerTestSuite) TestValidCertificateFingerprint(c *gc.C) {
	srvCert := s.srv.Certificate()
	certPool := x509.NewCertPool()
	certPool.AddCert(srvCert)
	certDer, err := x509.MarshalPKIXPublicKey(srvCert.PublicKey)
	c.Assert(err, gc.IsNil)
	fingerprint := sha256.Sum256(certDer)

	client := &http.Client{
		Transport: &http.Transport{
			DialTLS: dialer.WithPinnedCertVerification(
				fingerprint[:],
				// Use the same cert pool as the test server so
				// the tls dialer can verify the presented cert.
				&tls.Config{
					RootCAs: certPool,
				},
			),
		},
	}

	res, err := client.Get(s.srv.URL)
	c.Assert(err, gc.IsNil)
	body, err := ioutil.ReadAll(res.Body)
	_ = res.Body.Close()
	c.Assert(err, gc.IsNil)

	c.Assert(res.StatusCode, gc.Equals, http.StatusOK)
	c.Assert(string(body), gc.Equals, "success")
}

func (s *DialerTestSuite) TestInvalidCertificateFingerprint(c *gc.C) {
	srvCert := s.srv.Certificate()
	certPool := x509.NewCertPool()
	certPool.AddCert(srvCert)

	client := &http.Client{
		Transport: &http.Transport{
			DialTLS: dialer.WithPinnedCertVerification(
				[]byte("bogus-fingerprint"),
				// Use the same cert pool as the test server so
				// the tls dialer can verify the presented cert.
				&tls.Config{
					RootCAs: certPool,
				},
			),
		},
	}

	_, err := client.Get(s.srv.URL)
	c.Assert(err, gc.ErrorMatches, ".*remote server presented a certificate which does not match the provided fingerprint.*")
}
