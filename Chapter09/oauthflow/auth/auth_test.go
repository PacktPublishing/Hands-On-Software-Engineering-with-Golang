package auth_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/oauthflow/auth"
	"golang.org/x/oauth2"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(AuthHandlerTestSuite))

func Test(t *testing.T) {
	// Run all gocheck test-suites
	gc.TestingT(t)
}

type AuthHandlerTestSuite struct {
	srv        *httptest.Server
	srvHandler http.HandlerFunc

	authHandler *auth.Flow
}

func (s *AuthHandlerTestSuite) SetUpTest(c *gc.C) {
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.srvHandler != nil {
			s.srvHandler(w, r)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))

	// Bind to any available port for the OAuth redirect callbacks
	ah, err := auth.NewOAuthFlow(oauth2.Config{
		Endpoint: oauth2.Endpoint{
			AuthURL:  s.srv.URL + "/oauth/authorize",
			TokenURL: s.srv.URL + "/oauth/access_token",
		},
	}, "localhost:0", "")
	c.Assert(err, gc.IsNil)

	s.authHandler = ah
}

func (s *AuthHandlerTestSuite) TearDownTest(c *gc.C) {
	c.Assert(s.authHandler.Close(), gc.IsNil)
	s.srv.Close()
}

func (s *AuthHandlerTestSuite) TestThreeLegFlow(c *gc.C) {
	s.srvHandler = makeOAuthServerHandler(c, func(nonce string) string {
		return nonce
	})

	authURL, resCh, err := s.authHandler.Authenticate()
	c.Logf("simulating user visiting authorization URL: %q", authURL)
	c.Assert(err, gc.IsNil)

	go func() {
		res, err := http.Get(authURL)
		if err == nil {
			_ = res.Body.Close()
		}
	}()

	var authRes auth.Result
	select {
	case <-time.After(5 * time.Second):
		c.Fatal("timeout waiting for auth response")
	case authRes = <-resCh:
	}

	_, err = authRes.Client(context.TODO())
	c.Assert(err, gc.IsNil)
}

func (s *AuthHandlerTestSuite) TestRedirectWithUnexpectedNonce(c *gc.C) {
	s.srvHandler = makeOAuthServerHandler(c, func(nonce string) string {
		return "this-is-not-the-nonce-you-are-looking-for"
	})

	authURL, resCh, err := s.authHandler.Authenticate()
	c.Logf("simulating user visiting authorization URL: %q", authURL)
	c.Assert(err, gc.IsNil)

	go func() {
		res, err := http.Get(authURL)
		if err == nil {
			_ = res.Body.Close()
		}
	}()

	select {
	case <-time.After(time.Second):
	case <-resCh:
		c.Fatal("expected auth attempt to time out")
	}
}

func makeOAuthServerHandler(c *gc.C, nonceMutatorFn func(string) string) http.HandlerFunc {
	validToken := `
{
  "access_token":"access-token",
  "token_type":"bearer",
  "expires_in":3600
}`

	return func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.ParseForm(), gc.IsNil)

		reqURL := r.URL.String()
		c.Logf("[oauth server] received request: %q", reqURL)
		switch {
		case strings.Contains(reqURL, "/oauth/authorize"):
			redirURI, err := url.Parse(r.FormValue("redirect_uri"))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			qs := redirURI.Query()
			qs.Set("state", nonceMutatorFn(r.FormValue("state")))
			qs.Set("code", "token-code")
			redirURI.RawQuery = qs.Encode()
			http.Redirect(w, r, redirURI.String(), http.StatusFound)
			c.Logf("[oauth server] responding with redirect to: %q", redirURI.String())
		case strings.Contains(reqURL, "/oauth/access_token"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, validToken)
			c.Logf("[oauth server] responding with oauth token: %s", validToken)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}
}
