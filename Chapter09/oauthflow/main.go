package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/oauthflow/auth"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/github"
	"golang.org/x/xerrors"
)

func main() {
	if err := runOAuthFlow(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
}

func runOAuthFlow() error {
	cli, err := createOAuthClient(oauth2.Config{
		// The following credentials are hardcoded for demonstration
		// purposes only! Typically, these would be provided via
		// a mechanism like command line flags or envvars.
		ClientID:     "af4f362d92080f398cae",
		ClientSecret: "9c37ba8ea376bdffe033e25116208d3d15ebf53d",
		Scopes:       []string{"read:user"},
		Endpoint:     github.Endpoint,
	})
	if err != nil {
		return err
	}

	return printUserLoginName(cli)
}

// createOAuthClient executes the three-legged OAuth flow and returns an
// http.Client instance that can perform authenticated requests to GitHub's API
// endpoints.
func createOAuthClient(cfg oauth2.Config) (*http.Client, error) {
	authHandler, err := auth.NewOAuthFlow(cfg, "127.0.0.1:8080", "")
	if err != nil {
		return nil, err
	}
	defer func() { _ = authHandler.Close() }()

	authURL, resCh, err := authHandler.Authenticate()
	if err != nil {
		return nil, err
	}
	fmt.Printf("To run this example, please visit the following URL with your web browser to authorize:\n%s\n\n", authURL)

	var authRes auth.Result
	select {
	case <-time.After(60 * time.Second):
		return nil, xerrors.Errorf("timed out waiting for authorization")
	case authRes = <-resCh:
	}

	return authRes.Client(context.Background())
}

// printUserLoginName invokes the /user API endpoint using the provided
// http.Client instance and prints out the authenticated user's GitHub login
// name.
func printUserLoginName(cli *http.Client) error {
	// The obtained client will automatically inject the OAuth token into
	// outgoing requests and refresh it when it expires.
	res, err := cli.Get("https://api.github.com/user")
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()

	parsedRes := struct {
		Login string `json:"login"`
	}{}
	if err := json.NewDecoder(res.Body).Decode(&parsedRes); err != nil {
		return xerrors.Errorf("unable to parse API response: %w", err)
	}

	fmt.Printf("Your GitHub user name is %q\n", parsedRes.Login)
	return nil
}
