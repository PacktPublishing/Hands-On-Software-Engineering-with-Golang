package main

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/pincert/dialer"
	"golang.org/x/xerrors"
)

func main() {
	if err := verifyCert(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
}

func verifyCert() error {
	flag.Parse()
	if flag.NArg() != 1 {
		return xerrors.Errorf(`unexpected number of arguments

Usage: pincert HTTPS_ADDRESS

Example HTTPS addresses you can try:
- https://google.com
- https://youtube.com (uses same TLS cert as google.com)
- https://microsoft.com
`)
	}

	// Most web-sites rotate their TLS certificates every few months.
	// Instead of hardcoding a value here, we obtain the fingerprint for
	// the current TLS certificate used by google.com and pin against that.
	fingerprint, err := getGoogleCertFingerprint()
	if err != nil {
		return err
	}
	fmt.Printf("The SHA256 fingerprint for the public key used by \"google.com\" is:\n%s\n", hex.Dump(fingerprint))

	client := &http.Client{
		Transport: &http.Transport{
			DialTLS: dialer.WithPinnedCertVerification(fingerprint, new(tls.Config)),
		},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	target := flag.Arg(0)
	fmt.Printf("Attempting to connect to %q using the above pinned public key...\n", target)
	res, err := client.Get(target)
	if err != nil {
		return xerrors.Errorf("connection attempt failed: %w", err)
	}
	_ = res.Body.Close()

	fmt.Printf("Successfully connected to %q using the pinned public key for google.com!\n", target)
	return nil
}

// getGoogleCertFingerprint connects to "google.com:443", looks for the peer
// certificate which is used to secure google.com (and other google-related
// sites) and returns back the SHA256 fingerprint of its public key.
func getGoogleCertFingerprint() ([]byte, error) {
	conn, err := tls.Dial("tcp", "google.com:443", nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()

	for _, cert := range conn.ConnectionState().PeerCertificates {
		// Scan SAN entries till we find the one for "google.com"
		for _, san := range cert.DNSNames {
			if san != "google.com" {
				continue
			}

			// Generate fingerprint
			certDER, err := x509.MarshalPKIXPublicKey(cert.PublicKey)
			if err != nil {
				return nil, xerrors.Errorf("unable to serialize certificate public key: %w", err)
			}
			fingerprint := sha256.Sum256(certDER)
			return fingerprint[:], nil
		}
	}

	return nil, errors.New(`unable to obtain PK fingerprint for "google.com"`)
}
