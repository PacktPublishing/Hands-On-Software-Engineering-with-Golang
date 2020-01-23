package dialer

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"net"
	"time"

	"github.com/juju/clock"
)

// ErrMaxRetriesExceeded is returned by the retrying dialer to indicate that
// a connection was not possible after the configured number of max attempts.
var ErrMaxRetriesExceeded = errors.New("max number of dial retries exceeded")

const (
	maxJitter  = 1000 * time.Millisecond
	maxBackoff = 32 * time.Second
)

// DialFunc describes any function that can be invoked to dial a remote host.
type DialFunc func(network, address string) (net.Conn, error)

// RetryingDialer wraps a DialFunc with an exponential back-off retry mechanism.
type RetryingDialer struct {
	ctx         context.Context
	clk         clock.Clock
	dialFunc    DialFunc
	maxAttempts int
}

// NewRetryingDialer returns a new dialer that wraps dialFunc with a retry
// layer that waits between attempts using an exponential back-off algorithm.
// Dial attempts will be aborted if the attempts exceed maxAttempts or the
// provided context is cancelled.
func NewRetryingDialer(ctx context.Context, clk clock.Clock, dialFunc DialFunc, maxAttempts int) *RetryingDialer {
	if maxAttempts > 31 {
		panic("maxAttempts cannot exceed 31")
	}

	return &RetryingDialer{
		ctx:         ctx,
		clk:         clk,
		dialFunc:    dialFunc,
		maxAttempts: maxAttempts,
	}
}

// Dial a remote host.
func (d *RetryingDialer) Dial(network, address string) (conn net.Conn, err error) {
	for attempt := 1; attempt <= d.maxAttempts; attempt++ {
		if conn, err = d.dialFunc(network, address); err == nil {
			return conn, nil
		}

		log.Printf("dial %q: attempt %d failed; retrying after %s", address, attempt, expBackoff(attempt))
		select {
		case <-d.clk.After(expBackoff(attempt)): // Try again
		case <-d.ctx.Done():
			return nil, d.ctx.Err()
		}
	}
	return nil, ErrMaxRetriesExceeded
}

// expBackoff returns the time we need to wait after the i_th attempt. It is
// calculated using the following formula:
//
// min(pow(4ms, attempt) + jitter, maxBackoff)
//
// A jitter term is added to spread retries so as to avoid issues like the
// thundering herd problem.
func expBackoff(attempt int) time.Duration {
	jitter := time.Millisecond * time.Duration(rand.Int63n(maxJitter.Nanoseconds()/1e6))
	backOff := time.Duration(2<<uint64(attempt))*time.Millisecond + jitter
	if backOff < maxBackoff {
		return backOff
	}

	return maxBackoff
}
