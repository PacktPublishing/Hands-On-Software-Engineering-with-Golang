package dialer_test

import (
	"context"
	"log"
	"net"
	"testing"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/ch04/dialer"
	"github.com/jonboulle/clockwork"
)

func TestRetryingDialerWithRealClock(t *testing.T) {
	log.SetFlags(0)

	// Dial a random local port that nothing is listening on.
	clk := clockwork.NewRealClock()
	d := dialer.NewRetryingDialer(context.Background(), clk, net.Dial, 10)
	_, err := d.Dial("tcp", "127.0.0.1:65000")
	if err != dialer.ErrMaxRetriesExceeded {
		t.Fatalf("expected to get ErrMaxRetriesExceeded; got %v", err)
	}
}

func TestRetryingDialerWithFakeClock(t *testing.T) {
	log.SetFlags(0)

	doneCh := make(chan struct{})
	defer close(doneCh)
	clk := clockwork.NewFakeClock()
	go func() {
		for {
			select {
			case <-doneCh: // test completed; exit go-routine
				return
			default:
				clk.Advance(1 * time.Minute)
			}
		}
	}()

	// Dial a random local port that nothing is listening on.
	d := dialer.NewRetryingDialer(context.Background(), clk, net.Dial, 10)
	_, err := d.Dial("tcp", "127.0.0.1:65000")
	if err != dialer.ErrMaxRetriesExceeded {
		t.Fatalf("expected to get ErrMaxRetriesExceeded; got %v", err)
	}
}
