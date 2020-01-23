package main

import (
	"context"
	"fmt"
	"os"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter11/tracing/service"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter11/tracing/tracer"
)

func main() {
	fmt.Println(`
This demo assumes that you are running a dockerized version of Jaeger.
If not, start one by running the following command and try running 
this example again:

  docker run -d --name jaeger \
    -p 6831:6831/udp \
    -p 16686:16686 \
    jaegertracing/all-in-one:1.14

You can then access the Jaeger UI at http://localhost:16686`)
	ctx, cancelFn := context.WithCancel(context.Background())
	defer func() {
		cancelFn()
		_ = tracer.Pool.Close()
	}()

	if err := runMain(ctx); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "ERROR: %v", err)
		os.Exit(1)
	}
}

func runMain(ctx context.Context) error {
	// Simulate a deployed micro-service setup and get back a gateway service
	// for performing traceable queries
	gw, err := deployServices(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = gw.Close() }()

	// Run an example query to capture a span
	res, err := gw.CollectQuotes(ctx, "example")
	if err != nil {
		return err
	}
	fmt.Printf("Collected the following quotes:\n")
	for k, v := range res {
		fmt.Printf("  %q: %3.2f\n", k, v)
	}

	return nil
}

func deployServices(ctx context.Context) (*service.Gateway, error) {
	// Spin up 3 provider servers and keep track of their addresses
	var err error
	providerAddrs := make([]string, 3)
	for i := 0; i < len(providerAddrs); i++ {
		provider := service.NewProvider(fmt.Sprintf("vendor-%d", i))
		if providerAddrs[i], err = provider.Serve(ctx); err != nil {
			return nil, err
		}
	}

	// Spin up an aggregator and connect it with providers 1 and 2.
	aggr1 := service.NewAggregator("aggr-1", providerAddrs[1:])
	aggr1Addr, err := aggr1.Serve(ctx)
	if err != nil {
		return nil, err
	}

	// Spin another aggregator and connect it with provider 0 and aggregator 1.
	aggr0 := service.NewAggregator("aggr-0", []string{providerAddrs[0], aggr1Addr})
	aggr0Addr, err := aggr0.Serve(ctx)
	if err != nil {
		return nil, err
	}

	// Finally, create an API gateway and connect it to aggregator 0.
	return service.NewGateway("api-gateway", aggr0Addr)
}
