package service

import (
	"context"
	"sync"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter11/tracing/proto"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter11/tracing/tracer"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
)

// Aggregator collects and returns price quotes from a set of downstream
// providers.
type Aggregator struct {
	vendorID      string
	providerAddrs []string
	clients       []proto.QuoteServiceClient
}

// NewAggregator returns a new Aggregator instance that queries the providers
// at providerAddrs and returns back the results.
func NewAggregator(vendorID string, providerAddrs []string) *Aggregator {
	return &Aggregator{
		vendorID:      vendorID,
		providerAddrs: providerAddrs,
	}
}

// GetQuote implements proto.QuoteServiceServer.
func (a *Aggregator) GetQuote(ctx context.Context, req *proto.QuotesRequest) (*proto.QuotesResponse, error) {
	// Run requests in parallel and aggregate results
	aggRes := new(proto.QuotesResponse)
	for quotes := range a.sendRequests(ctx, req) {
		aggRes.Quotes = append(aggRes.Quotes, quotes...)
	}
	return aggRes, nil
}

// sendRequests queries all downstream providers in parallel and returns a
// channel for reading the quote results. The channel will be closed when all
// provider requests have returned.
func (a *Aggregator) sendRequests(ctx context.Context, req *proto.QuotesRequest) <-chan []*proto.Quote {
	var wg sync.WaitGroup
	wg.Add(len(a.clients))
	resCh := make(chan []*proto.Quote, len(a.clients))

	for _, client := range a.clients {
		go func(client proto.QuoteServiceClient) {
			defer wg.Done()
			if res, err := client.GetQuote(ctx, req); err == nil {
				resCh <- res.Quotes
			}
		}(client)
	}

	go func() {
		wg.Wait()
		close(resCh)
	}()

	return resCh
}

// Serve listens for incoming connections on a random open port until ctx
// expires. It returns back the address that the server is listening on.
func (a *Aggregator) Serve(ctx context.Context) (string, error) {
	tracer := tracer.MustGetTracer(a.vendorID)
	tracerClientOpt := grpc.WithUnaryInterceptor(otgrpc.OpenTracingClientInterceptor(tracer))

	for _, addr := range a.providerAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure(), tracerClientOpt)
		if err != nil {
			return "", xerrors.Errorf("dialing provider at %s: %w", addr, err)
		}
		a.clients = append(a.clients, proto.NewQuoteServiceClient(conn))
	}

	return doServe(ctx, a, tracer)
}
