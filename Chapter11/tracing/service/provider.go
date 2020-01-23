package service

import (
	"context"
	"math/rand"
	"net"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter11/tracing/proto"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter11/tracing/tracer"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
)

// Provider simulates a vendor service that returns price quotes for an SKU.
type Provider struct {
	vendorID string
}

// NewProvider returns a new Provider instance with the specified vendor ID.
func NewProvider(vendorID string) *Provider {
	return &Provider{
		vendorID: vendorID,
	}
}

// GetQuote implements proto.QuoteServiceServer.
func (p *Provider) GetQuote(ctx context.Context, req *proto.QuotesRequest) (*proto.QuotesResponse, error) {
	return &proto.QuotesResponse{
		Quotes: []*proto.Quote{
			{
				Vendor: p.vendorID,
				Price:  100.0 * rand.Float64(),
			},
		},
	}, nil
}

// Serve listens for incoming connections on a random open port until ctx
// expires. It returns back the address that the server is listening on.
func (p *Provider) Serve(ctx context.Context) (string, error) {
	return doServe(ctx, p, tracer.MustGetTracer(p.vendorID))
}

func doServe(ctx context.Context, srv proto.QuoteServiceServer, tracer opentracing.Tracer) (string, error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return "", err
	}

	tracerOpt := grpc.UnaryInterceptor(otgrpc.OpenTracingServerInterceptor(tracer))
	gsrv := grpc.NewServer(tracerOpt)
	proto.RegisterQuoteServiceServer(gsrv, srv)

	go func() {
		go func() { _ = gsrv.Serve(l) }()
		<-ctx.Done()
		gsrv.Stop()
		_ = l.Close()
	}()

	return l.Addr().String(), nil
}
