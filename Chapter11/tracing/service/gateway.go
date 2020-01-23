package service

import (
	"context"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter11/tracing/proto"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter11/tracing/tracer"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
)

// Gateway simulates an API gateway that would handle requests from a front-end.
type Gateway struct {
	conn   *grpc.ClientConn
	client proto.QuoteServiceClient
}

// NewGateway returns a Gateway that retrieves quotes from a downstream
// aggregator listening at aggrAddr.
func NewGateway(serviceName, aggrAddr string) (*Gateway, error) {
	tracerOpt := grpc.WithUnaryInterceptor(
		otgrpc.OpenTracingClientInterceptor(tracer.MustGetTracer(serviceName)),
	)

	conn, err := grpc.Dial(aggrAddr, grpc.WithInsecure(), tracerOpt)
	if err != nil {
		return nil, err
	}

	return &Gateway{
		conn:   conn,
		client: proto.NewQuoteServiceClient(conn),
	}, nil
}

// Close shuts down the gateway and terminates any outgoing connections.
func (gw *Gateway) Close() error {
	return gw.conn.Close()
}

// CollectQuotes obtains a list of price quotes for SKU and returns them as a
// map where the key is the vendor name and the value is the price.
func (gw *Gateway) CollectQuotes(ctx context.Context, SKU string) (map[string]float64, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "CollectQuotes")
	defer span.Finish()

	res, err := gw.client.GetQuote(ctx, &proto.QuotesRequest{SKU: SKU})
	if err != nil {
		return nil, err
	}

	quoteMap := make(map[string]float64, len(res.Quotes))
	for _, quote := range res.Quotes {
		quoteMap[quote.Vendor] = quote.Price
	}
	return quoteMap, nil
}
