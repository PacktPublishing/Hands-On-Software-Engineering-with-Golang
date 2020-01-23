package tracer

import (
	"io"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

// Pool keeps track of instantiated tracers and provides a helper method for
// closing all of them at once.
var Pool = new(pool)

type pool struct {
	mu            sync.Mutex
	tracerClosers []io.Closer
}

// Close all tracer instances currently tracked by the pool
func (p *pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var err error
	for _, closer := range p.tracerClosers {
		if cErr := closer.Close(); cErr != nil {
			err = multierror.Append(err, cErr)
		}
	}

	p.tracerClosers = nil
	return err
}

// MustGetTracer obtains and returns a new Jaeger tracer or panics if any error
// occurs.
func MustGetTracer(serviceName string) opentracing.Tracer {
	tracer, err := GetTracer(serviceName)
	if err != nil {
		panic(err)
	}
	return tracer
}

// GetTracer obtains and returns a new Jaeger tracer. To ensure that none of
// the traced spans are lost, callers must call Close on the exported Pool
// object before their application exits.
//
// Note: this method will force Jaeger to capture every emitted span to make
// testing easier.
func GetTracer(serviceName string) (opentracing.Tracer, error) {
	// Setup jaeger from envvars
	cfg, err := jaegercfg.FromEnv()
	if err != nil {
		return nil, err
	}

	// Sample every span (testing only)
	cfg.Sampler = &jaegercfg.SamplerConfig{
		Type:  jaeger.SamplerTypeConst,
		Param: 1,
	}

	cfg.ServiceName = serviceName

	tracer, closer, err := cfg.NewTracer()
	if err != nil {
		return nil, err
	}

	Pool.mu.Lock()
	Pool.tracerClosers = append(Pool.tracerClosers, closer)
	Pool.mu.Unlock()
	return tracer, nil
}
