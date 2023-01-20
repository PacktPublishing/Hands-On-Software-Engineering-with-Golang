package pipeline

import (
	"context"
	"sync"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/xerrors"
)

var _ StageParams = (*workerParams)(nil)

type workerParams struct {
	stage int

	// Channels for the worker's input, output and errors.
	inCh  <-chan Payload
	outCh chan<- Payload
	errCh chan<- error
}

func (p *workerParams) StageIndex() int        { return p.stage }
func (p *workerParams) Input() <-chan Payload  { return p.inCh }
func (p *workerParams) Output() chan<- Payload { return p.outCh }
func (p *workerParams) Error() chan<- error    { return p.errCh }

// Pipeline implements a modular, multi-stage pipeline. Each pipeline is
// constructed out of an input source, an output sink and zero or more
// processing stages.
type Pipeline struct {
	stages []StageRunner
}

// New returns a new pipeline instance where input payloads will traverse each
// one of the specified stages.
func New(stages ...StageRunner) *Pipeline {
	return &Pipeline{
		stages: stages,
	}
}

// Process reads the contents of the specified source, sends them through the
// various stages of the pipeline and directs the results to the specified sink
// and returns back any errors that may have occurred.
//
// Calls to Process block until:
//   - all data from the source has been processed OR
//   - an error occurs OR
//   - the supplied context expires
//
// It is safe to call Process concurrently with different sources and sinks.
func (p *Pipeline) Process(ctx context.Context, source Source, sink Sink) error {
	var wg sync.WaitGroup
	pCtx, ctxCancelFn := context.WithCancel(ctx)

	// Allocate channels for wiring together the source, the pipeline stages
	// and the output sink. The output of the i_th stage is used as an input
	// for the i+1_th stage. We need to allocate one extra channel than the
	// number of stages so we can also wire the source/sink.
	stageCh := make([]chan Payload, len(p.stages)+1)
	errCh := make(chan error, len(p.stages)+2)
	for i := 0; i < len(stageCh); i++ {
		stageCh[i] = make(chan Payload)
	}

	// Start a worker for each stage
	for i := 0; i < len(p.stages); i++ {
		wg.Add(1)
		go func(stageIndex int) {
			p.stages[stageIndex].Run(pCtx, &workerParams{
				stage: stageIndex,
				inCh:  stageCh[stageIndex],
				outCh: stageCh[stageIndex+1],
				errCh: errCh,
			})

			// Signal next stage that no more data is available.
			close(stageCh[stageIndex+1])
			wg.Done()
		}(i)
	}

	// Start source and sink workers
	wg.Add(2)
	go func() {
		sourceWorker(pCtx, source, stageCh[0], errCh)

		// Signal next stage that no more data is available.
		close(stageCh[0])
		wg.Done()
	}()

	go func() {
		sinkWorker(pCtx, sink, stageCh[len(stageCh)-1], errCh)
		wg.Done()
	}()

	// Close the error channel once all workers exit.
	go func() {
		wg.Wait()
		close(errCh)
		ctxCancelFn()
	}()

	// Collect any emitted errors and wrap them in a multi-error.
	var err error
	for pErr := range errCh {
		err = multierror.Append(err, pErr)
		ctxCancelFn()
	}
	return err
}

// sourceWorker implements a worker that reads Payload instances from a Source
// and pushes them to an output channel that is used as input for the first
// stage of the pipeline.
func sourceWorker(ctx context.Context, source Source, outCh chan<- Payload, errCh chan<- error) {
	for source.Next(ctx) {
		payload := source.Payload()
		select {
		case outCh <- payload:
		case <-ctx.Done():
			// Asked to shutdown
			return
		}
	}

	// Check for errors
	if err := source.Error(); err != nil {
		wrappedErr := xerrors.Errorf("pipeline source: %w", err)
		maybeEmitError(wrappedErr, errCh)
	}
}

// sinkWorker implements a worker that reads Payload instances from an input
// channel (the output of the last pipeline stage) and passes them to the
// provided sink.
func sinkWorker(ctx context.Context, sink Sink, inCh <-chan Payload, errCh chan<- error) {
	for {
		select {
		case payload, ok := <-inCh:
			if !ok {
				return
			}

			if err := sink.Consume(ctx, payload); err != nil {
				wrappedErr := xerrors.Errorf("pipeline sink: %w", err)
				maybeEmitError(wrappedErr, errCh)
				return
			}
			payload.MarkAsProcessed()
		case <-ctx.Done():
			// Asked to shutdown
			return
		}
	}
}

// maybeEmitError attempts to queue err to a buffered error channel. If the
// channel is full, the error is dropped.
func maybeEmitError(err error, errCh chan<- error) {
	select {
	case errCh <- err: // error emitted.
	default: // error channel is full with other errors.
	}
}
