package pipeline_test

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter07/pipeline"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(StageTestSuite))

type StageTestSuite struct{}

func (s StageTestSuite) TestFIFO(c *gc.C) {
	stages := make([]pipeline.StageRunner, 10)
	for i := 0; i < len(stages); i++ {
		stages[i] = pipeline.FIFO(makePassthroughProcessor())
	}

	src := &sourceStub{data: stringPayloads(3)}
	sink := new(sinkStub)

	p := pipeline.New(stages...)
	err := p.Process(context.TODO(), src, sink)
	c.Assert(err, gc.IsNil)
	c.Assert(sink.data, gc.DeepEquals, src.data)
	assertAllProcessed(c, src.data)
}

func (s StageTestSuite) TestFixedWorkerPool(c *gc.C) {
	numWorkers := 10
	syncCh := make(chan struct{})
	rendezvousCh := make(chan struct{})

	proc := pipeline.ProcessorFunc(func(_ context.Context, _ pipeline.Payload) (pipeline.Payload, error) {
		// Signal that we have reached the sync point and wait for the
		// green light to proceed by the test code.
		syncCh <- struct{}{}
		<-rendezvousCh
		return nil, nil
	})

	src := &sourceStub{data: stringPayloads(numWorkers)}

	p := pipeline.New(pipeline.FixedWorkerPool(proc, numWorkers))
	doneCh := make(chan struct{})
	go func() {
		err := p.Process(context.TODO(), src, nil)
		c.Assert(err, gc.IsNil)
		close(doneCh)
	}()

	// Wait for all workers to reach sync point. This means that each input
	// from the source is currently handled by a worker in parallel.
	for i := 0; i < numWorkers; i++ {
		select {
		case <-syncCh:
		case <-time.After(10 * time.Second):
			c.Fatalf("timed out waiting for worker %d to reach sync point", i)
		}
	}

	// Allow workers to proceed and wait for the pipeline to complete.
	close(rendezvousCh)
	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		c.Fatal("timed out waiting for pipeline to complete")
	}
}

func (s StageTestSuite) TestDynamicWorkerPool(c *gc.C) {
	numWorkers := 5
	syncCh := make(chan struct{}, numWorkers)
	rendezvousCh := make(chan struct{})

	proc := pipeline.ProcessorFunc(func(_ context.Context, _ pipeline.Payload) (pipeline.Payload, error) {
		// Signal that we have reached the sync point and wait for the
		// green light to proceed by the test code.
		syncCh <- struct{}{}
		<-rendezvousCh
		return nil, nil
	})

	src := &sourceStub{data: stringPayloads(numWorkers * 2)}

	p := pipeline.New(pipeline.DynamicWorkerPool(proc, numWorkers))
	doneCh := make(chan struct{})
	go func() {
		err := p.Process(context.TODO(), src, nil)
		c.Assert(err, gc.IsNil)
		close(doneCh)
	}()

	// Wait for all workers to reach sync point. This means that the pool
	// has scaled up to the max number of workers.
	for i := 0; i < numWorkers; i++ {
		select {
		case <-syncCh:
		case <-time.After(10 * time.Second):
			c.Fatalf("timed out waiting for worker %d to reach sync point", i)
		}
	}

	// Allow workers to proceed and process the next batch of records
	close(rendezvousCh)
	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		c.Fatal("timed out waiting for pipeline to complete")
	}

	assertAllProcessed(c, src.data)
}

func (s StageTestSuite) TestBroadcast(c *gc.C) {
	numProcs := 3
	procs := make([]pipeline.Processor, numProcs)
	for i := 0; i < numProcs; i++ {
		procs[i] = makeMutatingProcessor(i)
	}

	src := &sourceStub{data: stringPayloads(1)}
	sink := new(sinkStub)

	p := pipeline.New(pipeline.Broadcast(procs...))
	err := p.Process(context.TODO(), src, sink)
	c.Assert(err, gc.IsNil)

	expData := []pipeline.Payload{
		&stringPayload{val: "0_0", processed: true},
		&stringPayload{val: "0_1", processed: true},
		&stringPayload{val: "0_2", processed: true},
	}
	assertAllProcessed(c, src.data)

	// Processors run as go-routines so outputs will be shuffled. We need
	// to sort them first so we can check for equality.
	sort.Slice(expData, func(i, j int) bool {
		return expData[i].(*stringPayload).val < expData[j].(*stringPayload).val
	})
	sort.Slice(sink.data, func(i, j int) bool {
		return sink.data[i].(*stringPayload).val < sink.data[j].(*stringPayload).val
	})

	c.Assert(sink.data, gc.DeepEquals, expData)
}

func makeMutatingProcessor(index int) pipeline.Processor {
	return pipeline.ProcessorFunc(func(_ context.Context, p pipeline.Payload) (pipeline.Payload, error) {
		// Mutate payload to check that each processor got a copy
		sp := p.(*stringPayload)
		sp.val = fmt.Sprintf("%s_%d", sp.val, index)
		return p, nil
	})
}

func makePassthroughProcessor() pipeline.Processor {
	return pipeline.ProcessorFunc(func(_ context.Context, p pipeline.Payload) (pipeline.Payload, error) {
		return p, nil
	})
}
