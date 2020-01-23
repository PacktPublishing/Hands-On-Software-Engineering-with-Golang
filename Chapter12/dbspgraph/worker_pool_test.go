package dbspgraph

import (
	"context"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/dbspgraph/mocks"
	"github.com/golang/mock/gomock"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(WorkerPoolTestSuite))

type WorkerPoolTestSuite struct {
	pool *workerPool
}

func (s *WorkerPoolTestSuite) SetUpTest(c *gc.C) {
	s.pool = newWorkerPool()
}

func (s *WorkerPoolTestSuite) TearDownTest(c *gc.C) {
	c.Assert(s.pool.Close(), gc.IsNil)
}

func (s *WorkerPoolTestSuite) TestDetectWorkerDisconnect(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	triggerCh := make(chan struct{})
	mockStream := mocks.NewMockJobQueue_JobStreamServer(ctrl)
	mockStream.EXPECT().Context().DoAndReturn(func() context.Context {
		// trigger test code and wait for another trigger to return
		// an expired context
		triggerCh <- struct{}{}
		<-triggerCh

		ctx, cancelFn := context.WithCancel(context.TODO())
		cancelFn()
		return ctx
	})

	worker := newRemoteWorkerStream(mockStream)
	s.pool.AddWorker(worker)

	// Wait for worker health check worker to start
	<-triggerCh
	s.pool.mu.Lock()
	workerCount := len(s.pool.connectedWorkers)
	s.pool.mu.Unlock()
	c.Assert(workerCount, gc.Equals, 1)

	// Signal the worker to exit and wait for the dropped connection to be
	// removed from the pool
	triggerCh <- struct{}{}
	s.pool.healthCheckWg.Wait()

	s.pool.mu.Lock()
	workerCount = len(s.pool.connectedWorkers)
	s.pool.mu.Unlock()
	c.Assert(workerCount, gc.Equals, 0)
}

func (s *WorkerPoolTestSuite) TestReserveWorkersBlocksUntilWorkersAppear(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	mockStream := mocks.NewMockJobQueue_JobStreamServer(ctrl)
	mockStream.EXPECT().Context().Return(context.TODO()).AnyTimes()

	go func() {
		// Add first worker
		s.pool.AddWorker(newRemoteWorkerStream(mockStream))

		// Add second worker; this should trigger a re-check and unblock
		// the pool main loop
		s.pool.AddWorker(newRemoteWorkerStream(mockStream))
	}()

	workers, err := s.pool.ReserveWorkers(context.TODO(), 2)
	c.Assert(err, gc.IsNil)
	c.Assert(workers, gc.HasLen, 2)
}

func (s *WorkerPoolTestSuite) TestReserveAbortWhenPoolCloses(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	c.Assert(s.pool.Close(), gc.IsNil)

	mockStream := mocks.NewMockJobQueue_JobStreamServer(ctrl)
	mockStream.EXPECT().Context().Return(context.TODO()).AnyTimes()
	worker := newRemoteWorkerStream(mockStream)
	s.pool.AddWorker(worker)

	_, err := s.pool.ReserveWorkers(context.TODO(), 2)
	c.Assert(err, gc.Equals, errMasterShuttingDown)

	select {
	case err := <-worker.sendErrCh:
		c.Assert(err, gc.Not(gc.IsNil), gc.Commentf("healthcheck worker did not emit an error message"))
		c.Assert(err, gc.Equals, errMasterShuttingDown)
	case <-time.After(10 * time.Second):
		c.Fatal("timeout waiting for healthcheck worker to send shutdown message")
	}
}

func (s *WorkerPoolTestSuite) TestReserveAbortWhenContextExpires(c *gc.C) {
	ctrl := gomock.NewController(c)
	defer ctrl.Finish()

	ctx, cancelFn := context.WithTimeout(context.TODO(), time.Millisecond)
	defer cancelFn()
	_, err := s.pool.ReserveWorkers(ctx, 2)
	c.Assert(err, gc.Equals, context.DeadlineExceeded)
}
