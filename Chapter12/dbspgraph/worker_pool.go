package dbspgraph

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

//go:generate mockgen -package mocks -destination mocks/mocks_api.go github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/dbspgraph/proto JobQueue_JobStreamServer

// workerPool stores remote worker connections until they get reserved for a job.
type workerPool struct {
	poolCtx        context.Context
	poolShutdownFn func()

	healthCheckWg        sync.WaitGroup
	poolMembersChangedCh chan struct{}

	mu                 sync.Mutex
	stopHealthChecksCh chan struct{}
	connectedWorkers   map[string]*remoteWorkerStream
}

// newWorkerPool creates a new worker pool instance.
func newWorkerPool() *workerPool {
	poolCtx, poolShutdownFn := context.WithCancel(context.Background())

	return &workerPool{
		poolCtx:              poolCtx,
		poolShutdownFn:       poolShutdownFn,
		poolMembersChangedCh: make(chan struct{}, 1),
		stopHealthChecksCh:   make(chan struct{}),
		connectedWorkers:     make(map[string]*remoteWorkerStream),
	}
}

// Close shuts down the pool and disconnects all connected workers.
func (p *workerPool) Close() error {
	p.poolShutdownFn()
	p.healthCheckWg.Wait()
	p.mu.Lock()
	p.connectedWorkers = make(map[string]*remoteWorkerStream)
	p.mu.Unlock()
	return nil
}

// AddWorker adds a new worker to the pool.
func (p *workerPool) AddWorker(worker *remoteWorkerStream) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Allocate a unique ID for the worker
	var workerID string
	for workerID = uuid.New().String(); p.connectedWorkers[workerID] != nil; workerID = uuid.New().String() {
	}

	// Start a health-checking go-routine to detect if the worker disconnects
	// while waiting in the pool.
	p.connectedWorkers[workerID] = worker
	p.healthCheckWg.Add(1)
	go p.monitorWorkerHealth(workerID, worker, p.stopHealthChecksCh)
	p.notifyOfPoolMembershipChange()
}

// monitorWorkerHealth implements a worker that detects worker disconnects
// while the worker is waiting in the pool.
func (p *workerPool) monitorWorkerHealth(workerID string, w *remoteWorkerStream, stopSignalCh <-chan struct{}) {
	defer p.healthCheckWg.Done()
	for {
		select {
		case <-w.stream.Context().Done():
			p.removeWorker(workerID)
			return
		case <-p.poolCtx.Done():
			w.Close(errMasterShuttingDown)
			return
		case <-stopSignalCh:
			// Pool requested us to terminate as the worker has been reserved for a job.
			return
		}
	}
}

func (p *workerPool) notifyOfPoolMembershipChange() {
	select {
	case p.poolMembersChangedCh <- struct{}{}:
	default: // another change has already been enqueued
	}
}

func (p *workerPool) removeWorker(workerID string) {
	p.mu.Lock()
	delete(p.connectedWorkers, workerID)
	p.notifyOfPoolMembershipChange()
	p.mu.Unlock()
}

// ReserveWorkers blocks until either the context gets cancelled or at least
// minWorkers are available in the pool. In the latter case, the workers are
// removed from the pool and returned back to the caller.
func (p *workerPool) ReserveWorkers(ctx context.Context, minWorkers int) ([]*remoteWorkerStream, error) {
	for {
		// Check for required number of workers
		p.mu.Lock()
		if numWorkers := len(p.connectedWorkers); numWorkers > 0 && numWorkers >= minWorkers {
			break // retain the lock to avoid changes in the pool
		}
		p.mu.Unlock()
		select {
		case <-p.poolMembersChangedCh: // re-check the required worker count
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.poolCtx.Done():
			return nil, errMasterShuttingDown
		}
	}

	// Signal health check workers and wait for them to exit before handing
	// off the worker list to the caller. This avoids the problem of having
	// multiple readers accessing the worker channels.
	close(p.stopHealthChecksCh)
	p.healthCheckWg.Wait()

	// Extract list of workers from the pool and create a new signal
	// channel for future workers.
	workers := make([]*remoteWorkerStream, 0, len(p.connectedWorkers))
	for _, w := range p.connectedWorkers {
		workers = append(workers, w)
	}
	p.connectedWorkers = make(map[string]*remoteWorkerStream)
	p.stopHealthChecksCh = make(chan struct{})
	p.mu.Unlock()

	return workers, nil
}
