package dbspgraph

import (
	"context"
	"net"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/dbspgraph/job"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/dbspgraph/proto"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

var (
	minUUID = uuid.Nil
	maxUUID = uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")

	// ErrUnableToReserveWorkers is returned by the master to indicate that
	// the required number of workers for running a job is not available.
	ErrUnableToReserveWorkers = xerrors.Errorf("unable to reserve required number of workers")
)

// Master orchestrates the execution of a distributed graph-based algorithm
// built on top of the bspgraph framework across multiple workers.
type Master struct {
	cfg         MasterConfig
	workerPool  *workerPool
	srvListener net.Listener
}

// NewMaster creates a new Master instance with the specified configuration.
func NewMaster(cfg MasterConfig) (*Master, error) {
	if err := cfg.Validate(); err != nil {
		return nil, xerrors.Errorf("master config validation failed: %w", err)
	}

	return &Master{
		cfg:        cfg,
		workerPool: newWorkerPool(),
	}, nil
}

// Start listening on the configured address for incoming worker connections.
// Calls to Start are non-blocking. The caller must invoke the Close method
// to shutdown the server and clean up any reserved resources.
func (m *Master) Start() error {
	var err error
	if m.srvListener, err = net.Listen("tcp", m.cfg.ListenAddress); err != nil {
		return xerrors.Errorf("cannot start server: %w", err)
	}

	gSrv := grpc.NewServer()
	proto.RegisterJobQueueServer(gSrv, &masterRPCHandler{
		workerPool: m.workerPool,
		logger:     m.cfg.Logger,
	})
	m.cfg.Logger.WithField("addr", m.srvListener.Addr().String()).Info("listening for worker connections")
	go func(l net.Listener) { _ = gSrv.Serve(l) }(m.srvListener)

	return nil
}

// Close disconnects any connected workers and shuts down the gRPC server.
func (m *Master) Close() error {
	var err error

	if m.srvListener != nil {
		err = m.srvListener.Close()
		m.srvListener = nil
	}

	if cErr := m.workerPool.Close(); cErr != nil {
		err = multierror.Append(err, cErr)
	}

	return err
}

// RunJob creates a new job and coordinates its execution until the job
// completes, the context expires or some error occurs. The minWorkers
// parameter defines the minimum number of connected workers required for the
// job. It may be set to 0 to reserve all workers currently available. If the
// required number of workers is not available, RunJob blocks until either
// enough workers connect, or the workerAcquireTimeout (if non-zero) expires or
// if the provided context expires.
func (m *Master) RunJob(ctx context.Context, minWorkers int, workerAcquireTimeout time.Duration) error {
	var acquireCtx = ctx
	if workerAcquireTimeout != 0 {
		var cancelFn func()
		acquireCtx, cancelFn = context.WithTimeout(ctx, workerAcquireTimeout)
		defer cancelFn()
	}
	workers, err := m.workerPool.ReserveWorkers(acquireCtx, minWorkers)
	if err != nil {
		return ErrUnableToReserveWorkers
	}

	jobID := uuid.New().String()
	createdAt := time.Now().UTC().Truncate(time.Millisecond)
	logger := m.cfg.Logger.WithField("job_id", jobID)
	coordinator, err := newMasterJobCoordinator(ctx, masterJobCoordinatorConfig{
		jobDetails: job.Details{
			JobID:           jobID,
			CreatedAt:       createdAt,
			PartitionFromID: minUUID,
			PartitionToID:   maxUUID,
		},
		workers:    workers,
		jobRunner:  m.cfg.JobRunner,
		serializer: m.cfg.Serializer,
		logger:     logger,
	})
	if err != nil {
		err = xerrors.Errorf("unable to create job coordinator: %w", err)
		for _, w := range workers {
			w.Close(err)
		}
		return err
	}

	logger.WithFields(logrus.Fields{
		"job_id":      jobID,
		"created_at":  createdAt,
		"num_workers": len(workers),
	}).Info("coordinating execution of new job")

	if err = coordinator.RunJob(); err != nil {
		logger.WithField("err", err).Error("job execution failed")
		for _, w := range workers {
			w.Close(err)
		}
		return err
	}

	logger.Info("job completed successfully")
	for _, w := range workers {
		w.Close(nil)
	}
	return nil
}

// masterRPCHandler implements the gRPC server for the master node.
type masterRPCHandler struct {
	logger     *logrus.Entry
	workerPool *workerPool
}

// JobStream implements JobQueueServer.
func (h *masterRPCHandler) JobStream(stream proto.JobQueue_JobStreamServer) error {
	extraFields := make(logrus.Fields)
	if peerDetails, ok := peer.FromContext(stream.Context()); ok {
		extraFields["peer_addr"] = peerDetails.Addr.String()
	}

	h.logger.WithFields(extraFields).Info("worker connected")

	// Add worker to the pool and block until its output stream needs to be
	// closed either because the job has been completed or an error
	// occurred.
	workerStream := newRemoteWorkerStream(stream)
	h.workerPool.AddWorker(workerStream)
	return workerStream.HandleSendRecv()
}
