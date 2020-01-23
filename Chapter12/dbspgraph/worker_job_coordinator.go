package dbspgraph

import (
	"context"
	"sync"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/bspgraph"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/bspgraph/message"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/dbspgraph/job"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/dbspgraph/proto"
	"github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

type workerJobCoordinatorConfig struct {
	jobDetails   job.Details
	masterStream *remoteMasterStream
	jobRunner    job.Runner
	serializer   Serializer
	logger       *logrus.Entry
}

// workerJobCoordinator is used by the worker node to coordinate the execution
// of an assigned job with a master node.
type workerJobCoordinator struct {
	jobCtx       context.Context
	cancelJobCtx func()

	cfg     workerJobCoordinatorConfig
	barrier *workerStepBarrier

	mu             sync.Mutex
	asyncWorkerErr error
}

// newWorkerJobCoordinator creates a new coordinator instance with the
// specified worker list.
func newWorkerJobCoordinator(ctx context.Context, cfg workerJobCoordinatorConfig) *workerJobCoordinator {
	jobCtx, cancelJobCtx := context.WithCancel(ctx)
	return &workerJobCoordinator{
		jobCtx:       jobCtx,
		cancelJobCtx: cancelJobCtx,
		barrier:      newWorkerStepBarrier(jobCtx, cfg.masterStream),
		cfg:          cfg,
	}
}

// RunJob executes a graph algorithm on a local graph instance by coordinating
// with a remote master.
func (c *workerJobCoordinator) RunJob() error {
	// Create a wrapper for patching the user-defined executor callbacks so
	// they can be executed in coordination with the master node and pass
	// the resulting factory to the job runner to get back an Executor for
	// the graph.
	execFactory := newWorkerExecutorFactory(c.cfg.serializer, c.barrier)
	executor, err := c.cfg.jobRunner.StartJob(c.cfg.jobDetails, execFactory)
	if err != nil {
		c.cancelJobCtx()
		return xerrors.Errorf("unable to start job on worker: %w", err)
	}

	// Get the graph from the executor and register the coordinator as a
	// relayer for unknown destinations
	graph := executor.Graph()
	graph.RegisterRelayer(bspgraph.RelayerFunc(c.relayNonLocalMessage))

	// Start a goroutine to handle incoming master messages
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.cfg.masterStream.SetDisconnectCallback(c.handleMasterDisconnect)
		c.handleMasterPayloads(graph)
	}()

	// Run job to completion or until an error occurs
	if err = c.runJobToCompletion(executor); err != nil {
		c.cfg.jobRunner.AbortJob(c.cfg.jobDetails)
		if xerrors.Is(err, context.Canceled) {
			err = errJobAborted
		}
		if c.asyncWorkerErr != nil {
			err = c.asyncWorkerErr
		}
	}

	c.cancelJobCtx()
	wg.Wait() // wait for any spawned goroutines to exit before returning.
	return err
}

// handleWorkerDisconnect is invoked when the worker's connection to the master
// node is lost.
func (c *workerJobCoordinator) handleMasterDisconnect() {
	select {
	case <-c.jobCtx.Done(): // job already aborted
	default:
		c.cancelJobCtx()
	}
}

// runJobToCompletion executes all required graph supersteps until the
// user-defined condition is met.
func (c *workerJobCoordinator) runJobToCompletion(executor *bspgraph.Executor) error {
	if err := executor.RunToCompletion(c.jobCtx); err != nil {
		return err
	} else if _, err := c.barrier.Wait(&proto.Step{Type: proto.Step_EXECUTED_GRAPH}); err != nil {
		return errJobAborted
	} else if err := c.cfg.jobRunner.CompleteJob(c.cfg.jobDetails); err != nil {
		return err
	} else if _, err = c.barrier.Wait(&proto.Step{Type: proto.Step_PESISTED_RESULTS}); err != nil {
		return errJobAborted
	}

	// Notify master that we are done and block until the master terminates
	// the job stream.
	_, _ = c.barrier.Wait(&proto.Step{Type: proto.Step_COMPLETED_JOB})
	return nil
}

// handleMasterPayloads implements the receive loop for messages sent by the
// master node.
func (c *workerJobCoordinator) handleMasterPayloads(graph *bspgraph.Graph) {
	defer c.cancelJobCtx()
	var mPayload *proto.MasterPayload
	for {
		select {
		case mPayload = <-c.cfg.masterStream.RecvFromMasterChan():
		case <-c.jobCtx.Done():
			return
		}

		if mPayload == nil {
			return
		} else if relayMsg := mPayload.GetRelayMessage(); relayMsg != nil {
			if err := c.deliverGraphMessage(graph, relayMsg); err != nil {
				c.mu.Lock()
				c.asyncWorkerErr = err
				c.mu.Unlock()
				c.cancelJobCtx()
				return
			}
		} else if stepMsg := mPayload.GetStep(); stepMsg != nil {
			if err := c.barrier.Notify(stepMsg); err != nil {
				return
			}
		}
	}
}

// relayNonLocalMessage is invoked by the graph to deliver messages for
// destinations that are not known by the local graph instance.
func (c *workerJobCoordinator) relayNonLocalMessage(dst string, msg message.Message) error {
	serializedMsg, err := c.cfg.serializer.Serialize(msg)
	if err != nil {
		return xerrors.Errorf("unable to serialize message: %w", err)
	}

	return c.sendToMaster(&proto.WorkerPayload{
		Payload: &proto.WorkerPayload_RelayMessage{
			RelayMessage: &proto.RelayMessage{
				Destination: dst,
				Message:     serializedMsg,
			},
		},
	})
}

func (c *workerJobCoordinator) deliverGraphMessage(graph *bspgraph.Graph, relayMsg *proto.RelayMessage) error {
	payload, err := c.cfg.serializer.Unserialize(relayMsg.Message)
	if err != nil {
		return xerrors.Errorf("unable to decode relayed message: %w", err)
	}

	graphMsg, ok := payload.(message.Message)
	if !ok {
		return xerrors.Errorf("unable to relay message payloads that do not implement message.Message")
	}

	return graph.SendMessage(relayMsg.Destination, graphMsg)
}

// sendToMaster attempts to send a message to a remote master. It blocks
// until either the message is enqueued for sending or the job context expires.
func (c *workerJobCoordinator) sendToMaster(wMsg *proto.WorkerPayload) error {
	select {
	case c.cfg.masterStream.SendToMasterChan() <- wMsg:
		return nil
	case <-c.jobCtx.Done():
		return errJobAborted
	}
}
