package dbspgraph

import (
	"context"
	"io"
	"sync"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/dbspgraph/proto"
	"golang.org/x/xerrors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errJobAborted is send to a worker to indicate that the master has aborted a
// running job due to some error.
var errJobAborted = xerrors.Errorf("job was aborted")

// errMasterShuttingDown is sent to a worker to indicate that the master is
// shutting down.
var errMasterShuttingDown = xerrors.New("master is shutting down")

// remoteWorkerStream represents a remote worker connection.
type remoteWorkerStream struct {
	stream    proto.JobQueue_JobStreamServer
	recvMsgCh chan *proto.WorkerPayload
	sendMsgCh chan *proto.MasterPayload
	sendErrCh chan error

	mu             sync.Mutex
	onDisconnectFn func()
	disconnected   bool
}

// newRemoteWorkerStream creates a stream abstraction for interacting with a
// remote worker instance.
func newRemoteWorkerStream(stream proto.JobQueue_JobStreamServer) *remoteWorkerStream {
	return &remoteWorkerStream{
		stream:    stream,
		recvMsgCh: make(chan *proto.WorkerPayload, 1),
		sendMsgCh: make(chan *proto.MasterPayload, 1),
		sendErrCh: make(chan error, 1),
	}
}

// HandleSendRecv asynchronously handles both the send and receiving ends of
// a remotely connected worker. Calls to HandleSendRecv block until the
func (s *remoteWorkerStream) HandleSendRecv() error {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	go s.handleRecv(ctx, cancelFn)
	for {
		select {
		case mPayload := <-s.sendMsgCh:
			if err := s.stream.Send(mPayload); err != nil {
				return err
			}
		case err, ok := <-s.sendErrCh:
			if !ok { // signalled to close without an error
				return nil
			}
			return status.Errorf(codes.Aborted, err.Error())
		case <-ctx.Done():
			return status.Errorf(codes.Aborted, errJobAborted.Error())
		}
	}
}

// handleRecv handles the receiving end of a worker's stream
func (s *remoteWorkerStream) handleRecv(ctx context.Context, cancelFn func()) {
	for {
		wPayload, err := s.stream.Recv()
		if err != nil {
			s.handleDisconnect()
			cancelFn()
			return
		}

		select {
		case s.recvMsgCh <- wPayload:
		case <-ctx.Done():
			return
		}
	}
}

func (s *remoteWorkerStream) handleDisconnect() {
	s.mu.Lock()
	if s.onDisconnectFn != nil {
		s.onDisconnectFn()
	}
	s.disconnected = true
	s.mu.Unlock()
}

// RecvFromWorkerChan returns a channel for reading incoming payloads from the
// worker.
func (s *remoteWorkerStream) RecvFromWorkerChan() <-chan *proto.WorkerPayload {
	return s.recvMsgCh
}

// SendToWorkerChan returns a channel for sending master payloads to the worker.
func (s *remoteWorkerStream) SendToWorkerChan() chan<- *proto.MasterPayload {
	return s.sendMsgCh
}

// SetDisconnectCallback registers a callback which will be invoked when the
// remote worker disconnects.
func (s *remoteWorkerStream) SetDisconnectCallback(cb func()) {
	s.mu.Lock()
	s.onDisconnectFn = cb
	if s.disconnected {
		s.onDisconnectFn()
	}
	s.mu.Unlock()
}

// Close terminates the worker's connection with an optional error.
func (s *remoteWorkerStream) Close(err error) {
	if err != nil {
		s.sendErrCh <- err
	}
	close(s.sendErrCh)
}

// remoteMasterStream represents a connection to a master node.
type remoteMasterStream struct {
	stream    proto.JobQueue_JobStreamClient
	recvMsgCh chan *proto.MasterPayload
	sendMsgCh chan *proto.WorkerPayload

	ctx      context.Context
	cancelFn func()

	mu             sync.Mutex
	onDisconnectFn func()
	disconnected   bool
}

// newRemoteMasterStream creates a stream abstraction for interacting with a master.
func newRemoteMasterStream(stream proto.JobQueue_JobStreamClient) *remoteMasterStream {
	ctx, cancelFn := context.WithCancel(context.Background())

	return &remoteMasterStream{
		ctx:       ctx,
		cancelFn:  cancelFn,
		stream:    stream,
		recvMsgCh: make(chan *proto.MasterPayload, 1),
		sendMsgCh: make(chan *proto.WorkerPayload, 1),
	}
}

// HandleSendRecv asynchronously handles both the send and receiving ends of
// a connection to a master node. Calls to HandleSendRecv block until the
func (s *remoteMasterStream) HandleSendRecv() error {
	defer func() {
		s.cancelFn()
		_ = s.stream.CloseSend()
	}()
	go s.handleRecv()
	for {
		select {
		case wPayload := <-s.sendMsgCh:
			if err := s.stream.Send(wPayload); err != nil && !xerrors.Is(err, io.EOF) {
				return err
			}
		case <-s.ctx.Done():
			return nil
		}
	}
}

// handleRecv handles the receiving end of a master's stream
func (s *remoteMasterStream) handleRecv() {
	for {
		mPayload, err := s.stream.Recv()
		if err != nil {
			s.handleDisconnect()
			s.cancelFn()
			return
		}

		select {
		case s.recvMsgCh <- mPayload:
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *remoteMasterStream) handleDisconnect() {
	s.mu.Lock()
	if s.onDisconnectFn != nil {
		s.onDisconnectFn()
	}
	s.disconnected = true
	s.mu.Unlock()
}

// RecvFromMasterChan returns a channel for reading incoming payloads from the
// master.
func (s *remoteMasterStream) RecvFromMasterChan() <-chan *proto.MasterPayload {
	return s.recvMsgCh
}

// SendToMasterChan returns a channel for sending worker payloads to the master.
func (s *remoteMasterStream) SendToMasterChan() chan<- *proto.WorkerPayload {
	return s.sendMsgCh
}

// SetDisconnectCallback registers a callback which will be invoked when the
// connection to the master node is lost.
func (s *remoteMasterStream) SetDisconnectCallback(cb func()) {
	s.mu.Lock()
	s.onDisconnectFn = cb
	if s.disconnected {
		s.onDisconnectFn()
	}
	s.mu.Unlock()
}

// Close gracefully terminates the connection to the master.
func (s *remoteMasterStream) Close() {
	s.cancelFn()
}
