package dbspgraph

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/dbspgraph/proto"
	"google.golang.org/grpc"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(RealStreamTestSuite))

type RealStreamTestSuite struct {
	workerStream *remoteWorkerStream
	masterStream *remoteMasterStream
	srvListener  net.Listener
	cliConn      *grpc.ClientConn

	connectionComplete chan struct{}
}

func (s *RealStreamTestSuite) SetUpTest(c *gc.C) {
	s.connectionComplete = make(chan struct{})

	l, err := net.Listen("tcp", ":0")
	c.Assert(err, gc.IsNil)
	s.srvListener = l
	srv := grpc.NewServer()
	proto.RegisterJobQueueServer(srv, s)
	go func() { _ = srv.Serve(l) }()

	s.cliConn, err = grpc.Dial(l.Addr().String(), grpc.WithInsecure())
	c.Assert(err, gc.IsNil)
	cli := proto.NewJobQueueClient(s.cliConn)
	cliStream, err := cli.JobStream(context.TODO())
	c.Assert(err, gc.IsNil)
	s.masterStream = newRemoteMasterStream(cliStream)
	select {
	case <-s.connectionComplete:
	case <-time.After(5 * time.Second):
		c.Fatal("timeout waiting for connection to be established")
	}
}

func (s *RealStreamTestSuite) TearDownTest(c *gc.C) {
	if s.srvListener != nil {
		_ = s.srvListener.Close()
	}

	if s.cliConn != nil {
		_ = s.srvListener.Close()
	}
}

func (s *RealStreamTestSuite) TestGracefulDisconnectByWorker(c *gc.C) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := s.masterStream.HandleSendRecv()
		c.Assert(err, gc.IsNil)
	}()

	s.masterStream.SendToMasterChan() <- new(proto.WorkerPayload)
	c.Log("worker sent payload to master")
	<-s.workerStream.RecvFromWorkerChan()
	c.Log("master received payload from worker")
	s.workerStream.SendToWorkerChan() <- new(proto.MasterPayload)
	c.Log("master sent payload to worker")
	<-s.masterStream.RecvFromMasterChan()
	c.Log("worker received payload from master")
	s.masterStream.Close()
	c.Log("worker closed connection to master")

	wg.Wait()
}

func (s *RealStreamTestSuite) TestGracefulDisconnectByMaster(c *gc.C) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := s.masterStream.HandleSendRecv()
		c.Assert(err, gc.IsNil)
	}()

	s.masterStream.SendToMasterChan() <- new(proto.WorkerPayload)
	c.Log("worker sent payload to master")
	<-s.workerStream.RecvFromWorkerChan()
	s.workerStream.Close(nil)
	c.Log("master closed connection to worker without error")

	wg.Wait()
}

func (s *RealStreamTestSuite) JobStream(stream proto.JobQueue_JobStreamServer) error {
	s.workerStream = newRemoteWorkerStream(stream)
	s.connectionComplete <- struct{}{}
	return s.workerStream.HandleSendRecv()
}
