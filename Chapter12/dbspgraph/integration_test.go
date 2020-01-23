package dbspgraph_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/bspgraph"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/bspgraph/aggregator"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/bspgraph/message"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/dbspgraph"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/dbspgraph/job"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(DistributedGraphTestSuite))

type graphMessage string

func (graphMessage) Type() string { return "graph-message" }

type DistributedGraphTestSuite struct {
	logger    *logrus.Entry
	logOutput bytes.Buffer
}

func (s *DistributedGraphTestSuite) SetUpTest(c *gc.C) {
	s.logOutput.Reset()
	rootLogger := logrus.New()
	rootLogger.Level = logrus.DebugLevel
	rootLogger.Out = &s.logOutput

	s.logger = logrus.NewEntry(rootLogger)
}

func (s *DistributedGraphTestSuite) TearDownTest(c *gc.C) {
	c.Log(s.logOutput.String())
}

func (s *DistributedGraphTestSuite) TestSuccessfulJob(c *gc.C) {
	// We run 2 supersteps to ensure that our test messages are delivered.
	maxSupersteps := 2
	numWorkers := 20
	listenAddr := s.findFreePort(c)
	ctx, cancelFn := context.WithCancel(context.TODO())
	defer cancelFn()

	masterRunner := newJobRunner(c, maxSupersteps, true, s.logger.WithField("master", "true"))
	master, err := dbspgraph.NewMaster(dbspgraph.MasterConfig{
		ListenAddress: listenAddr,
		JobRunner:     masterRunner,
		Serializer:    new(serializer),
		Logger:        s.logger.WithField("master", "true"),
	})
	c.Assert(err, gc.IsNil)
	c.Assert(master.Start(), gc.IsNil)
	defer func() { c.Assert(master.Close(), gc.IsNil) }()

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for workerID := 0; workerID < numWorkers; workerID++ {
		go func(workerID int) {
			defer wg.Done()

			jr := newJobRunner(c, maxSupersteps, false, s.logger.WithField("worker_id", workerID))
			defer func() { c.Assert(jr.graph.Close(), gc.IsNil) }()
			worker, err := dbspgraph.NewWorker(dbspgraph.WorkerConfig{
				JobRunner:  jr,
				Serializer: new(serializer),
				Logger:     s.logger.WithField("worker_id", workerID),
			})
			c.Assert(err, gc.IsNil)
			defer func() { c.Assert(worker.Close(), gc.IsNil) }()
			c.Assert(worker.Dial(listenAddr, 15*time.Second), gc.IsNil)
			c.Assert(worker.RunJob(ctx), gc.IsNil)
			c.Assert(worker.Close(), gc.IsNil)
			c.Assert(jr.startJobCalled, gc.Equals, true)
			c.Assert(jr.abortJobCalled, gc.Equals, false)
			c.Assert(jr.completeJobCalled, gc.Equals, true)
		}(workerID)
	}

	c.Assert(master.RunJob(ctx, numWorkers, 10*time.Second), gc.IsNil)
	c.Assert(master.Close(), gc.IsNil)

	c.Assert(masterRunner.startJobCalled, gc.Equals, true)
	c.Assert(masterRunner.abortJobCalled, gc.Equals, false)
	c.Assert(masterRunner.completeJobCalled, gc.Equals, true)

	c.Assert(masterRunner.graph.Aggregator("msg_count").Get(), gc.Equals, numWorkers, gc.Commentf("expected the number of exchanged messages to be equal to the number of workers"))
	wg.Wait()
}

func (s *DistributedGraphTestSuite) TestWorkerFailsStartingJob(c *gc.C) {
	maxSupersteps := 1
	numWorkers := 5
	listenAddr := s.findFreePort(c)
	ctx, cancelFn := context.WithCancel(context.TODO())
	defer cancelFn()

	masterRunner := newJobRunner(c, maxSupersteps, true, s.logger.WithField("master", "true"))
	master, err := dbspgraph.NewMaster(dbspgraph.MasterConfig{
		ListenAddress: listenAddr,
		JobRunner:     masterRunner,
		Serializer:    new(serializer),
		Logger:        s.logger.WithField("master", "true"),
	})
	c.Assert(err, gc.IsNil)
	c.Assert(master.Start(), gc.IsNil)
	defer func() { c.Assert(master.Close(), gc.IsNil) }()

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for workerID := 0; workerID < numWorkers; workerID++ {
		go func(workerID int) {
			defer wg.Done()

			jr := newJobRunner(c, maxSupersteps, false, s.logger.WithField("worker_id", workerID))
			defer func() { c.Assert(jr.graph.Close(), gc.IsNil) }()
			if workerID == 0 {
				jr.startJobErr = xerrors.Errorf("could not start job")
			}

			worker, err := dbspgraph.NewWorker(dbspgraph.WorkerConfig{
				JobRunner:  jr,
				Serializer: new(serializer),
				Logger:     s.logger.WithField("worker_id", workerID),
			})
			c.Assert(err, gc.IsNil)
			defer func() { c.Assert(worker.Close(), gc.IsNil) }()
			c.Assert(worker.Dial(listenAddr, 15*time.Second), gc.IsNil)
			err = worker.RunJob(ctx)
			if workerID == 0 {
				c.Assert(err, gc.ErrorMatches, ".*could not start job")
			} else {
				c.Assert(err, gc.ErrorMatches, ".*job was aborted")
			}
			c.Assert(worker.Close(), gc.IsNil)
			c.Assert(jr.startJobCalled, gc.Equals, true)
			c.Assert(jr.abortJobCalled, gc.Equals, workerID != 0, gc.Commentf("AbortJob should be called on workers that don't report errors starting jobs"))
			c.Assert(jr.completeJobCalled, gc.Equals, false)
		}(workerID)
	}

	err = master.RunJob(ctx, numWorkers, 10*time.Second)
	c.Assert(err, gc.ErrorMatches, ".*job was aborted")
	c.Assert(masterRunner.startJobCalled, gc.Equals, true)
	c.Assert(masterRunner.abortJobCalled, gc.Equals, true)
	c.Assert(masterRunner.completeJobCalled, gc.Equals, false)
	wg.Wait()
}

func (s *DistributedGraphTestSuite) TestWorkerFailsInGraphComputeFunction(c *gc.C) {
	maxSupersteps := 1
	numWorkers := 10
	listenAddr := s.findFreePort(c)
	ctx, cancelFn := context.WithCancel(context.TODO())
	defer cancelFn()

	masterRunner := newJobRunner(c, maxSupersteps, true, s.logger.WithField("master", "true"))
	master, err := dbspgraph.NewMaster(dbspgraph.MasterConfig{
		ListenAddress: listenAddr,
		JobRunner:     masterRunner,
		Serializer:    new(serializer),
		Logger:        s.logger.WithField("master", "true"),
	})
	c.Assert(err, gc.IsNil)
	c.Assert(master.Start(), gc.IsNil)
	defer func() { c.Assert(master.Close(), gc.IsNil) }()

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for workerID := 0; workerID < numWorkers; workerID++ {
		go func(workerID int) {
			defer wg.Done()

			jr := newJobRunner(c, maxSupersteps, false, s.logger.WithField("worker_id", workerID))
			defer func() { c.Assert(jr.graph.Close(), gc.IsNil) }()
			if workerID == 0 {
				jr.computeFnErr = xerrors.Errorf("error in compute fn")
			}

			worker, err := dbspgraph.NewWorker(dbspgraph.WorkerConfig{
				JobRunner:  jr,
				Serializer: new(serializer),
				Logger:     s.logger.WithField("worker_id", workerID),
			})
			c.Assert(err, gc.IsNil)
			c.Assert(worker.Dial(listenAddr, 15*time.Second), gc.IsNil)
			err = worker.RunJob(ctx)
			if workerID == 0 {
				c.Assert(err, gc.ErrorMatches, ".*error in compute fn")
			} else {
				c.Assert(err, gc.ErrorMatches, ".*job was aborted")
			}
			c.Assert(worker.Close(), gc.IsNil)
			c.Assert(jr.startJobCalled, gc.Equals, true)
			c.Assert(jr.abortJobCalled, gc.Equals, true)
			c.Assert(jr.completeJobCalled, gc.Equals, false)
		}(workerID)
	}

	err = master.RunJob(ctx, numWorkers, 10*time.Second)
	c.Assert(err, gc.ErrorMatches, ".*job was aborted")
	c.Assert(masterRunner.startJobCalled, gc.Equals, true)
	c.Assert(masterRunner.abortJobCalled, gc.Equals, true)
	c.Assert(masterRunner.completeJobCalled, gc.Equals, false)
	wg.Wait()
}

func (s *DistributedGraphTestSuite) TestWorkerFailsInCompleteJob(c *gc.C) {
	maxSupersteps := 1
	numWorkers := 10
	listenAddr := s.findFreePort(c)
	ctx, cancelFn := context.WithCancel(context.TODO())
	defer cancelFn()

	masterRunner := newJobRunner(c, maxSupersteps, true, s.logger.WithField("master", "true"))
	master, err := dbspgraph.NewMaster(dbspgraph.MasterConfig{
		ListenAddress: listenAddr,
		JobRunner:     masterRunner,
		Serializer:    new(serializer),
		Logger:        s.logger.WithField("master", "true"),
	})
	c.Assert(err, gc.IsNil)
	c.Assert(master.Start(), gc.IsNil)
	defer func() { c.Assert(master.Close(), gc.IsNil) }()

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for workerID := 0; workerID < numWorkers; workerID++ {
		go func(workerID int) {
			defer wg.Done()

			jr := newJobRunner(c, maxSupersteps, false, s.logger.WithField("worker_id", workerID))
			defer func() { c.Assert(jr.graph.Close(), gc.IsNil) }()
			if workerID == 0 {
				jr.completeJobErr = xerrors.Errorf("error in complete job")
			}

			worker, err := dbspgraph.NewWorker(dbspgraph.WorkerConfig{
				JobRunner:  jr,
				Serializer: new(serializer),
				Logger:     s.logger.WithField("worker_id", workerID),
			})
			c.Assert(err, gc.IsNil)
			c.Assert(worker.Dial(listenAddr, 15*time.Second), gc.IsNil)
			err = worker.RunJob(ctx)
			if workerID == 0 {
				c.Assert(err, gc.ErrorMatches, ".*error in complete job")
				c.Assert(jr.completeJobCalled, gc.Equals, true)
			} else {
				c.Assert(err, gc.ErrorMatches, ".*job was aborted")
			}
			c.Assert(worker.Close(), gc.IsNil)
			c.Assert(jr.startJobCalled, gc.Equals, true)
			c.Assert(jr.abortJobCalled, gc.Equals, true)
		}(workerID)
	}

	err = master.RunJob(ctx, numWorkers, 10*time.Second)
	c.Assert(err, gc.ErrorMatches, ".*job was aborted")
	c.Assert(masterRunner.startJobCalled, gc.Equals, true)
	c.Assert(masterRunner.abortJobCalled, gc.Equals, true)
	wg.Wait()
}

func (s *DistributedGraphTestSuite) TestGraphMessageUnmarshalError(c *gc.C) {
	maxSupersteps := 2
	numWorkers := 5
	listenAddr := s.findFreePort(c)
	ctx, cancelFn := context.WithCancel(context.TODO())
	defer cancelFn()

	masterRunner := newJobRunner(c, maxSupersteps, true, s.logger.WithField("master", "true"))
	master, err := dbspgraph.NewMaster(dbspgraph.MasterConfig{
		ListenAddress: listenAddr,
		JobRunner:     masterRunner,
		Serializer:    new(serializer),
		Logger:        s.logger.WithField("master", "true"),
	})
	c.Assert(err, gc.IsNil)
	c.Assert(master.Start(), gc.IsNil)
	defer func() { c.Assert(master.Close(), gc.IsNil) }()

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for workerID := 0; workerID < numWorkers; workerID++ {
		go func(workerID int) {
			defer wg.Done()

			jr := newJobRunner(c, maxSupersteps, false, s.logger.WithField("worker_id", workerID))
			defer func() { c.Assert(jr.graph.Close(), gc.IsNil) }()
			worker, err := dbspgraph.NewWorker(dbspgraph.WorkerConfig{
				JobRunner:  jr,
				Serializer: &serializer{failOnUnserializeMsg: true},
				Logger:     s.logger.WithField("worker_id", workerID),
			})
			c.Assert(err, gc.IsNil)
			c.Assert(worker.Dial(listenAddr, 15*time.Second), gc.IsNil)
			err = worker.RunJob(ctx)
			c.Assert(err, gc.ErrorMatches, "(.*unserialization error|.*job was aborted)")
			c.Assert(worker.Close(), gc.IsNil)
			c.Assert(jr.startJobCalled, gc.Equals, true)
			c.Assert(jr.abortJobCalled, gc.Equals, true)
		}(workerID)
	}

	err = master.RunJob(ctx, numWorkers, 10*time.Second)
	c.Assert(err, gc.ErrorMatches, ".*job was aborted")
	c.Assert(masterRunner.startJobCalled, gc.Equals, true)
	c.Assert(masterRunner.abortJobCalled, gc.Equals, true)
	wg.Wait()
}

func (s *DistributedGraphTestSuite) TestTryToRelayMessageToUnknownDestination(c *gc.C) {
	maxSupersteps := 2
	numWorkers := 10
	listenAddr := s.findFreePort(c)
	ctx, cancelFn := context.WithCancel(context.TODO())
	defer cancelFn()

	masterRunner := newJobRunner(c, maxSupersteps, true, s.logger.WithField("master", "true"))
	master, err := dbspgraph.NewMaster(dbspgraph.MasterConfig{
		ListenAddress: listenAddr,
		JobRunner:     masterRunner,
		Serializer:    new(serializer),
		Logger:        s.logger.WithField("master", "true"),
	})
	c.Assert(err, gc.IsNil)
	c.Assert(master.Start(), gc.IsNil)
	defer func() { c.Assert(master.Close(), gc.IsNil) }()

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for workerID := 0; workerID < numWorkers; workerID++ {
		go func(workerID int) {
			defer wg.Done()

			jr := newJobRunner(c, maxSupersteps, false, s.logger.WithField("worker_id", workerID))
			if workerID == 0 {
				jr.sendBogusMessage = true
			}

			worker, err := dbspgraph.NewWorker(dbspgraph.WorkerConfig{
				JobRunner:  jr,
				Serializer: new(serializer),
				Logger:     s.logger.WithField("worker_id", workerID),
			})
			c.Assert(err, gc.IsNil)
			c.Assert(worker.Dial(listenAddr, 15*time.Second), gc.IsNil)
			err = worker.RunJob(ctx)
			c.Assert(err, gc.ErrorMatches, ".*job was aborted")
			c.Assert(worker.Close(), gc.IsNil)
			c.Assert(jr.startJobCalled, gc.Equals, true)
			c.Assert(jr.abortJobCalled, gc.Equals, true)
		}(workerID)
	}

	err = master.RunJob(ctx, numWorkers, 10*time.Second)
	c.Assert(err, gc.ErrorMatches, ".*job was aborted")
	c.Assert(masterRunner.startJobCalled, gc.Equals, true)
	c.Assert(masterRunner.abortJobCalled, gc.Equals, true)
	wg.Wait()
}

func (s *DistributedGraphTestSuite) findFreePort(c *gc.C) string {
	l, err := net.Listen("tcp", ":0")
	c.Assert(err, gc.IsNil)
	listenAddr := l.Addr().String()
	_ = l.Close()
	return listenAddr
}

type jobRunner struct {
	graph             *bspgraph.Graph
	executorCallbacks bspgraph.ExecutorCallbacks
	isMaster          bool
	logger            *logrus.Entry

	startJobErr    error
	completeJobErr error
	computeFnErr   error

	startJobCalled    bool
	completeJobCalled bool
	abortJobCalled    bool
	sendBogusMessage  bool
}

func newJobRunner(c *gc.C, maxSupersteps int, isMaster bool, logger *logrus.Entry) *jobRunner {
	j := &jobRunner{
		executorCallbacks: bspgraph.ExecutorCallbacks{
			PreStep:  func(context.Context, *bspgraph.Graph) error { return nil },
			PostStep: func(context.Context, *bspgraph.Graph, int) error { return nil },
			PostStepKeepRunning: func(_ context.Context, g *bspgraph.Graph, activeInStep int) (bool, error) {
				return g.Superstep() != (maxSupersteps - 1), nil
			},
		},
		isMaster: isMaster,
		logger:   logger,
	}

	j.setupGraph(c)
	return j
}

func (j *jobRunner) setupGraph(c *gc.C) {
	graph, err := bspgraph.NewGraph(bspgraph.GraphConfig{
		ComputeFn: func(g *bspgraph.Graph, v *bspgraph.Vertex, msgIt message.Iterator) error {
			if j.computeFnErr != nil {
				return j.computeFnErr
			}

			for msgIt.Next() {
				j.logger.Debugf("[STEP %d] %v received message: %v", g.Superstep(), v.ID(), msgIt.Message())
				g.Aggregator("msg_count").Aggregate(1)
			}
			g.Aggregator("accum").Aggregate(1)

			// At step 0, each vertex will send a message to the vertex with ID
			// 00000000-0000-0000-0000-000000000000
			if g.Superstep() == 0 {
				if j.sendBogusMessage {
					_ = g.SendMessage("badf00d1-feed-face-bad1-c0ffeec0ffee", graphMessage("message to unknown destination"))
				}
				j.logger.Debugf("[STEP %d] sending message from %v", g.Superstep(), v.ID())
				return g.SendMessage(uuid.Nil.String(), graphMessage(fmt.Sprintf("hello from %s", v.ID())))
			}
			return nil
		},
	})
	c.Assert(err, gc.IsNil)

	graph.RegisterAggregator("accum", new(aggregator.IntAccumulator))
	graph.RegisterAggregator("msg_count", new(aggregator.IntAccumulator))
	j.graph = graph
}

func (j *jobRunner) StartJob(jobDetails job.Details, execFactory bspgraph.ExecutorFactory) (*bspgraph.Executor, error) {
	j.startJobCalled = true
	if j.startJobErr != nil {
		return nil, j.startJobErr
	}

	if !j.isMaster {
		j.graph.AddVertex(jobDetails.PartitionFromID.String(), nil)
	}

	return execFactory(j.graph, j.executorCallbacks), nil
}

func (j *jobRunner) CompleteJob(_ job.Details) error {
	j.completeJobCalled = true
	if j.completeJobErr != nil {
		return j.completeJobErr
	}
	return nil
}

func (j *jobRunner) AbortJob(_ job.Details) {
	j.abortJobCalled = true
}

type serializer struct {
	failOnSerializeMsg          bool
	failOnSerializeAggregator   bool
	failOnUnserializeMsg        bool
	failOnUnSerializeAggregator bool
}

func (s *serializer) Serialize(v interface{}) (*any.Any, error) {
	switch val := v.(type) {
	case int:
		if s.failOnSerializeAggregator {
			return nil, xerrors.Errorf("serialization error")
		}
		return &any.Any{
			TypeUrl: "int",
			Value:   []byte(fmt.Sprint(val)),
		}, nil
	case graphMessage:
		if s.failOnSerializeMsg {
			return nil, xerrors.Errorf("serialization error")
		}
		return &any.Any{
			TypeUrl: val.Type(),
			Value:   []byte(fmt.Sprint(val)),
		}, nil
	default:
		return nil, xerrors.Errorf("serialize: unknown type %#+T", val)
	}
}

func (s *serializer) Unserialize(v *any.Any) (interface{}, error) {
	switch v.TypeUrl {
	case "int":
		if s.failOnUnSerializeAggregator {
			return nil, xerrors.Errorf("unserialization error")
		}
		intV, _ := strconv.ParseInt(string(v.Value), 10, 64)
		return int(intV), nil
	case "graph-message":
		if s.failOnUnserializeMsg {
			return nil, xerrors.Errorf("unserialization error")
		}
		return graphMessage(string(v.Value)), nil
	default:
		return nil, xerrors.Errorf("unserialize: unknown type %q", v.TypeUrl)
	}
}
