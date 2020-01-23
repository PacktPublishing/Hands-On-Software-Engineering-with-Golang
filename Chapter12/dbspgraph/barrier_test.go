package dbspgraph

import (
	"context"
	"sync"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/dbspgraph/proto"
	"golang.org/x/xerrors"
	gc "gopkg.in/check.v1"
)

var _ = gc.Suite(new(MasterBarrierTestSuite))

type MasterBarrierTestSuite struct {
}

func (s *MasterBarrierTestSuite) TestWaitForWorkers(c *gc.C) {
	var (
		wg sync.WaitGroup
		b  = newMasterStepBarrier(context.TODO(), 2)
	)
	wg.Add(2)

	for i := 0; i < 2; i++ {
		go func(i int) {
			defer wg.Done()
			step, err := b.Wait(&proto.Step{
				Type:         proto.Step_POST_KEEP_RUNNING,
				ActiveInStep: int64(i + 1),
			})
			c.Assert(err, gc.IsNil)
			c.Assert(step.ActiveInStep, gc.Equals, int64(3))
		}(i)
	}

	steps, err := b.WaitForWorkers(proto.Step_POST_KEEP_RUNNING)
	c.Assert(err, gc.IsNil)
	c.Assert(steps, gc.HasLen, 2, gc.Commentf("expected to collect steps from two workers"))

	var totalActive int64
	for _, step := range steps {
		totalActive += step.ActiveInStep
	}
	c.Assert(totalActive, gc.Equals, int64(3))

	// Unblock workers
	err = b.NotifyWorkers(&proto.Step{
		Type:         proto.Step_POST_KEEP_RUNNING,
		ActiveInStep: totalActive,
	})
	c.Assert(err, gc.IsNil)

	wg.Wait()
}

func (s *MasterBarrierTestSuite) TestContextCancelledWhileWorkerEnteringBarrier(c *gc.C) {
	ctx, cancelFn := context.WithCancel(context.TODO())
	cancelFn()

	b := newMasterStepBarrier(ctx, 1)
	_, err := b.Wait(&proto.Step{Type: proto.Step_PRE})
	c.Assert(xerrors.Is(err, errJobAborted), gc.Equals, true)
}

func (s *MasterBarrierTestSuite) TestContextCancelledWhileWorkerExitingBarrier(c *gc.C) {
	ctx, cancelFn := context.WithCancel(context.TODO())
	b := newMasterStepBarrier(ctx, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := b.Wait(&proto.Step{Type: proto.Step_PRE})
		c.Assert(xerrors.Is(err, errJobAborted), gc.Equals, true)
	}()

	// Wait for worker to enter and then cancel the context
	_, err := b.WaitForWorkers(proto.Step_PRE)
	c.Assert(err, gc.IsNil)
	cancelFn()

	// Wait for worker go-routine to exit
	wg.Wait()
}

func (s *MasterBarrierTestSuite) TestContextCancelledWhileWaitingForWorkers(c *gc.C) {
	ctx, cancelFn := context.WithCancel(context.TODO())
	cancelFn()

	b := newMasterStepBarrier(ctx, 1)

	_, err := b.WaitForWorkers(proto.Step_POST_KEEP_RUNNING)
	c.Assert(xerrors.Is(err, errJobAborted), gc.Equals, true)
}

func (s *MasterBarrierTestSuite) TestContextCancelledWhileNotifyingWorkers(c *gc.C) {
	ctx, cancelFn := context.WithCancel(context.TODO())
	cancelFn()

	b := newMasterStepBarrier(ctx, 1)

	err := b.NotifyWorkers(&proto.Step{Type: proto.Step_EXECUTED_GRAPH})
	c.Assert(xerrors.Is(err, errJobAborted), gc.Equals, true)
}

func (s *MasterBarrierTestSuite) TestUnsupportedStepType(c *gc.C) {
	b := newMasterStepBarrier(context.TODO(), 1)

	_, err := b.WaitForWorkers(proto.Step_INVALID)
	c.Assert(err, gc.ErrorMatches, `unsupported step type "INVALID"`)

	err = b.NotifyWorkers(&proto.Step{})
	c.Assert(err, gc.ErrorMatches, `unsupported step type "INVALID"`)

	_, err = b.Wait(&proto.Step{})
	c.Assert(err, gc.ErrorMatches, `unsupported step type "INVALID"`)
}

var _ = gc.Suite(new(WorkerBarrierTestSuite))

type WorkerBarrierTestSuite struct {
}

func (s *WorkerBarrierTestSuite) TestWaitAndNotify(c *gc.C) {
	stream := newRemoteMasterStream(nil)
	defer close(stream.recvMsgCh)
	b := newWorkerStepBarrier(context.TODO(), stream)

	go func() {
		// Get message sent out by the barrier and simulate notification.
		for {
			payload, ok := <-stream.sendMsgCh
			if !ok {
				return
			}
			err := b.Notify(&proto.Step{Type: payload.GetStep().Type})
			c.Assert(err, gc.IsNil)
		}
	}()

	for stepType := range proto.Step_Type_name {
		if stepType == 0 {
			continue // ignore invalid
		}
		got, err := b.Wait(&proto.Step{Type: proto.Step_Type(stepType)})
		c.Assert(err, gc.IsNil, gc.Commentf("step type %s", proto.Step_Type_name[stepType]))
		c.Assert(got.Type, gc.Equals, proto.Step_Type(stepType), gc.Commentf("step type %s", proto.Step_Type_name[stepType]))
	}
}

func (s *WorkerBarrierTestSuite) TestUnsupportedStepType(c *gc.C) {
	b := newWorkerStepBarrier(context.TODO(), nil)

	_, err := b.Wait(&proto.Step{})
	c.Assert(err, gc.ErrorMatches, `unsupported step type "INVALID"`)

	err = b.Notify(&proto.Step{})
	c.Assert(err, gc.ErrorMatches, `unsupported step type "INVALID"`)
}

func (s *WorkerBarrierTestSuite) TestContextCancelledWhileWaiting(c *gc.C) {
	ctx, cancelFn := context.WithCancel(context.TODO())
	cancelFn()

	// Use a nil send channel to block for ever
	stream := newRemoteMasterStream(nil)
	stream.sendMsgCh = nil
	b := newWorkerStepBarrier(ctx, stream)

	_, err := b.Wait(&proto.Step{Type: proto.Step_PRE})
	c.Assert(err, gc.Equals, errJobAborted)
}

func (s *WorkerBarrierTestSuite) TestContextCancelledWhileNotifying(c *gc.C) {
	ctx, cancelFn := context.WithCancel(context.TODO())
	cancelFn()

	stream := newRemoteMasterStream(nil)
	b := newWorkerStepBarrier(ctx, stream)

	err := b.Notify(&proto.Step{Type: proto.Step_PRE})
	c.Assert(err, gc.Equals, errJobAborted)
}
