package dbspgraph

import (
	"context"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/dbspgraph/proto"
	"golang.org/x/xerrors"
)

// masterStepBarrier implements a barrier primitive for master nodes.
type masterStepBarrier struct {
	ctx        context.Context
	numWorkers int
	waitCh     map[proto.Step_Type]chan *proto.Step
	notifyCh   map[proto.Step_Type]chan *proto.Step
}

// newMasterStepBarrier creates a new barrier instance for a master node that
// will be accessed by the specified number of remote workers.
func newMasterStepBarrier(ctx context.Context, numWorkers int) *masterStepBarrier {
	waitCh := make(map[proto.Step_Type]chan *proto.Step)
	notifyCh := make(map[proto.Step_Type]chan *proto.Step)
	for stepType := range proto.Step_Type_name {
		if proto.Step_Type(stepType) == proto.Step_INVALID {
			continue
		}
		waitCh[proto.Step_Type(stepType)] = make(chan *proto.Step)
		notifyCh[proto.Step_Type(stepType)] = make(chan *proto.Step)
	}

	return &masterStepBarrier{
		ctx:        ctx,
		numWorkers: numWorkers,
		waitCh:     waitCh,
		notifyCh:   notifyCh,
	}
}

// WaitForWorkers blocks until all workers enter the barrier for stepType (or
// the context associated with the barrier expires) and returns back the Step
// messages received by the workers.
func (b *masterStepBarrier) WaitForWorkers(stepType proto.Step_Type) ([]*proto.Step, error) {
	waitCh, exists := b.waitCh[stepType]
	if !exists {
		return nil, xerrors.Errorf("unsupported step type %q", proto.Step_Type_name[int32(stepType)])
	}

	collectedSteps := make([]*proto.Step, b.numWorkers)
	for i := 0; i < b.numWorkers; i++ {
		select {
		case step := <-waitCh:
			collectedSteps[i] = step
		case <-b.ctx.Done():
			return nil, errJobAborted
		}
	}

	return collectedSteps, nil
}

// NotifyWorkers broadcasts the provided Step message to all workers waiting
// on the barrier for the specified message type.
func (b *masterStepBarrier) NotifyWorkers(step *proto.Step) error {
	notifyCh, exists := b.notifyCh[step.Type]
	if !exists {
		return xerrors.Errorf("unsupported step type %q", proto.Step_Type_name[int32(step.Type)])
	}

	for i := 0; i < b.numWorkers; i++ {
		select {
		case notifyCh <- step:
		case <-b.ctx.Done():
			return errJobAborted
		}
	}

	return nil
}

// Wait enters the barrier for the specified Step type and blocks until
// NotifyWorkers is invoked. The method returns back the Step message passed
// to NotifyWorkers.
func (b *masterStepBarrier) Wait(step *proto.Step) (*proto.Step, error) {
	waitCh, exists := b.waitCh[step.Type]
	if !exists {
		return nil, xerrors.Errorf("unsupported step type %q", proto.Step_Type_name[int32(step.Type)])
	}

	// Join the wait channel
	select {
	case waitCh <- step:
	case <-b.ctx.Done():
		return nil, errJobAborted
	}

	// Wait for notification from master
	select {
	case step = <-b.notifyCh[step.Type]:
		return step, nil
	case <-b.ctx.Done():
		return nil, errJobAborted
	}
}

// workerStepBarrier implements a barrier primitive for worker nodes which
// communicates with a masterStepBarrier on a master node.
type workerStepBarrier struct {
	ctx    context.Context
	stream *remoteMasterStream
	waitCh map[proto.Step_Type]chan *proto.Step
}

// newWorkerStepBarrier creates a new barrier instance for a worker node that
// communicates with a master barrier instance over the provided channels.
func newWorkerStepBarrier(ctx context.Context, stream *remoteMasterStream) *workerStepBarrier {
	waitCh := make(map[proto.Step_Type]chan *proto.Step)
	for stepType := range proto.Step_Type_name {
		if proto.Step_Type(stepType) == proto.Step_INVALID {
			continue
		}
		waitCh[proto.Step_Type(stepType)] = make(chan *proto.Step)
	}

	return &workerStepBarrier{
		ctx:    ctx,
		stream: stream,
		waitCh: waitCh,
	}
}

// Wait enters the barrier for the specified Step type on the master node
// and blocks until the Notify method is invoked. The method returns back the
// Step message response that was passed to Notify.
func (b *workerStepBarrier) Wait(step *proto.Step) (*proto.Step, error) {
	ch, exists := b.waitCh[step.Type]
	if !exists {
		return nil, xerrors.Errorf("unsupported step type %q", proto.Step_Type_name[int32(step.Type)])
	}

	select {
	case b.stream.SendToMasterChan() <- &proto.WorkerPayload{Payload: &proto.WorkerPayload_Step{Step: step}}:
	case <-b.ctx.Done():
		return nil, errJobAborted
	}

	// Wait for notification to exit the barrier.
	select {
	case step = <-ch:
		return step, nil
	case <-b.ctx.Done():
		return nil, errJobAborted
	}
}

// Notify passes a Step message broadcasted by the master to the worker waiting
// on the barrier specified by the Step type.
func (b *workerStepBarrier) Notify(step *proto.Step) error {
	ch, exists := b.waitCh[step.Type]
	if !exists {
		return xerrors.Errorf("unsupported step type %q", proto.Step_Type_name[int32(step.Type)])
	}

	// Notify waiter
	select {
	case ch <- step:
		return nil
	case <-b.ctx.Done():
		return errJobAborted
	}
}
