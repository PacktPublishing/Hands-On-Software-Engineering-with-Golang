package dbspgraph

import (
	"context"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/bspgraph"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter12/dbspgraph/proto"
	"github.com/golang/protobuf/ptypes/any"
	"golang.org/x/xerrors"
)

// masterExecutorFactory provides an ExecutorFactory that wraps the user-defined
// executor callbacks for the graph with additional synchronization logic that
// ensures that the graph computation steps are executed in lock-step with the
// connected workers and that the global aggregator state is correctly updated
// and broadcasted to each worker.
type masterExecutorFactory struct {
	serializer Serializer
	barrier    *masterStepBarrier

	origCallbacks bspgraph.ExecutorCallbacks
}

// newMasterExecutorFactory creates a new executor factory for wrapping the
// user-defined executor callback functions with the required master node
// synchronization logic.
func newMasterExecutorFactory(serializer Serializer, barrier *masterStepBarrier) bspgraph.ExecutorFactory {
	f := &masterExecutorFactory{
		serializer: serializer,
		barrier:    barrier,
	}

	return func(g *bspgraph.Graph, cb bspgraph.ExecutorCallbacks) *bspgraph.Executor {
		f.origCallbacks = cb
		patchedCb := bspgraph.ExecutorCallbacks{
			PreStep:             f.preStepCallback,
			PostStep:            f.postStepCallback,
			PostStepKeepRunning: f.postStepKeepRunningCallback,
		}

		return bspgraph.NewExecutor(g, patchedCb)
	}
}

func (f *masterExecutorFactory) preStepCallback(ctx context.Context, g *bspgraph.Graph) error {
	// Wait for all workers to reach the barrier and then allow them to proceed
	if _, err := f.barrier.WaitForWorkers(proto.Step_PRE); err != nil {
		return err
	} else if err := f.barrier.NotifyWorkers(&proto.Step{Type: proto.Step_PRE}); err != nil {
		return err
	}

	if f.origCallbacks.PreStep != nil {
		return f.origCallbacks.PreStep(ctx, g)
	}
	return nil
}

func (f *masterExecutorFactory) postStepCallback(ctx context.Context, g *bspgraph.Graph, activeInStep int) error {
	workerSteps, err := f.barrier.WaitForWorkers(proto.Step_POST)
	if err != nil {
		return err
	}

	// Merge deltas from each individual worker into the global state and
	// broadcast it back to all workers.
	for _, workerStep := range workerSteps {
		if err = mergeWorkerAggregatorDeltas(g, workerStep.AggregatorValues, f.serializer); err != nil {
			return xerrors.Errorf("unable to merge aggregator deltas into global state: %w", err)
		}
	}

	globalAggrValues, err := serializeAggregatorValues(g, f.serializer, false)
	if err != nil {
		return xerrors.Errorf("unable to serialize global aggregator values: %w", err)
	}

	if err := f.barrier.NotifyWorkers(&proto.Step{
		Type:             proto.Step_POST,
		AggregatorValues: globalAggrValues,
	}); err != nil {
		return err
	}

	if f.origCallbacks.PostStep != nil {
		return f.origCallbacks.PostStep(ctx, g, activeInStep)
	}
	return nil
}

func (f *masterExecutorFactory) postStepKeepRunningCallback(ctx context.Context, g *bspgraph.Graph, activeInStep int) (bool, error) {
	workerSteps, err := f.barrier.WaitForWorkers(proto.Step_POST_KEEP_RUNNING)
	if err != nil {
		return false, err
	}

	// Calculate total number of active vertices across all worker
	// instances and broadcast it back.
	for _, workerStep := range workerSteps {
		activeInStep += int(workerStep.ActiveInStep)
	}

	if err := f.barrier.NotifyWorkers(&proto.Step{
		Type:         proto.Step_POST_KEEP_RUNNING,
		ActiveInStep: int64(activeInStep),
	}); err != nil {
		return false, err
	}

	// Master will send back the global activeInStep value that we need to pass
	// to the wrapped callback
	if f.origCallbacks.PostStepKeepRunning != nil {
		return f.origCallbacks.PostStepKeepRunning(ctx, g, activeInStep)
	}
	return true, nil
}

// workerExecutorFactory provides an ExecutorFactory that wraps the
// user-defined executor callbacks for the graph with additional
// synchronization logic that synchronizes with a master node and ensures that
// the graph computation steps are executed in lock-step with other workers.
type workerExecutorFactory struct {
	serializer Serializer
	barrier    *workerStepBarrier

	origCallbacks bspgraph.ExecutorCallbacks
}

// newWorkerExecutorFactory creates a new executor factory for wrapping the
// user-defined executor callback functions with the required worker node
// synchronization logic.
func newWorkerExecutorFactory(serializer Serializer, barrier *workerStepBarrier) bspgraph.ExecutorFactory {
	f := &workerExecutorFactory{
		serializer: serializer,
		barrier:    barrier,
	}

	return func(g *bspgraph.Graph, cb bspgraph.ExecutorCallbacks) *bspgraph.Executor {
		f.origCallbacks = cb
		patchedCb := bspgraph.ExecutorCallbacks{
			PreStep:             f.preStepCallback,
			PostStep:            f.postStepCallback,
			PostStepKeepRunning: f.postStepKeepRunningCallback,
		}

		return bspgraph.NewExecutor(g, patchedCb)
	}
}

func (f *workerExecutorFactory) preStepCallback(ctx context.Context, g *bspgraph.Graph) error {
	// Enter barrier and wait for master to signal us
	if _, err := f.barrier.Wait(&proto.Step{Type: proto.Step_PRE}); err != nil {
		return err
	}

	if f.origCallbacks.PreStep != nil {
		return f.origCallbacks.PreStep(ctx, g)
	}
	return nil
}

func (f *workerExecutorFactory) postStepCallback(ctx context.Context, g *bspgraph.Graph, activeInStep int) error {
	// Send the local change *deltas* to master while entering the barrier.
	aggrValues, err := serializeAggregatorValues(g, f.serializer, true)
	if err != nil {
		return xerrors.Errorf("unable to serialize aggregator deltas")
	}

	stepUpdateMsg, err := f.barrier.Wait(&proto.Step{
		Type:             proto.Step_POST,
		AggregatorValues: aggrValues,
	})
	if err != nil {
		return err
	}

	// Master will send back the new global aggregator values which it
	// calculated by processing the deltas from all workers.
	if err = setAggregatorValues(g, stepUpdateMsg.AggregatorValues, f.serializer); err != nil {
		return err
	}

	if f.origCallbacks.PostStep != nil {
		return f.origCallbacks.PostStep(ctx, g, activeInStep)
	}
	return nil
}

func (f *workerExecutorFactory) postStepKeepRunningCallback(ctx context.Context, g *bspgraph.Graph, activeInStep int) (bool, error) {
	// Send active in step to master and wait for the aggregated
	// active in step value for all workers
	stepUpdateMsg, err := f.barrier.Wait(&proto.Step{
		Type:         proto.Step_POST_KEEP_RUNNING,
		ActiveInStep: int64(activeInStep),
	})
	if err != nil {
		return false, err
	}

	// Master will send back the global activeInStep value that we need to
	// pass to the wrapped callback.
	if f.origCallbacks.PostStepKeepRunning != nil {
		return f.origCallbacks.PostStepKeepRunning(ctx, g, int(stepUpdateMsg.ActiveInStep))
	}
	return true, nil
}

func mergeWorkerAggregatorDeltas(g *bspgraph.Graph, deltaValues map[string]*any.Any, serializer Serializer) error {
	for aggrName, serializedValue := range deltaValues {
		aggr := g.Aggregator(aggrName)
		if aggr == nil {
			return xerrors.Errorf("worker sent a value for aggregator %q which is not known to the local graph instance", aggrName)
		}

		val, err := serializer.Unserialize(serializedValue)
		if err != nil {
			return xerrors.Errorf("unable to unserialize delta value for aggregator %q: %w", aggrName, err)
		}
		aggr.Aggregate(val)
	}
	return nil
}

func serializeAggregatorValues(g *bspgraph.Graph, serializer Serializer, serializeDeltas bool) (map[string]*any.Any, error) {
	aggrMap := g.Aggregators()
	if len(aggrMap) == 0 {
		return nil, nil
	}

	var (
		aggrValues = make(map[string]*any.Any)
		aggrVal    interface{}
	)
	for aggrName, aggr := range aggrMap {
		if serializeDeltas {
			aggrVal = aggr.Delta()
		} else {
			aggrVal = aggr.Get()
		}
		serializedValue, err := serializer.Serialize(aggrVal)
		if err != nil {
			return nil, xerrors.Errorf("unable to serialize value for aggregator %q: %w", aggrName, err)
		}

		aggrValues[aggrName] = serializedValue
	}

	return aggrValues, nil
}

func setAggregatorValues(g *bspgraph.Graph, aggrValues map[string]*any.Any, serializer Serializer) error {
	for aggrName, serializedValue := range aggrValues {
		aggr := g.Aggregator(aggrName)
		if aggr == nil {
			return xerrors.Errorf("master sent a value for aggregator %q which is not known to the local graph instance", aggrName)
		}

		val, err := serializer.Unserialize(serializedValue)
		if err != nil {
			return xerrors.Errorf("unable to unserialize value for aggregator %q: %w", aggrName, err)
		}
		aggr.Set(val)
	}
	return nil
}
