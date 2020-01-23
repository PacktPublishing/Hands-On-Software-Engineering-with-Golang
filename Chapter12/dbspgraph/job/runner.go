package job

import "github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/bspgraph"

// Runner is implemented by types that can execute distributed bspgraph jobs.
type Runner interface {
	// StartJob initializes the underlying bspgraph.Graph instance and
	// invokes the provided ExecutorFactory to create an executor for the
	// graph supersteps.
	StartJob(Details, bspgraph.ExecutorFactory) (*bspgraph.Executor, error)

	// CompleteJob is responsible for persisting the locally computed
	// values after a successful execution of a distributed graph algorithm.
	CompleteJob(Details) error

	// AbortJob is responsible for cleaning up the underlying graph after
	// an unsuccessful execution of a distributed graph algorithm.
	AbortJob(Details)
}
