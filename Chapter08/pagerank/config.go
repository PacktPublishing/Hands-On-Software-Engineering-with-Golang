package pagerank

import (
	multierror "github.com/hashicorp/go-multierror"
	"golang.org/x/xerrors"
)

// Config encapsulates the required parameters for creating a new PageRank
// calculator instance.
type Config struct {
	// DampingFactor is the probability that a random surfer will click on
	// one of the outgoing links on the page they are currently visiting
	// instead of visiting (teleporting to) a random page in the graph.
	//
	// If not specified, a default value of 0.85 will be used instead.
	DampingFactor float64

	// At each step of the iterative PageRank algorithm, an accumulator
	// tracks the sum of absolute differences (SAD) of the PageRank
	// scores for each vertex in the graph.
	//
	// The algorithm will keep executing until the aggregated SAD for all
	// vertices becomes less than MinSADForConvergence.
	//
	// If not specified, a default value of 0.001 will be used instead.
	MinSADForConvergence float64

	// The number of workers to spin up for computing PageRank scores. If
	// not specified, a default value of 1 will be used instead.
	ComputeWorkers int
}

// validate checks whether the PageRank calculator configuration is valid and
// sets the default values where required.
func (c *Config) validate() error {
	var err error
	if c.DampingFactor < 0 || c.DampingFactor > 1.0 {
		err = multierror.Append(err, xerrors.New("DampingFactor must be in the range (0, 1]"))
	} else if c.DampingFactor == 0 {
		c.DampingFactor = 0.85
	}

	if c.MinSADForConvergence < 0 || c.MinSADForConvergence >= 1.0 {
		err = multierror.Append(err, xerrors.New("MinSADForConvergence must be in the range (0, 1)"))
	} else if c.MinSADForConvergence == 0 {
		c.MinSADForConvergence = 0.001
	}

	if c.ComputeWorkers <= 0 {
		c.ComputeWorkers = 1
	}

	return err
}
