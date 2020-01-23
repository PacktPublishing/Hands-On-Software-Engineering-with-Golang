package job

import (
	"time"

	"github.com/google/uuid"
)

// Details encapsulates the information about a job executed by a master or a
// worker node.
type Details struct {
	// JobID returns a unique ID for this job.
	JobID string

	// CreatedAt returns the creation time for this job.
	CreatedAt time.Time

	// The [start, end) values of the UUID range allocated for this job.
	PartitionFromID uuid.UUID
	PartitionToID   uuid.UUID
}
