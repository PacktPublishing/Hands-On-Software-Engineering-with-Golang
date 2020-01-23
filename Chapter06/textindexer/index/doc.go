package index

import (
	"time"

	"github.com/google/uuid"
)

// Document describes a web-oage whose content has been indexed by Links 'R' Us.
type Document struct {
	// The ID of the linkgraph entry that points to this document.
	LinkID uuid.UUID

	// The URL were the document was obtained from.
	URL string

	// The document title (if available).
	Title string

	// The document body
	Content string

	// The last time this document was indexed.
	IndexedAt time.Time

	// The PageRank score assigned to this document.
	PageRank float64
}
