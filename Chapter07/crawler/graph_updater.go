package crawler

import (
	"context"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/linkgraph/graph"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter07/pipeline"
)

type graphUpdater struct {
	updater Graph
}

func newGraphUpdater(updater Graph) *graphUpdater {
	return &graphUpdater{
		updater: updater,
	}
}

func (u *graphUpdater) Process(ctx context.Context, p pipeline.Payload) (pipeline.Payload, error) {
	payload := p.(*crawlerPayload)

	src := &graph.Link{
		ID:          payload.LinkID,
		URL:         payload.URL,
		RetrievedAt: time.Now(),
	}
	if err := u.updater.UpsertLink(src); err != nil {
		return nil, err
	}

	// Upsert discovered no-follow links without creating an edge
	for _, dstLink := range payload.NoFollowLinks {
		dst := &graph.Link{URL: dstLink}
		if err := u.updater.UpsertLink(dst); err != nil {
			return nil, err
		}
	}

	// Upsert discovered links and create edges for them. Keep track of
	// the current time so we can drop stale edges that have not been
	// updated after this loop.
	removeEdgesOlderThan := time.Now()
	for _, dstLink := range payload.Links {
		dst := &graph.Link{URL: dstLink}

		if err := u.updater.UpsertLink(dst); err != nil {
			return nil, err
		}

		if err := u.updater.UpsertEdge(&graph.Edge{Src: src.ID, Dst: dst.ID}); err != nil {
			return nil, err
		}
	}

	// Drop stale edges that were not touched while upserting the outgoing
	// edges.
	if err := u.updater.RemoveStaleEdges(src.ID, removeEdgesOlderThan); err != nil {
		return nil, err
	}

	return p, nil
}
