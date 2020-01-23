package textindexerapi

import (
	"context"
	"time"

	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter06/textindexer/index"
	"github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter09/linksrus/textindexerapi/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/google/uuid"
)

var _ proto.TextIndexerServer = (*TextIndexerServer)(nil)

// TextIndexerServer provides a gRPC layer for indexing and querying documents.
type TextIndexerServer struct {
	i index.Indexer
}

// NewTextIndexerServer creates a new server instance that uses the provided
// indexer as its backing store.
func NewTextIndexerServer(i index.Indexer) *TextIndexerServer {
	return &TextIndexerServer{i: i}
}

// Index inserts a new document to the index or updates the index entry for
// and existing document.
func (s *TextIndexerServer) Index(_ context.Context, req *proto.Document) (*proto.Document, error) {
	doc := &index.Document{
		LinkID:  uuidFromBytes(req.LinkId),
		URL:     req.Url,
		Title:   req.Title,
		Content: req.Content,
	}

	err := s.i.Index(doc)
	if err != nil {
		return nil, err
	}

	req.IndexedAt = timeToProto(doc.IndexedAt)
	return req, nil
}

// UpdateScore updates the PageRank score for a document with the specified
// link ID.
func (s *TextIndexerServer) UpdateScore(_ context.Context, req *proto.UpdateScoreRequest) (*empty.Empty, error) {
	linkID := uuidFromBytes(req.LinkId)
	return new(empty.Empty), s.i.UpdateScore(linkID, req.PageRankScore)
}

// Search the index for a particular query and stream the results back to the
// client. The first response will include the total result count while all
// subsequent responses will include documents from the resultset.
func (s *TextIndexerServer) Search(req *proto.Query, w proto.TextIndexer_SearchServer) error {
	query := index.Query{
		Type:       index.QueryType(req.Type),
		Expression: req.Expression,
		Offset:     req.Offset,
	}

	it, err := s.i.Search(query)
	if err != nil {
		return err
	}

	// Send back the total document count
	countRes := &proto.QueryResult{
		Result: &proto.QueryResult_DocCount{DocCount: it.TotalCount()},
	}
	if err = w.Send(countRes); err != nil {
		_ = it.Close()
		return err
	}

	// Start streaming
	for it.Next() {
		doc := it.Document()
		res := proto.QueryResult{
			Result: &proto.QueryResult_Doc{
				Doc: &proto.Document{
					LinkId:    doc.LinkID[:],
					Url:       doc.URL,
					Title:     doc.Title,
					Content:   doc.Content,
					IndexedAt: timeToProto(doc.IndexedAt),
				},
			},
		}
		if err = w.SendMsg(&res); err != nil {
			_ = it.Close()
			return err
		}
	}

	if err = it.Error(); err != nil {
		_ = it.Close()
		return err
	}

	return it.Close()
}

func uuidFromBytes(b []byte) uuid.UUID {
	if len(b) != 16 {
		return uuid.Nil
	}

	var dst uuid.UUID
	copy(dst[:], b)
	return dst
}

func timeToProto(t time.Time) *timestamp.Timestamp {
	ts, _ := ptypes.TimestampProto(t)
	return ts
}
