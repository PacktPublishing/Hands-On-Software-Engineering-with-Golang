package service

import (
	"encoding/binary"
	"math"

	pr "github.com/PacktPublishing/Hands-On-Software-Engineering-with-Golang/Chapter08/pagerank"
	"github.com/golang/protobuf/ptypes/any"
	"golang.org/x/xerrors"
)

type serializer struct {
}

// Serialize encodes the given value into an any.Any protobuf message.
func (serializer) Serialize(v interface{}) (*any.Any, error) {
	scratchBuf := make([]byte, binary.MaxVarintLen64)
	switch val := v.(type) {
	case int:
		nBytes := binary.PutVarint(scratchBuf, int64(val))
		return &any.Any{
			TypeUrl: "i",
			Value:   scratchBuf[:nBytes],
		}, nil
	case float64:
		nBytes := binary.PutUvarint(scratchBuf, math.Float64bits(val))
		return &any.Any{
			TypeUrl: "f",
			Value:   scratchBuf[:nBytes],
		}, nil
	case pr.IncomingScoreMessage:
		nBytes := binary.PutUvarint(scratchBuf, math.Float64bits(val.Score))
		return &any.Any{
			TypeUrl: "m",
			Value:   scratchBuf[:nBytes],
		}, nil
	default:
		return nil, xerrors.Errorf("serialize: unknown type %#+T", val)
	}
}

// Unserialize decodes the given any.Any protobuf value.
func (serializer) Unserialize(v *any.Any) (interface{}, error) {
	switch v.TypeUrl {
	case "i":
		val, _ := binary.Varint(v.Value)
		return int(val), nil
	case "f":
		val, _ := binary.Uvarint(v.Value)
		return math.Float64frombits(val), nil
	case "m":
		val, _ := binary.Uvarint(v.Value)
		return pr.IncomingScoreMessage{
			Score: math.Float64frombits(val),
		}, nil
	default:
		return nil, xerrors.Errorf("unserialize: unknown type %q", v.TypeUrl)
	}
}
