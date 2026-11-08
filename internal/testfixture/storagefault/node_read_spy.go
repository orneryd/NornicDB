package storagefault

import (
	"context"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// NodeReadSpy counts full point reads while preserving the Engine method set.
// In particular it does not advertise lightweight QueryNodeReader support.
type NodeReadSpy struct {
	storage.Engine
	Full atomic.Int64
}

func (s *NodeReadSpy) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	s.Full.Add(1)
	return s.Engine.GetNode(ctx, id)
}

// QueryNodeReadSpy composes full-read counting with lightweight reads. Tests
// that require only Engine use NodeReadSpy without this optional capability.
type QueryNodeReadSpy struct {
	*NodeReadSpy
	Query atomic.Int64
}

func NewQueryNodeReadSpy(engine storage.Engine) *QueryNodeReadSpy {
	return &QueryNodeReadSpy{NodeReadSpy: &NodeReadSpy{Engine: engine}}
}

func (s *QueryNodeReadSpy) GetQueryNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	s.Query.Add(1)
	return s.Engine.(storage.QueryNodeReader).GetQueryNode(ctx, id)
}
