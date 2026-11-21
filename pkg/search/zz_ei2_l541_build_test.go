package search

// Final-build adapters for zz_ei2_l541_test.go (research only, untracked).

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type zzei2FaultEngine struct {
	*nodeSnapshotGateEngine
	fail atomic.Bool
}

func (e *zzei2FaultEngine) IsCurrentTemporalNode(context.Context, *storage.Node, time.Time) (bool, error) {
	if e.fail.Swap(false) {
		return false, context.DeadlineExceeded
	}
	return true, nil
}

func zzei2NewFaultEngine(g *nodeSnapshotGateEngine) *zzei2FaultEngine { return &zzei2FaultEngine{nodeSnapshotGateEngine: g} }
func zzei2SetFail(e *zzei2FaultEngine, v bool)                       { e.fail.Store(v) }
func zzei2CreateNode(t *testing.T, g *nodeSnapshotGateEngine, n *storage.Node) {
	_, err := g.CreateNode(t.Context(), n)
	require.NoError(t, err)
}
func zzei2UpdateNode(t *testing.T, g *nodeSnapshotGateEngine, n *storage.Node) {
	require.NoError(t, g.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: n}))
}
func zzei2Refresh(s *Service, id storage.NodeID) error { return s.RefreshNode(context.Background(), id) }
func zzei2RunRetry(s *Service, w RefreshRetry) error {
	return s.RefreshNodeWithMutation(context.Background(), w.Node)
}
