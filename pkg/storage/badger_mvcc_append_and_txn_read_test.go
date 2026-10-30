package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMVCCAppendNodeAndEdgeVersions_RoundTripAndNilValidation(t *testing.T) {
	engine := newTestEngine(t)
	v1 := MVCCVersion{CommitTimestamp: time.Unix(10, 0).UTC(), CommitSequence: 1}
	v2 := MVCCVersion{CommitTimestamp: time.Unix(20, 0).UTC(), CommitSequence: 2}

	require.ErrorIs(t, engine.AppendNodeVersion(nil, v1), ErrInvalidData)
	require.ErrorIs(t, engine.AppendEdgeVersion(nil, v1), ErrInvalidData)

	n := &Node{ID: "tenant:n1", Labels: []string{"L"}, Properties: map[string]any{"k": "v1"}}
	e := &Edge{ID: "tenant:e1", StartNode: "tenant:n1", EndNode: "tenant:n2", Type: "REL", Properties: map[string]any{"w": int64(1)}}

	require.NoError(t, engine.AppendNodeVersion(n, v1))
	head, err := engine.GetNodeCurrentHead(n.ID)
	require.NoError(t, err)
	require.Equal(t, uint64(1), head.Version.CommitSequence)
	require.False(t, head.Tombstoned)

	require.NoError(t, engine.AppendNodeTombstone(n.ID, v2))
	head, err = engine.GetNodeCurrentHead(n.ID)
	require.NoError(t, err)
	require.Equal(t, uint64(2), head.Version.CommitSequence)
	require.True(t, head.Tombstoned)

	require.NoError(t, engine.AppendEdgeVersion(e, v1))
	eHead, err := engine.GetEdgeCurrentHead(e.ID)
	require.NoError(t, err)
	require.Equal(t, uint64(1), eHead.Version.CommitSequence)
	require.False(t, eHead.Tombstoned)

	require.NoError(t, engine.AppendEdgeTombstone(e.ID, v2))
	eHead, err = engine.GetEdgeCurrentHead(e.ID)
	require.NoError(t, err)
	require.Equal(t, uint64(2), eHead.Version.CommitSequence)
	require.True(t, eHead.Tombstoned)
}

func TestBadgerTransaction_GetCommittedEdgeLocked_ReadTSBranches(t *testing.T) {
	engine := newTestEngine(t)

	// Seed endpoints in committed state.
	_, err := engine.CreateNode(&Node{ID: "tenant:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "tenant:b", Labels: []string{"N"}})
	require.NoError(t, err)

	// readTS == zero path: missing edge returns ErrNotFound.
	txZero, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, txZero.SetNamespace("tenant"))
	_, err = txZero.getCommittedEdgeLocked("tenant:missing")
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, txZero.Rollback())

	// readTS != zero path: edge committed after reader begin must be hidden.
	txRead, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, txRead.SetNamespace("tenant"))

	txWrite, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, txWrite.SetNamespace("tenant"))
	err = txWrite.CreateEdge(&Edge{ID: "tenant:e1", StartNode: "tenant:a", EndNode: "tenant:b", Type: "REL"})
	require.NoError(t, err)
	require.NoError(t, txWrite.Commit())

	_, err = txRead.getCommittedEdgeLocked("tenant:e1")
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, txRead.Rollback())

	// New reader should see committed edge through visible-at path.
	txAfter, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, txAfter.SetNamespace("tenant"))
	edge, err := txAfter.getCommittedEdgeLocked("tenant:e1")
	require.NoError(t, err)
	require.Equal(t, EdgeID("tenant:e1"), edge.ID)
	require.NoError(t, txAfter.Rollback())
}
