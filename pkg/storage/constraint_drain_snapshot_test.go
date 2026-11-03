package storage

import (
	"context"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestDirectConstraintSeesHolderDrainedAfterNativeSnapshot(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{Name: "doc_key", Type: ConstraintNodeKey, Label: "Doc", Properties: []string{"tenant", "id"}}))
	bufferedCreate(t, engine, &Node{ID: "test:holder", Labels: []string{"Doc"}, Properties: map[string]any{"tenant": "t", "id": "k"}})
	candidate := &Node{ID: "test:duplicate", Labels: []string{"Doc"}, Properties: map[string]any{"tenant": "t", "id": "k"}}
	err := engine.withUpdate(t.Context(), updateRequest{}, func(txn *badger.Txn) error {
		require.NoError(t, engine.FlushWriteBehind(context.Background()))
		return engine.validateNodeConstraintsInTxn(t.Context(), txn, candidate, schema, "test", candidate.ID)
	})
	require.Error(t, err, "draining a holder after snapshot creation must not make its key available")
}

func TestDirectRelationshipConstraintSeesHolderDrainedAfterNativeSnapshot(t *testing.T) {
	engine := relatedAckedKeyEngine(t, Constraint{Name: "link_key", Type: ConstraintRelationshipKey, EntityType: ConstraintEntityRelationship, Label: "LINK", Properties: []string{"token"}})
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id})
		require.NoError(t, err)
	}
	relatedAckedBufferedEdge(t, engine, &Edge{ID: "test:holder", Type: "LINK", StartNode: "test:a", EndNode: "test:b", Properties: map[string]any{"token": "k"}})
	candidate := &Edge{ID: "test:duplicate", Type: "LINK", StartNode: "test:a", EndNode: "test:b", Properties: map[string]any{"token": "k"}}
	err := engine.withUpdate(t.Context(), updateRequest{}, func(txn *badger.Txn) error {
		require.NoError(t, engine.FlushWriteBehind(context.Background()))
		return engine.validateEdgeConstraintsInTxn(t.Context(), txn, candidate, engine.GetSchemaForNamespace("test"), "test", candidate.ID)
	})
	require.Error(t, err, "drained relationship holders must remain part of the integrity population")
}
