package storage

// NOR-377 direct-writer integrity: a direct (non-transactional) writer must
// validate a node or relationship key against the constraint's stored
// candidates in its own publishing transaction together with the
// acknowledged overlay. A stored-only population (the physical label/edge-type
// index) does not contain an acknowledged-but-undrained write, so it admits a
// duplicate of a key the staged transaction validator rejects and every read
// already returns.
//
// The fixture states are the ones the DDL owners leave behind: the schema
// constraint exists, single-property UNIQUE value caches were refreshed by the
// same exported refresh the durable definition path calls, and nothing has
// drained.

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// relatedSynchronousKeyEngine is the stored-only control fixture: no overlay
// exists, so every candidate these tests see is already in the publishing
// transaction.
func relatedSynchronousKeyEngine(t *testing.T, c Constraint) *BadgerEngine {
	t.Helper()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{synchronous: true})
	sm := engine.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraint(c))
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(t.Context(), engine, sm))
	return engine
}

// relatedBufferedUpdate moves an acknowledged buffered node's constraint key.
func relatedBufferedUpdate(t *testing.T, engine *BadgerEngine, id NodeID, properties map[string]any) {
	t.Helper()
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	current, err := tx.GetNode(t.Context(), id)
	require.NoError(t, err)
	moved := &Node{ID: current.ID, Labels: append([]string(nil), current.Labels...), Properties: properties}
	require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: moved, Policy: ClientNodeEdits}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered(), "the move must be an acknowledged buffered write")
}

func relatedBufferedDeleteNode(t *testing.T, engine *BadgerEngine, id NodeID) {
	t.Helper()
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteGraphNode(t.Context(), id, RejectConnected))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered(), "the delete must be an acknowledged buffered write")
}

func relatedBufferedUpdateEdgeToken(t *testing.T, engine *BadgerEngine, id EdgeID, properties map[string]any) {
	t.Helper()
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	current, err := tx.GetEdge(t.Context(), id)
	require.NoError(t, err)
	moved := &Edge{ID: current.ID, Type: current.Type, StartNode: current.StartNode, EndNode: current.EndNode, Properties: properties}
	require.NoError(t, tx.UpdateEdge(t.Context(), EdgeUpdateRequest{After: moved}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered(), "the move must be an acknowledged buffered write")
}

func relatedBufferedDeleteEdge(t *testing.T, engine *BadgerEngine, id EdgeID) {
	t.Helper()
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteEdge(t.Context(), id))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered(), "the delete must be an acknowledged buffered write")
}

// relatedViolation asserts the exact violation the direct decision reports:
// the declared constraint type and the message downstream Bolt classifiers
// match.
func relatedViolation(t *testing.T, err error, wantType ConstraintType, wantMessage string) {
	t.Helper()
	require.Error(t, err, "the direct writer must reject the duplicate")
	var violation *ConstraintViolationError
	require.ErrorAs(t, err, &violation)
	require.Equal(t, wantType, violation.Type)
	require.Contains(t, violation.Message, wantMessage)
	require.Contains(t, err.Error(), wantMessage)
}

// relatedEndpointNodes seeds the two endpoint nodes every relationship test
// needs.
func relatedEndpointNodes(t *testing.T, engine *BadgerEngine) {
	t.Helper()
	for _, id := range []NodeID{"test:a", "test:b"} {
		if _, err := engine.GetNode(t.Context(), id); err == nil {
			continue
		}
		_, err := engine.CreateNode(t.Context(), &Node{ID: id})
		require.NoError(t, err)
	}
}

func relatedAckedKeyEngine(t *testing.T, c Constraint) *BadgerEngine {
	t.Helper()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	sm := engine.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddConstraint(c))
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(t.Context(), engine, sm))
	return engine
}

func relatedAckedBufferedEdge(t *testing.T, engine *BadgerEngine, edge *Edge) {
	t.Helper()
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.CreateEdge(t.Context(), edge))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered(), "the holder must be an acknowledged buffered write")
}

// relatedStagedDuplicateRejected is the control that the transaction validator
// still rejects the duplicate of the acknowledged key.
func relatedStagedDuplicateRejected(t *testing.T, engine *BadgerEngine, node *Node, edge *Edge) {
	t.Helper()
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	var writeErr error
	if node != nil {
		_, writeErr = tx.CreateNode(t.Context(), node)
	} else {
		writeErr = tx.CreateEdge(t.Context(), edge)
	}
	if writeErr == nil {
		writeErr = tx.Commit()
	}
	require.Error(t, writeErr, "the staged transaction validator must reject the duplicate of an acknowledged key")
	_ = tx.Rollback()
}

func TestRelatedDirectNodeUniqueSeesAckedHolder(t *testing.T) {
	engine := relatedAckedKeyEngine(t, Constraint{
		Name: "doc_id_unique", Type: ConstraintUnique, Label: "Doc", Properties: []string{"id"},
	})
	bufferedCreate(t, engine, &Node{ID: "test:holder", Labels: []string{"Doc"}, Properties: map[string]any{"id": "k"}})
	require.Greater(t, engine.writeBehind.PendingOps(), 0, "the holder must stay acknowledged and undrained")

	holder, err := engine.GetNode(t.Context(), "test:holder")
	require.NoError(t, err)
	require.Equal(t, "k", holder.Properties["id"], "every read already returns the acknowledged holder")
	relatedStagedDuplicateRejected(t, engine, &Node{ID: "test:staged", Labels: []string{"Doc"}, Properties: map[string]any{"id": "k"}}, nil)

	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:direct", Labels: []string{"Doc"}, Properties: map[string]any{"id": "k"}})
	require.Error(t, err, "the direct writer must reject a duplicate of an acknowledged UNIQUE holder")
}

func TestRelatedDirectNodeKeySeesAckedHolder(t *testing.T) {
	engine := relatedAckedKeyEngine(t, Constraint{
		Name: "doc_key", Type: ConstraintNodeKey, Label: "DocKey", Properties: []string{"tenant", "id"},
	})
	key := map[string]any{"tenant": "t", "id": "k"}
	bufferedCreate(t, engine, &Node{ID: "test:holder", Labels: []string{"DocKey"}, Properties: key})
	require.Greater(t, engine.writeBehind.PendingOps(), 0)

	holder, err := engine.GetNode(t.Context(), "test:holder")
	require.NoError(t, err)
	require.Equal(t, "k", holder.Properties["id"])
	relatedStagedDuplicateRejected(t, engine, &Node{ID: "test:staged", Labels: []string{"DocKey"}, Properties: map[string]any{"tenant": "t", "id": "k"}}, nil)

	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:direct", Labels: []string{"DocKey"}, Properties: map[string]any{"tenant": "t", "id": "k"}})
	require.Error(t, err, "the direct writer must reject a duplicate of an acknowledged NODE KEY holder")
}

func TestRelatedDirectRelationshipUniqueSeesAckedHolder(t *testing.T) {
	engine := relatedAckedKeyEngine(t, Constraint{
		Name: "link_token_unique", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship,
		Label: "LINK", Properties: []string{"token"},
	})
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id})
		require.NoError(t, err)
	}
	relatedAckedBufferedEdge(t, engine, &Edge{
		ID: "test:holder", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
		Properties: map[string]any{"token": "tok"},
	})
	require.Greater(t, engine.writeBehind.PendingOps(), 0)

	holder, err := engine.GetEdge(t.Context(), "test:holder")
	require.NoError(t, err)
	require.Equal(t, "tok", holder.Properties["token"], "every read already returns the acknowledged holder")
	relatedStagedDuplicateRejected(t, engine, nil, &Edge{
		ID: "test:staged", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
		Properties: map[string]any{"token": "tok"},
	})

	err = engine.CreateEdge(t.Context(), &Edge{
		ID: "test:direct", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
		Properties: map[string]any{"token": "tok"},
	})
	require.Error(t, err, "the direct writer must reject a duplicate of an acknowledged relationship UNIQUE holder")
}

func TestRelatedDirectRelationshipKeySeesAckedHolder(t *testing.T) {
	engine := relatedAckedKeyEngine(t, Constraint{
		Name: "link_key", Type: ConstraintRelationshipKey, EntityType: ConstraintEntityRelationship,
		Label: "LINK", Properties: []string{"tenant", "token"},
	})
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id})
		require.NoError(t, err)
	}
	relatedAckedBufferedEdge(t, engine, &Edge{
		ID: "test:holder", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
		Properties: map[string]any{"tenant": "t", "token": "tok"},
	})
	require.Greater(t, engine.writeBehind.PendingOps(), 0)

	holder, err := engine.GetEdge(t.Context(), "test:holder")
	require.NoError(t, err)
	require.Equal(t, "tok", holder.Properties["token"])
	relatedStagedDuplicateRejected(t, engine, nil, &Edge{
		ID: "test:staged", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
		Properties: map[string]any{"tenant": "t", "token": "tok"},
	})

	err = engine.CreateEdge(t.Context(), &Edge{
		ID: "test:direct", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
		Properties: map[string]any{"tenant": "t", "token": "tok"},
	})
	require.Error(t, err, "the direct writer must reject a duplicate of an acknowledged RELATIONSHIP KEY holder")
}

func TestRelatedDirectNodeKeyMovedAckedHolderReleasesOldKey(t *testing.T) {
	engine := relatedAckedKeyEngine(t, Constraint{
		Name: "doc_key", Type: ConstraintNodeKey, Label: "DocKey", Properties: []string{"tenant", "id"},
	})
	bufferedCreate(t, engine, &Node{ID: "test:holder", Labels: []string{"DocKey"}, Properties: map[string]any{"tenant": "t", "id": "k"}})
	relatedBufferedUpdate(t, engine, "test:holder", map[string]any{"tenant": "t", "id": "k2"})

	holder, err := engine.GetNode(t.Context(), "test:holder")
	require.NoError(t, err)
	require.Equal(t, "k2", holder.Properties["id"], "the acknowledged move is what every read returns")

	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:old", Labels: []string{"DocKey"}, Properties: map[string]any{"tenant": "t", "id": "k"}})
	require.NoError(t, err, "a key the acknowledged holder moved away from is reusable")

	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:new", Labels: []string{"DocKey"}, Properties: map[string]any{"tenant": "t", "id": "k2"}})
	relatedViolation(t, err, ConstraintNodeKey, "already exists (nodeID: test:holder)")
}

func TestRelatedDirectNodeKeyDeletedAckedHolderReleasesKey(t *testing.T) {
	engine := relatedAckedKeyEngine(t, Constraint{
		Name: "doc_key", Type: ConstraintNodeKey, Label: "DocKey", Properties: []string{"tenant", "id"},
	})
	bufferedCreate(t, engine, &Node{ID: "test:holder", Labels: []string{"DocKey"}, Properties: map[string]any{"tenant": "t", "id": "k"}})
	relatedBufferedDeleteNode(t, engine, "test:holder")

	_, err := engine.GetNode(t.Context(), "test:holder")
	require.True(t, errors.Is(err, ErrNotFound), "the acknowledged delete is what every read returns: %v", err)

	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:reuse", Labels: []string{"DocKey"}, Properties: map[string]any{"tenant": "t", "id": "k"}})
	require.NoError(t, err, "a key the acknowledged holder deleted is reusable")
}

func TestRelatedDirectNodeKeyDistinctValuesSucceed(t *testing.T) {
	engine := relatedAckedKeyEngine(t, Constraint{
		Name: "doc_key", Type: ConstraintNodeKey, Label: "DocKey", Properties: []string{"tenant", "id"},
	})
	bufferedCreate(t, engine, &Node{ID: "test:holder", Labels: []string{"DocKey"}, Properties: map[string]any{"tenant": "t", "id": "k"}})

	_, err := engine.CreateNode(t.Context(), &Node{ID: "test:other-tenant", Labels: []string{"DocKey"}, Properties: map[string]any{"tenant": "t2", "id": "k"}})
	require.NoError(t, err, "a different tenant is a different key")
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:other-id", Labels: []string{"DocKey"}, Properties: map[string]any{"tenant": "t", "id": "k2"}})
	require.NoError(t, err, "a different id is a different key")

	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:duplicate", Labels: []string{"DocKey"}, Properties: map[string]any{"tenant": "t", "id": "k"}})
	relatedViolation(t, err, ConstraintNodeKey, "already exists (nodeID: test:holder)")
}

func TestRelatedDirectNodeKeyNullKeyPropertyRejected(t *testing.T) {
	engine := relatedAckedKeyEngine(t, Constraint{
		Name: "doc_key", Type: ConstraintNodeKey, Label: "DocKey", Properties: []string{"tenant", "id"},
	})

	t.Run("missing", func(t *testing.T) {
		_, err := engine.CreateNode(t.Context(), &Node{ID: "test:missing", Labels: []string{"DocKey"}, Properties: map[string]any{"tenant": "t"}})
		relatedViolation(t, err, ConstraintNodeKey, "NODE KEY property id cannot be null")
	})

	t.Run("null", func(t *testing.T) {
		_, err := engine.CreateNode(t.Context(), &Node{ID: "test:null", Labels: []string{"DocKey"}, Properties: map[string]any{"tenant": "t", "id": nil}})
		relatedViolation(t, err, ConstraintNodeKey, "NODE KEY property id cannot be null")
	})
}

func TestRelatedDirectRelationshipUniqueMovedAckedHolderReleasesToken(t *testing.T) {
	engine := relatedAckedKeyEngine(t, Constraint{
		Name: "link_token_unique", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship,
		Label: "LINK", Properties: []string{"token"},
	})
	relatedEndpointNodes(t, engine)
	relatedAckedBufferedEdge(t, engine, &Edge{
		ID: "test:holder", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
		Properties: map[string]any{"token": "tok"},
	})
	relatedBufferedUpdateEdgeToken(t, engine, "test:holder", map[string]any{"token": "tok2"})

	holder, err := engine.GetEdge(t.Context(), "test:holder")
	require.NoError(t, err)
	require.Equal(t, "tok2", holder.Properties["token"], "the acknowledged move is what every read returns")

	err = engine.CreateEdge(t.Context(), &Edge{
		ID: "test:old", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
		Properties: map[string]any{"token": "tok"},
	})
	require.NoError(t, err, "a token the acknowledged holder moved away from is reusable")

	err = engine.CreateEdge(t.Context(), &Edge{
		ID: "test:new", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
		Properties: map[string]any{"token": "tok2"},
	})
	relatedViolation(t, err, ConstraintUnique, "Relationship with token=tok2 already exists (edgeID: test:holder)")
}

func TestRelatedDirectRelationshipUniqueDeletedAckedHolderReleasesToken(t *testing.T) {
	engine := relatedAckedKeyEngine(t, Constraint{
		Name: "link_token_unique", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship,
		Label: "LINK", Properties: []string{"token"},
	})
	relatedEndpointNodes(t, engine)
	relatedAckedBufferedEdge(t, engine, &Edge{
		ID: "test:holder", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
		Properties: map[string]any{"token": "tok"},
	})
	relatedBufferedDeleteEdge(t, engine, "test:holder")

	_, err := engine.GetEdge(t.Context(), "test:holder")
	require.True(t, errors.Is(err, ErrNotFound), "the acknowledged delete is what every read returns: %v", err)

	err = engine.CreateEdge(t.Context(), &Edge{
		ID: "test:reuse", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
		Properties: map[string]any{"token": "tok"},
	})
	require.NoError(t, err, "a token the acknowledged holder deleted is reusable")
}

func TestRelatedDirectRelationshipKeyDistinctCompositeValuesSucceed(t *testing.T) {
	engine := relatedAckedKeyEngine(t, Constraint{
		Name: "link_key", Type: ConstraintRelationshipKey, EntityType: ConstraintEntityRelationship,
		Label: "LINK", Properties: []string{"tenant", "token"},
	})
	relatedEndpointNodes(t, engine)
	relatedAckedBufferedEdge(t, engine, &Edge{
		ID: "test:holder", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
		Properties: map[string]any{"tenant": "t", "token": "tok"},
	})

	err := engine.CreateEdge(t.Context(), &Edge{
		ID: "test:other-tenant", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
		Properties: map[string]any{"tenant": "t2", "token": "tok"},
	})
	require.NoError(t, err, "a different tenant is a different relationship key")
	err = engine.CreateEdge(t.Context(), &Edge{
		ID: "test:other-token", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
		Properties: map[string]any{"tenant": "t", "token": "tok2"},
	})
	require.NoError(t, err, "a different token is a different relationship key")

	err = engine.CreateEdge(t.Context(), &Edge{
		ID: "test:duplicate", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
		Properties: map[string]any{"tenant": "t", "token": "tok"},
	})
	relatedViolation(t, err, ConstraintRelationshipKey, "duplicate composite key already exists (edgeID: test:holder)")
}

func TestRelatedDirectRelationshipUniqueNullComponentDoesNotViolate(t *testing.T) {
	engine := relatedAckedKeyEngine(t, Constraint{
		Name: "link_composite_unique", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship,
		Label: "LINK", Properties: []string{"tenant", "token"},
	})
	relatedEndpointNodes(t, engine)
	relatedAckedBufferedEdge(t, engine, &Edge{
		ID: "test:holder", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
		Properties: map[string]any{"tenant": "t", "token": "tok"},
	})

	err := engine.CreateEdge(t.Context(), &Edge{
		ID: "test:null-component", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
		Properties: map[string]any{"token": "tok"},
	})
	require.NoError(t, err, "a null composite component does not violate uniqueness")

	err = engine.CreateEdge(t.Context(), &Edge{
		ID: "test:duplicate", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
		Properties: map[string]any{"tenant": "t", "token": "tok"},
	})
	relatedViolation(t, err, ConstraintUnique, "duplicate composite key already exists (edgeID: test:holder)")
}

// TestRelatedSynchronousStoredDuplicatesStillRejected is the stored-only
// control: without an overlay the direct writer's decision is unchanged, and
// distinct values stay admissible.
func TestRelatedSynchronousStoredDuplicatesStillRejected(t *testing.T) {
	t.Run("node unique", func(t *testing.T) {
		engine := relatedSynchronousKeyEngine(t, Constraint{
			Name: "doc_id_unique", Type: ConstraintUnique, Label: "Doc", Properties: []string{"id"},
		})
		_, err := engine.CreateNode(t.Context(), &Node{ID: "test:holder", Labels: []string{"Doc"}, Properties: map[string]any{"id": "k"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: "test:duplicate", Labels: []string{"Doc"}, Properties: map[string]any{"id": "k"}})
		relatedViolation(t, err, ConstraintUnique, "k")
	})

	t.Run("node key", func(t *testing.T) {
		engine := relatedSynchronousKeyEngine(t, Constraint{
			Name: "doc_key", Type: ConstraintNodeKey, Label: "DocKey", Properties: []string{"tenant", "id"},
		})
		_, err := engine.CreateNode(t.Context(), &Node{ID: "test:holder", Labels: []string{"DocKey"}, Properties: map[string]any{"tenant": "t", "id": "k"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: "test:distinct", Labels: []string{"DocKey"}, Properties: map[string]any{"tenant": "t2", "id": "k"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &Node{ID: "test:duplicate", Labels: []string{"DocKey"}, Properties: map[string]any{"tenant": "t", "id": "k"}})
		relatedViolation(t, err, ConstraintNodeKey, "already exists (nodeID: test:holder)")
	})

	t.Run("relationship unique", func(t *testing.T) {
		engine := relatedSynchronousKeyEngine(t, Constraint{
			Name: "link_token_unique", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship,
			Label: "LINK", Properties: []string{"token"},
		})
		relatedEndpointNodes(t, engine)
		err := engine.CreateEdge(t.Context(), &Edge{
			ID: "test:holder", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
			Properties: map[string]any{"token": "tok"},
		})
		require.NoError(t, err)
		err = engine.CreateEdge(t.Context(), &Edge{
			ID: "test:duplicate", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
			Properties: map[string]any{"token": "tok"},
		})
		relatedViolation(t, err, ConstraintUnique, "Relationship with token=tok already exists (edgeID: test:holder)")
	})

	t.Run("relationship key", func(t *testing.T) {
		engine := relatedSynchronousKeyEngine(t, Constraint{
			Name: "link_key", Type: ConstraintRelationshipKey, EntityType: ConstraintEntityRelationship,
			Label: "LINK", Properties: []string{"tenant", "token"},
		})
		relatedEndpointNodes(t, engine)
		err := engine.CreateEdge(t.Context(), &Edge{
			ID: "test:holder", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
			Properties: map[string]any{"tenant": "t", "token": "tok"},
		})
		require.NoError(t, err)
		err = engine.CreateEdge(t.Context(), &Edge{
			ID: "test:distinct", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
			Properties: map[string]any{"tenant": "t2", "token": "tok"},
		})
		require.NoError(t, err)
		err = engine.CreateEdge(t.Context(), &Edge{
			ID: "test:duplicate", Type: "LINK", StartNode: "test:a", EndNode: "test:b",
			Properties: map[string]any{"tenant": "t", "token": "tok"},
		})
		relatedViolation(t, err, ConstraintRelationshipKey, "duplicate composite key already exists (edgeID: test:holder)")
	})
}
