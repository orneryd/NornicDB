package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConstraintValueLocksDoNotReacquireEntityAuthority(t *testing.T) {
	for _, edgeCase := range []bool{false, true} {
		for _, constrained := range []bool{false, true} {
			name := "node"
			if edgeCase {
				name = "relationship"
			}
			if constrained {
				name += "/constrained"
			} else {
				name += "/unconstrained"
			}
			t.Run(name, func(t *testing.T) {
				schema := NewSchemaManager()
				kind := ConstraintEntityNode
				label := "Doc"
				if edgeCase {
					kind = ConstraintEntityRelationship
					label = "LINK"
				}
				if constrained {
					require.NoError(t, schema.AddConstraint(Constraint{Name: "unique_value", Type: ConstraintUnique, EntityType: kind, Label: label, Properties: []string{"uid"}}))
				}
				var nodes []NodeID
				var edges []EdgeID
				if edgeCase {
					nodes = []NodeID{"test:a", "test:b"}
					edges = []EdgeID{"test:e"}
				} else {
					nodes = []NodeID{"test:n"}
				}
				releaseEntity, err := schema.acquireUniqueConstraintCommitLocks(context.Background(), "owner", entityCommitKeys(nodes, edges, nil))
				require.NoError(t, err)
				defer releaseEntity()
				var releaseValues func()
				if edgeCase {
					releaseValues, err = schema.acquireUniqueConstraintCommitLocks(context.Background(), "owner", schema.appendConstraintValueKeys(nil, nil, []*Edge{{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: label, Properties: map[string]any{"uid": "value"}}}))
				} else {
					releaseValues, err = schema.acquireUniqueConstraintCommitLocks(context.Background(), "owner", schema.appendConstraintValueKeys(nil, []*Node{{ID: "test:n", Labels: []string{label}, Properties: map[string]any{"uid": "value"}}}, nil))
				}
				require.NoError(t, err)
				defer releaseValues()
				schema.uniqueConstraintCommitLocksMu.Lock()
				holds := make(map[uniqueConstraintLockKey]int)
				for key, lock := range schema.uniqueConstraintCommitLocks {
					holds[key] = lock.holds
				}
				schema.uniqueConstraintCommitLocksMu.Unlock()
				for _, id := range nodes {
					require.Equal(t, 1, holds[uniqueConstraintLockKey{entityKind: 1, entityID: string(id)}], "only canonical entity owner holds node")
				}
				for _, id := range edges {
					require.Equal(t, 1, holds[uniqueConstraintLockKey{entityKind: 2, entityID: string(id)}], "only canonical entity owner holds relationship")
				}
				releaseEntity()
				schema.uniqueConstraintCommitLocksMu.Lock()
				remaining := make(map[uniqueConstraintLockKey]int)
				for key, lock := range schema.uniqueConstraintCommitLocks {
					remaining[key] = lock.holds
				}
				schema.uniqueConstraintCommitLocksMu.Unlock()
				if constrained {
					require.Len(t, remaining, 1)
					for key, count := range remaining {
						require.Zero(t, key.entityKind)
						require.Equal(t, 1, count)
					}
				} else {
					require.Empty(t, remaining)
				}
				releaseValues()
				schema.uniqueConstraintCommitLocksMu.Lock()
				empty := len(schema.uniqueConstraintCommitLocks) == 0
				schema.uniqueConstraintCommitLocksMu.Unlock()
				require.True(t, empty)
			})
		}
	}
}

// entityAuthorityHold is the authority one canonical registry entry grants. A
// node key carries either the exclusive owner counter (node write or delete) or
// shared relationship-endpoint readers; both share one canonical identity.
type entityAuthorityHold struct {
	exclusive, shared int
}

func (h entityAuthorityHold) total() int { return h.exclusive + h.shared }

// entityAuthoritySnapshot copies every canonical registry entry under the
// registry mutex. It reports the raw counters so a test asserts the exact mode
// and strength of a hold instead of trusting a summary.
func entityAuthoritySnapshot(schema *SchemaManager) map[uniqueConstraintLockKey]entityAuthorityHold {
	schema.uniqueConstraintCommitLocksMu.Lock()
	defer schema.uniqueConstraintCommitLocksMu.Unlock()
	out := make(map[uniqueConstraintLockKey]entityAuthorityHold)
	for key, lock := range schema.uniqueConstraintCommitLocks {
		out[key] = entityAuthorityHold{exclusive: lock.holds, shared: lock.readerCount()}
	}
	return out
}

// TestPreparedEntityAuthorityRetainsOnlyItsCanonicalHold pins the authority a
// prepared entity commit owns. A prepared node commit owns its node and its
// constraint values exclusively. A prepared relationship commit owns the
// relationship and the relationship's value keys exclusively and holds each
// unchanged endpoint only as shared reader authority, so independent prepared
// relationships still commit over the same endpoint. Holds are recorded against
// the canonical key identity, never a per-mode duplicate, and must actually
// exclude a conflicting node write until the owner is gone.
func TestPreparedEntityAuthorityRetainsOnlyItsCanonicalHold(t *testing.T) {
	for _, name := range []string{"node", "relationship", "mixed"} {
		relationship := name == "relationship"
		mixed := name == "mixed"
		t.Run(name, func(t *testing.T) {
			b := newTestEngine(t)
			schema := b.GetSchemaForNamespace("test")
			kind := ConstraintEntityNode
			label := "Doc"
			if relationship {
				kind = ConstraintEntityRelationship
				label = "LINK"
			}
			require.NoError(t, schema.AddConstraint(Constraint{Name: "prepared_unique", Type: ConstraintUnique, EntityType: kind, Label: label, Properties: []string{"uid"}}))
			if mixed {
				require.NoError(t, schema.AddConstraint(Constraint{Name: "prepared_relationship_unique", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "LINK", Properties: []string{"uid"}}))
			}
			if relationship || mixed {
				for _, id := range []NodeID{"test:a", "test:b"} {
					_, err := b.CreateNode(&Node{ID: id})
					require.NoError(t, err)
				}
			}
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.SetNamespace("test"))
			if relationship || mixed {
				require.NoError(t, tx.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"uid": "value"}}))
			}
			if !relationship {
				_, err = tx.CreateNode(&Node{ID: "test:n", Labels: []string{"Doc"}, Properties: map[string]any{"uid": "value"}})
				require.NoError(t, err)
			}
			p, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: tx.ID})
			require.NoError(t, err)
			defer p.Abort()

			// Endpoint authority is held once for the prepared commit window and is
			// not regranted by accepted-command retention; exclusive entity and
			// value authority is reentrant and doubles.
			endpoint := relationship || mixed
			preparedExpected := map[uniqueConstraintLockKey]entityAuthorityHold{
				{entityKind: 1, entityID: "test:n"}: {exclusive: 1},
			}
			if relationship {
				preparedExpected = map[uniqueConstraintLockKey]entityAuthorityHold{
					{entityKind: 1, entityID: "test:a"}: {shared: 1},
					{entityKind: 1, entityID: "test:b"}: {shared: 1},
					{entityKind: 2, entityID: "test:e"}: {exclusive: 1},
				}
			}
			if mixed {
				preparedExpected = map[uniqueConstraintLockKey]entityAuthorityHold{
					{entityKind: 1, entityID: "test:a"}: {shared: 1},
					{entityKind: 1, entityID: "test:b"}: {shared: 1},
					{entityKind: 1, entityID: "test:n"}: {exclusive: 1},
					{entityKind: 2, entityID: "test:e"}: {exclusive: 1},
				}
			}
			acceptedExpected := make(map[uniqueConstraintLockKey]entityAuthorityHold, len(preparedExpected))
			for key, hold := range preparedExpected {
				hold.exclusive *= 2
				acceptedExpected[key] = hold
			}
			// preparedValues is every constraint value key the prepared transaction
			// leases for its validated images.
			preparedValues := map[uniqueConstraintLockKey]entityAuthorityHold{
				{label: "Doc", property: "uid", value: "value"}: {exclusive: 1},
			}
			if mixed {
				preparedValues = map[uniqueConstraintLockKey]entityAuthorityHold{
					{label: "Doc", property: "uid", value: "value"}:                       {exclusive: 1},
					{label: "\x00relationship:LINK", property: `["uid"]`, value: "value"}: {exclusive: 1},
				}
			}
			if relationship {
				// Relationship-value keys use the "\x00relationship:" label domain,
				// which keeps a node and a relationship with equal label and value
				// in disjoint key spaces.
				preparedValues = map[uniqueConstraintLockKey]entityAuthorityHold{
					{label: "\x00relationship:LINK", property: `["uid"]`, value: "value"}: {exclusive: 1},
				}
			}
			// entityKeys strips constraint value keys (kind 0) so entity and
			// endpoint authority is compared without their value leases.
			entityKeys := func(snapshot map[uniqueConstraintLockKey]entityAuthorityHold) map[uniqueConstraintLockKey]entityAuthorityHold {
				entities := make(map[uniqueConstraintLockKey]entityAuthorityHold)
				for key, hold := range snapshot {
					if key.entityKind != 0 {
						entities[key] = hold
					}
				}
				return entities
			}
			check := func(stage string, wantEntity map[uniqueConstraintLockKey]entityAuthorityHold, valueExclusive int) {
				t.Helper()
				snapshot := entityAuthoritySnapshot(schema)
				require.NotEmpty(t, snapshot, stage)
				require.Equal(t, wantEntity, entityKeys(snapshot), stage)
				wantValues := make(map[uniqueConstraintLockKey]entityAuthorityHold, len(preparedValues))
				for key, hold := range preparedValues {
					wantValues[key] = entityAuthorityHold{exclusive: valueExclusive * hold.exclusive}
				}
				values := make(map[uniqueConstraintLockKey]entityAuthorityHold)
				for key, hold := range snapshot {
					if key.entityKind == 0 {
						values[key] = hold
					}
				}
				require.Equal(t, wantValues, values, "%s: value keys stay exclusively owned", stage)
			}
			check("prepared", preparedExpected, 1)
			releaseAccepted, err := p.RetainAcceptedAuthority()
			require.NoError(t, err)
			defer releaseAccepted()
			check("accepted", acceptedExpected, 2)
			releaseAccepted()
			check("released", preparedExpected, 1)

			// The retained endpoint authority must be a real hold, not a bookkeeping
			// entry: an independent node write cannot prepare over an endpoint this
			// prepared transaction still stabilizes.
			if endpoint {
				for _, end := range []NodeID{"test:a", "test:b"} {
					blocked, err := b.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, blocked.SetNamespace("test"))
					node, err := blocked.GetNode(end)
					require.NoError(t, err)
					node.Properties = map[string]any{"mutation": "blocked"}
					require.NoError(t, blocked.UpdateGraphNode(NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
					waiter, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
					_, err = blocked.PrepareEntityCommit(waiter, EntityCommitRequest{LogicalID: "endpoint-writer"})
					cancel()
					require.ErrorIs(t, err, context.DeadlineExceeded, "shared endpoint authority must exclude an exclusive node writer")
					// A rejected prepare already detached and closed the transaction.
					require.ErrorIs(t, blocked.Rollback(), ErrTransactionClosed)
				}
			}
			p.Abort()
			require.Empty(t, entityAuthoritySnapshot(schema))
			// With the prepared owner gone, the endpoint write it blocked proceeds.
			if endpoint {
				owner, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, owner.SetNamespace("test"))
				node, err := owner.GetNode("test:a")
				require.NoError(t, err)
				node.Properties = map[string]any{"mutation": "allowed"}
				require.NoError(t, owner.UpdateGraphNode(NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
				require.NoError(t, owner.Commit())
			}
		})
	}
}

// TestPreparedRelationshipEndpointSerializesConflictingWrites drives the shared
// endpoint hold through the public transaction API. While a prepared
// relationship commit owns an endpoint, a node update and a node delete of that
// endpoint must both wait: the exclusive key is granted only after the read
// holds are gone. The waiting writer is observed through the waiter registry
// and the lock-wait hook, never a wall-clock guess, and its resolution after the
// endpoint owner publishes is asserted per mutation kind.
func TestPreparedRelationshipEndpointSerializesConflictingWrites(t *testing.T) {
	b := newTestEngine(t)
	schema := b.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{Name: "edge_unique", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "LINK", Properties: []string{"uid"}}))
	for _, id := range []NodeID{"test:hub", "test:a", "test:b"} {
		_, err := b.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	prepareEdge := func(local, end string) *PreparedEntityCommit {
		tx, err := b.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("test"))
		require.NoError(t, tx.CreateEdge(&Edge{ID: EdgeID(local), StartNode: "test:hub", EndNode: NodeID(end), Type: "LINK", Properties: map[string]any{"uid": local}}))
		p, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: local})
		require.NoError(t, err)
		return p
	}
	waiting := func(owner string) bool {
		schema.uniqueConstraintCommitLocksMu.Lock()
		defer schema.uniqueConstraintCommitLocksMu.Unlock()
		return schema.uniqueConstraintLockWaits[owner] != nil
	}
	type conflict struct {
		name string
		// build stages the conflicting mutation.
		build func(tx *BadgerTransaction) error
		// afterOwner asserts what the blocked prepare reports once the endpoint
		// owner's relationship is published.
		afterOwner func(t *testing.T, err error)
	}
	for _, caseConflict := range []conflict{
		{
			name: "update",
			build: func(tx *BadgerTransaction) error {
				node, err := tx.GetNode("test:a")
				if err != nil {
					return err
				}
				node.Properties = map[string]any{"mutation": "update"}
				return tx.UpdateGraphNode(NodeUpdateRequest{After: node, Policy: ClientNodeEdits})
			},
			// The update touches only the node, so it proceeds on the published
			// endpoint and commits.
			afterOwner: func(t *testing.T, err error) { require.NoError(t, err) },
		},
		{
			name: "delete",
			build: func(tx *BadgerTransaction) error {
				return tx.DeleteGraphNode("test:a", CascadeEdges)
			},
			// The cascade delete also removes the relationship the owner just
			// published, which this delete's snapshot predates: it is admitted by
			// the key authority and then rejected by the engine's existing snapshot
			// conflict rather than silently applied.
			afterOwner: func(t *testing.T, err error) { require.ErrorIs(t, err, ErrConflict) },
		},
	} {
		t.Run(caseConflict.name, func(t *testing.T) {
			owner := prepareEdge("test:e-"+caseConflict.name, "test:a")
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetNamespace("test"))
			require.NoError(t, caseConflict.build(tx))
			enrolled := make(chan struct{})
			var once bool
			restore := SetUniqueKeyLockWaitHookForTesting(func() {
				if !once {
					once = true
					close(enrolled)
				}
			})
			defer restore()
			type result struct {
				p   *PreparedEntityCommit
				err error
			}
			done := make(chan result, 1)
			go func() {
				p, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: "endpoint-node-" + caseConflict.name})
				done <- result{p, err}
			}()
			select {
			case <-enrolled:
			case <-time.After(5 * time.Second):
				t.Fatal("conflicting write never enrolled in the endpoint wait")
			}
			require.Eventually(t, func() bool { return waiting(tx.ID) }, time.Second, time.Millisecond)
			select {
			case got := <-done:
				if got.p != nil {
					got.p.Abort()
				}
				t.Fatalf("conflicting write prepared while the endpoint was held: %v", got.err)
			case <-time.After(50 * time.Millisecond):
			}
			// Publishing the relationship releases its endpoint hold.
			require.NoError(t, owner.Commit(nil))
			got := <-done
			caseConflict.afterOwner(t, got.err)
			if got.p != nil {
				require.NoError(t, got.p.Commit(nil))
			}
			require.Empty(t, entityAuthoritySnapshot(schema))
		})
	}

	// An unrelated prepared relationship over the same hub and a different end
	// is not serialized behind the endpoint hold: shared authority is real but
	// not exclusive.
	held := prepareEdge("test:e-held", "test:a")
	other := prepareEdge("test:e-other", "test:b")
	require.NoError(t, other.Commit(nil))
	held.Abort()
	require.Empty(t, entityAuthoritySnapshot(schema))
}
