package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
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
			p, err := tx.PrepareEntityCommit(context.Background(), tx.ID)
			require.NoError(t, err)
			defer p.Abort()
			snapshot := func() map[uniqueConstraintLockKey]int {
				schema.uniqueConstraintCommitLocksMu.Lock()
				defer schema.uniqueConstraintCommitLocksMu.Unlock()
				out := make(map[uniqueConstraintLockKey]int)
				for key, lock := range schema.uniqueConstraintCommitLocks {
					out[key] = lock.holds
				}
				return out
			}
			check := func(want int) {
				t.Helper()
				holds := snapshot()
				require.NotEmpty(t, holds)
				for _, count := range holds {
					require.Equal(t, want, count)
				}
			}
			check(1)
			if mixed {
				values := 0
				for key := range snapshot() {
					if key.entityKind == 0 {
						values++
					}
				}
				require.Equal(t, 2, values, "node and relationship same values retain distinct key domains")
			}
			releaseAccepted, err := p.RetainAcceptedAuthority()
			require.NoError(t, err)
			defer releaseAccepted()
			check(2)
			releaseAccepted()
			check(1)
			p.Abort()
			require.Empty(t, snapshot())
		})
	}
}
