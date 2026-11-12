package storage

import (
	"context"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func relationshipUniqueCommitFixture(t testing.TB, existing int, constraints ...bool) *BadgerEngine {
	t.Helper()
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err = engine.CreateNode(context.Background(), &Node{ID: id})
		require.NoError(t, err)
	}
	tx, err := engine.BeginTransaction(context.Background())
	require.NoError(t, err)
	for i := 0; i < existing; i++ {
		require.NoError(t, tx.CreateEdge(context.Background(), &Edge{ID: EdgeID(fmt.Sprintf("test:existing%d", i)), StartNode: "test:a", EndNode: "test:b", Type: "T", Properties: map[string]interface{}{"id": int64(i)}}))
	}
	require.NoError(t, tx.Commit())
	if len(constraints) == 0 || constraints[0] {
		require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{Name: "unique_rel_id", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "T", Properties: []string{"id"}}))
	}
	return engine
}
func commitUniqueRelationships(t testing.TB, engine *BadgerEngine, first, count int) {
	t.Helper()
	tx, err := engine.BeginTransaction(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	for i := 0; i < count; i++ {
		require.NoError(t, tx.CreateEdge(context.Background(), &Edge{ID: EdgeID(fmt.Sprintf("test:new%d", first+i)), StartNode: "test:a", EndNode: "test:b", Type: "T", Properties: map[string]interface{}{"id": int64(first + i)}}))
	}
	require.NoError(t, tx.Commit())
}
func TestRelationshipUniqueCommitUsesValueIndex(t *testing.T) {
	engine := relationshipUniqueCommitFixture(t, 128)
	// A first validation may initialize a derived index. Subsequent commits
	// must not enumerate unrelated relationships after each write.
	commitUniqueRelationships(t, engine, 1000, 1)
	var scans atomic.Int64
	restore := setUniqueConstraintScanHook(func() { scans.Add(1) })
	defer restore()
	commitUniqueRelationships(t, engine, 2000, 16)
	require.Zero(t, scans.Load(), "indexed uniqueness must not type-scan per pending relationship")
}
func BenchmarkRelationshipUniqueCommit(b *testing.B) {
	for _, existing := range []int{8448, 32000} {
		for _, pending := range []int{1, 16} {
			b.Run(fmt.Sprintf("existing%d/pending%d", existing, pending), func(b *testing.B) {
				b.StopTimer()
				engine := relationshipUniqueCommitFixture(b, existing)
				commitUniqueRelationships(b, engine, existing+100, 1)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					tx, err := engine.BeginTransaction(b.Context())
					require.NoError(b, err)
					for j := 0; j < pending; j++ {
						id := existing + 1000 + i*pending + j
						require.NoError(b, tx.CreateEdge(b.Context(), &Edge{ID: EdgeID(fmt.Sprintf("test:new%d", id)), StartNode: "test:a", EndNode: "test:b", Type: "T", Properties: map[string]interface{}{"id": int64(id)}}))
					}
					b.StartTimer()
					err = tx.Commit()
					b.StopTimer()
					require.NoError(b, err)
				}
			})
		}
	}
}

func TestRelationshipUniqueIndexPreservesTransactionSemantics(t *testing.T) {
	for _, scenario := range []string{"committed duplicate", "pending duplicate", "delete then recreate", "update then reuse", "rollback", "type change", "other namespace", "numeric equality", "list fallback"} {
		t.Run(scenario, func(t *testing.T) {
			engine := relationshipUniqueCommitFixture(t, 4)
			commitUniqueRelationships(t, engine, 100, 1)
			create := func(tx *BadgerTransaction, id EdgeID, value interface{}) error {
				return tx.CreateEdge(t.Context(), &Edge{ID: id, StartNode: "test:a", EndNode: "test:b", Type: "T", Properties: map[string]interface{}{"id": value}})
			}
			tx, err := engine.BeginTransaction(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			switch scenario {
			case "committed duplicate":
				require.NoError(t, create(tx, "test:duplicate", int64(1)))
				require.Error(t, tx.Commit())
			case "pending duplicate":
				require.NoError(t, create(tx, "test:p1", int64(200)))
				require.NoError(t, create(tx, "test:p2", float64(200)))
				require.Error(t, tx.Commit())
			case "delete then recreate":
				require.NoError(t, tx.DeleteEdge(t.Context(), "test:existing1"))
				require.NoError(t, create(tx, "test:replacement", int64(1)))
				require.NoError(t, tx.Commit())
			case "update then reuse":
				edge, err := tx.GetEdge(t.Context(), "test:existing1")
				require.NoError(t, err)
				edge.Properties["id"] = int64(200)
				require.NoError(t, tx.UpdateEdge(t.Context(), EdgeUpdateRequest{After: edge}))
				require.NoError(t, create(tx, "test:replacement", int64(1)))
				require.NoError(t, tx.Commit())
				next, err := engine.BeginTransaction(t.Context())
				require.NoError(t, err)
				defer next.Rollback()
				require.NoError(t, create(next, "test:dup-new", int64(200)))
				require.Error(t, next.Commit())
			case "rollback":
				require.NoError(t, tx.DeleteEdge(t.Context(), "test:existing1"))
				require.NoError(t, create(tx, "test:rolledback", int64(200)))
				require.NoError(t, tx.Rollback())
				commitUniqueRelationships(t, engine, 200, 1)
				next, err := engine.BeginTransaction(t.Context())
				require.NoError(t, err)
				defer next.Rollback()
				require.NoError(t, create(next, "test:still-duplicate", int64(1)))
				require.Error(t, next.Commit())
			case "type change":
				edge, err := engine.GetEdge(t.Context(), "test:existing1")
				require.NoError(t, err)
				edge.Type = "OTHER"
				require.NoError(t, engine.UpdateEdge(t.Context(), EdgeUpdateRequest{After: edge}))
				require.NoError(t, create(tx, "test:type-replacement", int64(1)))
				require.NoError(t, tx.Commit())
			case "other namespace":
				require.NoError(t, tx.Rollback())
				for _, id := range []NodeID{"other:a", "other:b"} {
					_, err := engine.CreateNode(t.Context(), &Node{ID: id})
					require.NoError(t, err)
				}
				require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "other:edge", Type: "T", StartNode: "other:a", EndNode: "other:b", Properties: map[string]interface{}{"id": int64(200)}}))
				commitUniqueRelationships(t, engine, 200, 1)
			case "numeric equality":
				require.NoError(t, create(tx, "test:numeric", float64(1)))
				require.Error(t, tx.Commit())
			case "list fallback":
				require.NoError(t, create(tx, "test:list1", []interface{}{float64(200)}))
				require.NoError(t, tx.Commit())
				next, err := engine.BeginTransaction(t.Context())
				require.NoError(t, err)
				defer next.Rollback()
				stored, err := engine.GetEdge(t.Context(), "test:list1")
				require.NoError(t, err)
				// Use the durable representation: existing list equality is type-sensitive.
				require.NoError(t, create(next, "test:list2", stored.Properties["id"]))
				require.Error(t, next.Commit())
			}
		})
	}
}
func TestRelationshipUniqueIndexConcurrentCommits(t *testing.T) {
	for _, tc := range []struct {
		name       string
		existing   int
		seed       bool
		direct     bool
		properties func() map[string]interface{}
	}{
		{name: "scalar/transaction", existing: 4, seed: true,
			properties: func() map[string]interface{} { return map[string]interface{}{"id": int64(999)} }},
		{name: "scalar/direct", existing: 4, seed: true, direct: true,
			properties: func() map[string]interface{} { return map[string]interface{}{"id": int64(999)} }},
		{name: "list/transaction", existing: 2,
			properties: func() map[string]interface{} { return map[string]interface{}{"id": []interface{}{float64(200)}} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine := relationshipUniqueCommitFixture(t, tc.existing)
			if tc.seed {
				commitUniqueRelationships(t, engine, 100, 1)
			}
			edges := []*Edge{
				{ID: "test:writer0", Type: "T", StartNode: "test:a", EndNode: "test:b", Properties: tc.properties()},
				{ID: "test:writer1", Type: "T", StartNode: "test:a", EndNode: "test:b", Properties: tc.properties()},
			}
			tx, err := engine.BeginTransaction(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.CreateEdge(t.Context(), edges[0]))
			peer, err := engine.BeginTransaction(t.Context())
			require.NoError(t, err)
			defer peer.Rollback()
			if !tc.direct {
				require.NoError(t, peer.CreateEdge(t.Context(), edges[1]))
			}
			start := make(chan struct{})
			out := make(chan error, 2)
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); <-start; out <- tx.Commit() }()
			go func() {
				defer wg.Done()
				<-start
				if tc.direct {
					out <- engine.CreateEdge(t.Context(), edges[1])
				} else {
					out <- peer.Commit()
				}
			}()
			close(start)
			wg.Wait()
			close(out)
			successes := 0
			for err := range out {
				if err == nil {
					successes++
				} else {
					asConstraintViolation(t, err)
				}
			}
			require.Equal(t, 1, successes)
		})
	}
}
func TestRelationshipUniqueCompositeIndexSignedZero(t *testing.T) {
	engine := relationshipUniqueCommitFixture(t, 0, false)
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{Name: "composite", Type: ConstraintRelationshipKey, EntityType: ConstraintEntityRelationship, Label: "T", Properties: []string{"tenant", "id"}}))
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:zero", Type: "T", StartNode: "test:a", EndNode: "test:b", Properties: map[string]interface{}{"tenant": "x", "id": math.Copysign(0, -1)}}))
	for i := 0; i < 2; i++ {
		tx, err := engine.BeginTransaction(t.Context())
		require.NoError(t, err)
		require.NoError(t, tx.CreateEdge(t.Context(), &Edge{ID: EdgeID(fmt.Sprintf("test:dup%d", i)), Type: "T", StartNode: "test:a", EndNode: "test:b", Properties: map[string]interface{}{"tenant": "x", "id": float64(0)}}))
		require.Error(t, tx.Commit())
		_ = tx.Rollback()
	}
}
func BenchmarkRelationshipUnconstrainedCommit(b *testing.B) {
	b.StopTimer()
	engine := relationshipUniqueCommitFixture(b, 8448, false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := engine.BeginTransaction(b.Context())
		require.NoError(b, err)
		for j := 0; j < 16; j++ {
			id := 10000 + i*16 + j
			require.NoError(b, tx.CreateEdge(b.Context(), &Edge{ID: EdgeID(fmt.Sprintf("test:new%d", id)), StartNode: "test:a", EndNode: "test:b", Type: "T", Properties: map[string]interface{}{"id": int64(id)}}))
		}
		b.StartTimer()
		err = tx.Commit()
		b.StopTimer()
		require.NoError(b, err)
	}
}

func TestRelationshipUniqueNaNUsesScanFallback(t *testing.T) {
	engine := relationshipUniqueCommitFixture(t, 2)
	commitUniqueRelationships(t, engine, 100, 1)
	var scans atomic.Int64
	restore := setUniqueConstraintScanHook(func() { scans.Add(1) })
	defer restore()
	for i := 0; i < 2; i++ {
		tx, err := engine.BeginTransaction(t.Context())
		require.NoError(t, err)
		require.NoError(t, tx.CreateEdge(t.Context(), &Edge{ID: EdgeID(fmt.Sprintf("test:nan%d", i)), Type: "T", StartNode: "test:a", EndNode: "test:b", Properties: map[string]interface{}{"id": math.NaN()}}))
		// NaN remains unequal under the existing constraint comparator. It must
		// not become an unfindable map/commit-lock key in the derived index.
		require.NoError(t, tx.Commit())
	}
	require.Equal(t, int64(2), scans.Load())
}

// Distinct WTF-8 property names are part of the exact UTF-16 identifier
// contract. A runtime index key must not normalize them into one cache slot.
func TestRelationshipUniqueIndexSeparatesSurrogatePropertyNames(t *testing.T) {
	first, second := "\xed\xa0\x80", "\xed\xb0\x80"
	engine := relationshipUniqueCommitFixture(t, 0, false)
	schema := engine.GetSchemaForNamespace("test")
	for i, property := range []string{first, second} {
		require.NoError(t, schema.AddConstraint(Constraint{Name: fmt.Sprintf("surrogate_%d", i), Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "T", Properties: []string{property}}))
	}
	create := func(id EdgeID, a, b string) error {
		tx, err := engine.BeginTransaction(t.Context())
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := tx.CreateEdge(t.Context(), &Edge{ID: id, StartNode: "test:a", EndNode: "test:b", Type: "T", Properties: map[string]interface{}{first: a, second: b}}); err != nil {
			return err
		}
		return tx.Commit()
	}
	require.NoError(t, create("test:first", "a", "x"))
	var violation *ConstraintViolationError
	require.ErrorAs(t, create("test:duplicate", "b", "x"), &violation, "the second property must have its own uniqueness index")
	_, err := engine.GetEdge(t.Context(), "test:duplicate")
	require.ErrorIs(t, err, ErrNotFound, "a rejected duplicate must not be published")
	require.NoError(t, create("test:distinct", "b", "y"))
	require.ErrorAs(t, create("test:duplicate_first", "a", "z"), &violation)
}

func TestRelationshipPropertyKeyPreservesExactIdentity(t *testing.T) {
	sets := [][]string{{"\xed\xa0\x80"}, {"\xed\xb0\x80"}, {"\ufffd\ufffd\ufffd"}, {"a", "b"}, {"a,b"}, {"a\x00b"}, {"a", "", "b"}, {""}}
	seen := make(map[string][]string, len(sets))
	for _, properties := range sets {
		key := relationshipPropertyKey(properties)
		require.NotContains(t, seen, key, "distinct ordered property names must remain distinct")
		require.Equal(t, key, relationshipPropertyKey(append([]string(nil), properties...)))
		seen[key] = properties
	}
}
