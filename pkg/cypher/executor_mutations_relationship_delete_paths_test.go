package cypher

import (
	"context"
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPublicDeleteProjectionRows(t *testing.T) {
	for _, p := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(p.name, func(t *testing.T) {
			t.Cleanup(p.setup())
			t.Run("node duplicates and literal", func(t *testing.T) {
				e, _ := newTestExecutor(t)
				_, err := e.Execute(t.Context(), `CREATE(:Victim{name:'v'})`, nil)
				require.NoError(t, err)
				r, err := e.Execute(t.Context(), `MATCH(n:Victim) UNWIND [1,2] AS duplicate DELETE n RETURN count(*),count(n),42 AS literal`, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(2), int64(2), int64(42)}}, r.Rows)
				require.Equal(t, 1, r.Stats.NodesDeleted)
			})
			t.Run("deleted property errors atomically", func(t *testing.T) {
				e, _ := newTestExecutor(t)
				_, err := e.Execute(t.Context(), `CREATE(:Victim{name:'v'})`, nil)
				require.NoError(t, err)
				_, err = e.Execute(t.Context(), `MATCH(n:Victim) DELETE n RETURN n.name`, nil)
				requireDeletedEntityError(t, err)
				r, err := e.Execute(t.Context(), `MATCH(n:Victim) RETURN count(n)`, nil)
				require.NoError(t, err)
				require.EqualValues(t, 1, r.Rows[0][0])
			})
			t.Run("edge duplicates empty view and type", func(t *testing.T) {
				e, _ := newTestExecutor(t)
				_, err := e.Execute(t.Context(), `CREATE(:N)-[:R{weight:7}]->(:N)`, nil)
				require.NoError(t, err)
				r, err := e.Execute(t.Context(), `MATCH()-[r:R]->() UNWIND [1,2,3] AS duplicate DELETE r RETURN count(r),r,type(r)`, nil)
				require.NoError(t, err)
				require.EqualValues(t, 3, r.Rows[0][0])
				require.Equal(t, "R", r.Rows[0][2])
				edge, ok := r.Rows[0][1].(*storage.Edge)
				require.True(t, ok, "deleted edge identity stays typed: %T", r.Rows[0][1])
				require.Empty(t, edge.Properties)
				require.Equal(t, 1, r.Stats.RelationshipsDeleted)
				requirePublicDeleteEdges(t, e, 0)
			})
			t.Run("mixed targets", func(t *testing.T) {
				e, _ := newTestExecutor(t)
				_, err := e.Execute(t.Context(), `CREATE(:Victim)-[:R]->(:Kept)`, nil)
				require.NoError(t, err)
				r, err := e.Execute(t.Context(), `MATCH(n:Victim)-[r:R]->() DELETE n,r RETURN count(n),count(r),count(*)`, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1), int64(1), int64(1)}}, r.Rows)
				require.Equal(t, 1, r.Stats.NodesDeleted)
				require.Equal(t, 1, r.Stats.RelationshipsDeleted)
			})
			t.Run("path targets", func(t *testing.T) {
				e, _ := newTestExecutor(t)
				_, err := e.Execute(t.Context(), `CREATE(:N)-[:R]->(:N)`, nil)
				require.NoError(t, err)
				r, err := e.Execute(t.Context(), `MATCH p=()-->() DELETE p RETURN size(nodes(p))`, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(2)}}, r.Rows)
				require.Equal(t, 2, r.Stats.NodesDeleted)
				requirePublicDeleteEdges(t, e, 0)
			})
		})
	}
}

func TestPublicDeleteTransactionPendingAndSnapshot(t *testing.T) {
	for _, terminal := range []string{"ROLLBACK", "COMMIT"} {
		t.Run("pending/"+terminal, func(t *testing.T) {
			e := seedPublicDeleteGraph(t)
			peer := NewStorageExecutor(e.storage)
			_, err := e.Execute(t.Context(), `CREATE INDEX n_id FOR(n:N) ON(n.id)`, nil)
			require.NoError(t, err)
			beginPublicDeleteTransaction(t, e, t.Context())
			_, err = e.Execute(t.Context(), `MATCH(target:N{id:'b'}) CREATE(source:N{id:'pending'})-[:PENDING]->(target)`, nil)
			require.NoError(t, err)
			r, err := e.Execute(t.Context(), `MATCH(source:N{id:'pending'}) RETURN count(source)`, nil)
			require.NoError(t, err)
			require.EqualValues(t, 1, r.Rows[0][0])
			r, err = peer.Execute(t.Context(), `MATCH(source:N{id:'pending'}) RETURN count(source)`, nil)
			require.NoError(t, err)
			require.EqualValues(t, 0, r.Rows[0][0])
			r, err = e.Execute(t.Context(), `MATCH(source:N{id:'pending'}) MATCH(source)-[rel:PENDING]->() DELETE rel`, nil)
			require.NoError(t, err)
			require.Equal(t, 1, r.Stats.RelationshipsDeleted)
			_, err = e.Execute(t.Context(), terminal, nil)
			require.NoError(t, err)
			requirePublicDeleteEdges(t, peer, 4)
			r, err = peer.Execute(t.Context(), `MATCH(source:N{id:'pending'}) RETURN count(source)`, nil)
			require.NoError(t, err)
			want := 0
			if terminal == "COMMIT" {
				want = 1
			}
			require.EqualValues(t, want, r.Rows[0][0])
		})
	}
	t.Run("snapshot excludes late edge", func(t *testing.T) {
		e := seedPublicDeleteGraph(t)
		peer := NewStorageExecutor(e.storage)
		_, err := e.Execute(t.Context(), `CREATE INDEX n_id FOR(n:N) ON(n.id)`, nil)
		require.NoError(t, err)
		beginPublicDeleteTransaction(t, e, t.Context())
		_, err = peer.Execute(t.Context(), `MATCH(source:N{id:'a'}) CREATE(source)-[:R]->(:N{id:'late'})`, nil)
		require.NoError(t, err)
		r, err := e.Execute(t.Context(), `MATCH(source:N{id:'a'}) MATCH(source)-[rel:R]->() DELETE rel`, nil)
		require.NoError(t, err)
		require.Equal(t, 1, r.Stats.RelationshipsDeleted)
		_, err = e.Execute(t.Context(), "COMMIT", nil)
		require.NoError(t, err)
		requirePublicDeleteEdges(t, peer, 4)
		r, err = peer.Execute(t.Context(), `MATCH(a:N{id:'a'})-[r:R]->(b:N{id:'late'}) RETURN count(r)`, nil)
		require.NoError(t, err)
		require.EqualValues(t, 1, r.Rows[0][0])
	})
}

// The public native transaction uses its own snapshot reads; the retired
// wrapper-method faults were not on that path. Reuse the actual stored-entity
// read boundary to prove prepublication read failure and rollback. This covers
// that native preparation phase, not a claimed adjacency-wrapper invocation.
type deleteReadBoundary struct {
	*storagefault.ReadFailureEngine
	hits atomic.Int64
}

func (b *deleteReadBoundary) WithStoredEntityRead(kind storage.StoredEntityReadKind, id string, read func() error) error {
	b.hits.Add(1)
	return b.ReadFailureEngine.WithStoredEntityRead(kind, id, read)
}

func TestPublicDeleteReadErrors(t *testing.T) {
	for _, direction := range []string{"outgoing", "incoming", "undirected from start", "undirected from end", "target"} {
		t.Run(direction, func(t *testing.T) {
			real := newTestMemoryEngine(t)
			fault := errors.New("selected native read failure")
			faults := storagefault.NewReadFailureEngine(real, real, fault)
			boundary := &deleteReadBoundary{ReadFailureEngine: faults}
			e := NewStorageExecutor(storage.NewNamespacedEngine(faults, "delete_read_fault"))
			_, err := e.Execute(t.Context(), `CREATE(:N{id:'a'})-[:R]->(:N{id:'b'})`, nil)
			require.NoError(t, err)
			require.NoError(t, real.InitializeStoredEntityObserver("delete_read_fault", func(storage.StoredEntitySnapshot) (storage.StoredEntityObserver, error) {
				return func(storage.StoredEntityPublication) (func(), error) { return nil, nil }, nil
			}, boundary))
			operation := storagefault.EdgeRead
			pattern := `(a:N{id:'a'})-[r:R]->(target)`
			targets := "r"
			switch direction {
			case "incoming":
				pattern = `(b:N{id:'b'})<-[r:R]-(target)`
			case "undirected from start":
				pattern = `(a:N{id:'a'})-[r:R]-(target)`
			case "undirected from end":
				pattern = `(b:N{id:'b'})-[r:R]-(target)`
			case "target":
				operation = storagefault.NodeRead
				targets = "r,target"
			}
			faults.SetFailure(operation)
			_, err = e.Execute(t.Context(), "MATCH "+pattern+" DELETE "+targets, nil)
			faults.SetFailure("")
			readErr := err
			requirePublicDeleteEdges(t, e, 1)
			result, err := e.Execute(t.Context(), `MATCH(n:N) RETURN count(n)`, nil)
			require.NoError(t, err)
			require.EqualValues(t, 2, result.Rows[0][0])
			require.ErrorIs(t, readErr, fault)
			require.Positive(t, boundary.hits.Load(), "failure must reach the real native read continuation")
		})
	}
}

func TestPublicDeleteDetachAndIndexedSource(t *testing.T) {
	t.Run("detach", func(t *testing.T) {
		e := seedPublicDeleteGraph(t)
		r, err := e.Execute(t.Context(), `MATCH(a:N{id:'a'}) DETACH DELETE a RETURN count(a)`, nil)
		require.NoError(t, err)
		require.Equal(t, 1, r.Stats.NodesDeleted)
		require.Equal(t, 3, r.Stats.RelationshipsDeleted)
		requirePublicDeleteEdges(t, e, 1)
	})
	t.Run("indexed explicit source", func(t *testing.T) {
		e, wrapped := newCountingExecutor(t)
		_, err := e.Execute(t.Context(), `CREATE INDEX repo_id FOR(n:Repository) ON(n.id)`, nil)
		require.NoError(t, err)
		_, err = e.Execute(t.Context(), `UNWIND range(0,499) AS i CREATE(:Repository{id:toString(i)})`, nil)
		require.NoError(t, err)
		beginPublicDeleteTransaction(t, e, t.Context())
		wrapped.reset()
		r, err := e.Execute(t.Context(), `MATCH(source:Repository{id:'0'}) RETURN source.id`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"0"}}, r.Rows)
		require.Zero(t, wrapped.GetNodesByLabelCalls())
		_, err = e.Execute(t.Context(), "ROLLBACK", nil)
		require.NoError(t, err)
	})
}

func TestPublicEmptyDeleteKeepsUnrelatedPopulation(t *testing.T) {
	e, _ := newTestExecutor(t)
	_, err := e.Execute(t.Context(), `UNWIND range(0,99) AS i CREATE(:Repository{id:toString(i)})-[:DEPLOYS_FROM]->(:Repository)`, nil)
	require.NoError(t, err)
	r, err := e.Execute(t.Context(), `MATCH(r:MissingRationale)-[rel:EXPLAINS]->() WHERE r.repo_id IN $ids AND rel.evidence_source IN $sources DELETE rel`, map[string]interface{}{"ids": []string{"missing"}, "sources": []string{"generated"}})
	require.NoError(t, err)
	require.Zero(t, r.Stats.RelationshipsDeleted)
	requirePublicDeleteEdges(t, e, 100)
}

func TestPublicDeleteCancellationPreservesGraph(t *testing.T) {
	e := seedPublicDeleteGraph(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := e.Execute(ctx, `MATCH(a:N{id:'a'})-[r]->() DELETE r RETURN count(r)`, nil)
	require.ErrorIs(t, err, context.Canceled)
	requirePublicDeleteEdges(t, e, 4)
	r, err := e.Execute(t.Context(), `MATCH(a:N{id:'a'})-[r:R]->() DELETE r RETURN count(r)`, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, r.Rows[0][0])
	requirePublicDeleteEdges(t, e, 3)
}

// Public-query baseline: real native explicit snapshot, one indexed source
// returned each time. This replaces a dead private helper workload; its timings
// are a new baseline, not comparable to that component's historical timings.
func BenchmarkPublicDeleteIndexedSource(b *testing.B) {
	for _, count := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			e, wrapped := newCountingExecutor(b)
			ctx := context.Background()
			_, err := e.Execute(ctx, `CREATE INDEX repo_id FOR(r:Repository) ON(r.id)`, nil)
			require.NoError(b, err)
			for i := 0; i < count; i++ {
				_, err = e.Execute(ctx, `CREATE(:Repository{id:$id})`, map[string]interface{}{"id": fmt.Sprint(i)})
				require.NoError(b, err)
			}
			beginPublicDeleteTransaction(b, e, ctx)
			wrapped.reset()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := e.Execute(ctx, `MATCH(source:Repository{id:'0'}) RETURN source.id`, nil)
				if err != nil {
					b.Fatal(err)
				}
				if len(r.Rows) != 1 || r.Rows[0][0] != "0" {
					b.Fatalf("expected one source, got%v", r.Rows)
				}
			}
			b.StopTimer()
			require.Zero(b, wrapped.GetNodesByLabelCalls())
			_, err = e.Execute(ctx, "ROLLBACK", nil)
			require.NoError(b, err)
		})
	}
}

// Register release before opening the public handle, so any assertion failure
// in a test or benchmark cannot strand a snapshot and block engine cleanup.
func beginPublicDeleteTransaction(t testing.TB, e *StorageExecutor, ctx context.Context) {
	t.Helper()
	t.Cleanup(func() {
		if e.txContext != nil && e.txContext.active {
			_, err := e.Execute(context.Background(), "ROLLBACK", nil)
			if err != nil {
				t.Errorf("cleanup active public transaction: %v", err)
			}
		}
	})
	_, err := e.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
}
