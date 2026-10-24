package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A bound MATCH must not depend on a label-free candidate scan at all. Keep
// the labelled seed capability available so both scan contracts are exercised.
type boundMatchCandidateEngine struct {
	storage.Engine
	scans int
}

func (s *boundMatchCandidateEngine) StreamNodes(context.Context, func(*storage.Node) error) error {
	s.scans++
	return errors.New("unexpected candidate scan for a bound node")
}

func (s *boundMatchCandidateEngine) AllNodes() ([]*storage.Node, error) {
	s.scans++
	return nil, errors.New("unexpected candidate scan for a bound node")
}

func (s *boundMatchCandidateEngine) StreamNodesByLabelProjected(label string, _ []string, visit func(*storage.Node) error) error {
	nodes, err := s.Engine.GetNodesByLabel(label)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if err := visit(node); err != nil {
			return err
		}
	}
	return nil
}

func newBoundMatchCandidateStore(t testing.TB, size int) *storage.NamespacedEngine {
	t.Helper()
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "bound_match")
	for i := 0; i < size; i++ {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"Item"}, Properties: map[string]interface{}{"k": int64(1)}})
		require.NoError(t, err)
	}
	return store
}

func TestBoundNodeMatchSkipsCandidateScan(t *testing.T) {
	store := newBoundMatchCandidateStore(t, 2)
	node, err := store.GetNode("n0")
	require.NoError(t, err)
	spy := &boundMatchCandidateEngine{Engine: store}
	executor := NewStorageExecutor(spy)
	executor.cache = nil
	for _, test := range []struct {
		query string
		rows  [][]interface{}
	}{
		{"MATCH (n:Item) WITH n MATCH (n) RETURN n.k AS k", [][]interface{}{{int64(1)}, {int64(1)}}},
		{"MATCH (n:Item), (n) RETURN n.k AS k", [][]interface{}{{int64(1)}, {int64(1)}}},
		{"MATCH (n:Item) WITH n MATCH (n), (m:Item) RETURN n.k AS k, m.k AS other", [][]interface{}{{int64(1), int64(1)}, {int64(1), int64(1)}, {int64(1), int64(1)}, {int64(1), int64(1)}}},
		{"WITH $node AS n MATCH (n) RETURN n.k AS k", [][]interface{}{{int64(1)}}},
		{"WITH null AS n MATCH (n) RETURN n.k AS k", [][]interface{}{}},
		{"WITH null AS n MATCH (n), (m) RETURN n.k AS k", [][]interface{}{}},
	} {
		t.Run(test.query, func(t *testing.T) {
			spy.scans = 0
			result, err := executor.Execute(context.Background(), test.query, map[string]interface{}{"node": node})
			require.NoError(t, err)
			if len(test.rows) == 0 {
				require.Empty(t, result.Rows)
			} else {
				require.Equal(t, test.rows, result.Rows)
			}
			require.Zero(t, spy.scans, "a bound MATCH reuses its input instead of discovering unbound candidates")
		})
	}
}

func BenchmarkBoundNodeMatchCandidateReuse(b *testing.B) {
	store := newBoundMatchCandidateStore(b, 1000)
	executor := NewStorageExecutor(store)
	executor.cache = nil
	ctx := context.Background()
	query := "MATCH (n:Item) WITH n MATCH (n) RETURN sum(n.k) AS total"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := executor.Execute(ctx, query, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 || result.Rows[0][0] != int64(1000) {
			b.Fatalf("bound-node sum changed: %v", result.Rows)
		}
	}
}
