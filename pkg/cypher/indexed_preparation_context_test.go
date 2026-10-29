package cypher

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Public statements must reach the preparation owner's own observation, through
// executor context wrappers, before any result callback or MERGE creation.
func TestIndexedStatementPreparationCancellation(t *testing.T) {
	for _, tc := range []struct{ name, index, query, owner string }{
		{"schema committed filtering", "CREATE INDEX p_k FOR (n:P) ON (n.k)", "MATCH (n:P) WHERE n.k = 'committed' RETURN n.k", "lookupLockedWithContext"},
		{"composite committed copy", "CREATE INDEX p_kl FOR (n:P) ON (n.k, n.l)", "MATCH (n:P {k: 'committed', l: 1}) RETURN n.k", "copySliceWithContext"},
		{"ordered key preparation", "CREATE INDEX p_k FOR (n:P) ON (n.k)", "MATCH (n:P) WHERE n.k IS NOT NULL RETURN n.k", "sortedKeysViewLockedWithContext"},
		{"ordered group snapshot", "CREATE INDEX p_k FOR (n:P) ON (n.k)", "MATCH (n:P) WHERE n.k IS NOT NULL RETURN n.k ORDER BY n.k LIMIT 1", "VisitPropertyIndexGroupsInRangeWithContext.func1"},
		{"ordered candidate collection", "CREATE INDEX p_k FOR (n:P) ON (n.k)", "MATCH (n:P) WHERE n.k >= 'committed' RETURN n.k", "orderedIDsLockedWithContext"},
		{"where first build", "CREATE INDEX p_k FOR (n:P) ON (n.k)", "MATCH (n:P) WHERE n.k = 'pending' RETURN n.k", "pendingValueMatchesLockedWithContext"},
		{"inline first build", "CREATE INDEX p_k FOR (n:P) ON (n.k)", "MATCH (n:P {k: 'pending'}) RETURN n.k", "pendingValueMatchesLockedWithContext"},
		// The residual property prevents the arity-one composite shortcut, so
		// MERGE must use the property matcher and its first pending-index build.
		{"merge first build", "CREATE INDEX p_k FOR (n:P) ON (n.k)", "MERGE (n:P {k: 'pending', l: 1}) RETURN n.k", "pendingValueMatchesLockedWithContext"},
		{"where first pair", "CREATE INDEX p_k FOR (n:P) ON (n.k)", "MATCH (n:P) WHERE n.k = 'pending' RETURN n.k", "trackWithContext"},
		{"where pending collection", "CREATE INDEX p_k FOR (n:P) ON (n.k)", "MATCH (n:P) WHERE n.k = 'pending' RETURN n.k", "MergePendingPropertyMatchesWithContext"},
		{"where committed filter", "CREATE INDEX p_k FOR (n:P) ON (n.k)", "MATCH (n:P) WHERE n.k = 'committed' RETURN n.k", "MergePendingPropertyMatchesWithContext"},
		{"composite inline pending", "CREATE INDEX p_kl FOR (n:P) ON (n.k, n.l)", "MATCH (n:P {k: 'pending', l: 1}) RETURN n.k", "MergePendingCompositeMatchesWithContext"},
		{"composite merge pending", "CREATE INDEX p_kl FOR (n:P) ON (n.k, n.l)", "MERGE (n:P {k: 'pending', l: 1}) RETURN n.k", "MergePendingCompositeMatchesWithContext"},
		{"composite committed filter", "CREATE INDEX p_kl FOR (n:P) ON (n.k, n.l)", "MATCH (n:P {k: 'committed', l: 1}) RETURN n.k", "MergePendingCompositeMatchesWithContext"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			e := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
			run := func(query string) *ExecuteResult {
				t.Helper()
				result, err := e.Execute(context.Background(), query, nil)
				require.NoError(t, err, query)
				return result
			}
			run(tc.index)
			run("CREATE (:P {k: 'committed', l: 1})")
			t.Cleanup(func() { _, _ = e.Execute(context.Background(), "ROLLBACK", nil) })
			// No indexed lookup occurs before this first read. Other-label nodes
			// also force the index builder to enumerate their pending records.
			prepare := func() {
				run("BEGIN")
				if strings.HasPrefix(tc.name, "ordered ") {
					return
				}
				run("UNWIND range(1, 12) AS i CREATE (:Other {v: i})")
				run("CREATE (:P {k: 'pending', l: 1})")
			}
			prepare()
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: tc.owner, cancel: cancel}
			result, err := e.Execute(ctx, tc.query, nil)
			require.GreaterOrEqual(t, ctx.hits, 2, "public indexed statement must reach preparation owner: %v", err)
			require.ErrorIs(t, err, context.Canceled)
			if result != nil {
				require.Empty(t, result.Rows)
			}
			// Public query failure marks an explicit transaction failed. Keep
			// that contract: rollback, then repeat in a fresh transaction.
			run("ROLLBACK")
			prepare()
			retry := run(tc.query)
			expected := 1

			require.Len(t, retry.Rows, expected, "retry must retain the indexed pending candidate")
		})
	}
}

func TestIndexedBufferedPreparationCancellation(t *testing.T) {
	for _, tc := range []struct {
		name, query, owner string
		unique             bool
	}{
		{"namespace pending filter", "MATCH (n:P) WHERE n.k = 1 RETURN n.k", "appendInNamespaceWithContext", false},
		{"ordered pending entries", "MATCH (n:P) WHERE n.k IS NOT NULL RETURN n.k", "propertyEntriesWithContext", false},
		{"unique pending holder", "MERGE (n:P {k: 1}) RETURN n.k", "lookupUniqueValueWithContext", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			e := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
			run := func(query string) *ExecuteResult {
				t.Helper()
				r, err := e.Execute(context.Background(), query, nil)
				require.NoError(t, err, query)
				return r
			}
			if !tc.unique {
				run("CREATE INDEX p_k FOR (n:P) ON (n.k)")
			}
			run("UNWIND range(1,12) AS i CREATE (:P {k:i})")
			if tc.unique {
				run("CREATE CONSTRAINT p_unique FOR (n:P) REQUIRE n.k IS UNIQUE")
			}
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: tc.owner, cancel: cancel}
			result, err := e.Execute(ctx, tc.query, nil)
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			if result != nil {
				require.Empty(t, result.Rows)
			}
			retry := run(tc.query)
			expected := 1
			if tc.name == "ordered pending entries" {
				expected = 12
			}
			require.Len(t, retry.Rows, expected)
		})
	}
}
