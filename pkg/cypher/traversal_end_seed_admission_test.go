package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// traversalEndSeedFixture is the population an indexed end-seeded traversal
// reads: enough start-side nodes that the end side is the cheaper orientation,
// and end nodes an index seek returns with different admissions behind them.
//
//   - visible: every label the pattern requires, no internal label.
//   - partial: the indexed end label only, so an id seek returns it while the
//     pattern's second required label is missing.
//   - hidden: every required label plus an internal label, which a user
//     database's MATCH rows hide.
//   - outbound: visible with an outgoing relationship, so the ordinary
//     start-side orientation has a positive counterpart.
//
// visible, hidden and outbound share the kind value their mixture query seeks,
// so one index read hands the traversal admitted and excluded candidates
// together. The fixture is a disposable store owned by the test; nothing here
// writes to a shared or configured database.
func traversalEndSeedFixture(t *testing.T, ctx context.Context, exec *StorageExecutor) {
	t.Helper()
	starts := make([]string, 0, 12)
	for ord := 0; ord < 12; ord++ {
		starts = append(starts, fmt.Sprintf("(s%d:SeedStart {ord: %d})", ord, ord))
	}
	for _, statement := range []string{
		`CREATE ` + strings.Join(starts, ", ") + `,
			(s0)-[:SEED_LINK]->(e1:SeedEnd:SeedRequired {id: 'visible', kind: 'mix'}),
			(s1)-[:SEED_LINK]->(e2:SeedEnd {id: 'partial', kind: 'partial'}),
			(s2)-[:SEED_LINK]->(e3:SeedEnd:SeedRequired:_internal {id: 'hidden', kind: 'mix'}),
			(e3)-[:SEED_LINK]->(s9),
			(e0:SeedEnd:SeedRequired {id: 'outbound', kind: 'outbound'})-[:SEED_LINK]->(s10)`,
		"CREATE INDEX seed_end_id FOR (n:SeedEnd) ON (n.id)",
		"CREATE INDEX seed_end_kind FOR (n:SeedEnd) ON (n.kind)",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
}

// TestIndexedEndSeedTraversalKeepsNodeAdmission covers the route that seeds a
// traversal from an indexed end node and walks it reversed
// (chooseTraversalEndSeeds -> traverseFromEndSeeds -> traverseFromNode). The
// walk's own start node is admitted by pipelineNodeMatchesPattern, the same
// acceptance test a start-side scan applies, so reversing a pattern cannot
// return a row whose end pattern rejects the node: every required label is
// present, the inline properties match, and a user database hides internal
// labels. The ordinary start-side orientation is asserted on the same fixture
// (the two indexed-start queries), so both orientations admit their walk start
// through that one test rather than through a second copy of it.
func TestIndexedEndSeedTraversalKeepsNodeAdmission(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		ctx := t.Context()
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "seed_admission_route"), QueryCachePolicy{MaxEntries: 0})
		traversalEndSeedFixture(t, ctx, exec)

		forEachTraversalTransactionMode(t, exec, func(t *testing.T) {
			for _, tc := range []struct {
				name  string
				query string
				rows  [][]interface{}
			}{
				{
					name:  "unindexed endpoint keeps canonical admission",
					query: "MATCH (s:SeedStart {ord: 2})-[:SEED_LINK]->(e:SeedEnd) RETURN e.id AS id",
				},
				{
					name:  "streamed endpoints keep canonical admission",
					query: "MATCH (s:SeedStart)-[:SEED_LINK]->(e:SeedEnd) RETURN e.id AS id ORDER BY id",
					rows:  [][]interface{}{{"partial"}, {"visible"}},
				},
				{
					name:  "admitted end keeps its row",
					query: "MATCH (s:SeedStart)-[:SEED_LINK]->(e:SeedEnd:SeedRequired {id: 'visible'}) RETURN e.id AS id",
					rows:  [][]interface{}{{"visible"}},
				},
				{
					name:  "internal end is hidden",
					query: "MATCH (s:SeedStart)-[:SEED_LINK]->(e:SeedEnd:SeedRequired {id: 'hidden'}) RETURN e.id AS id",
				},
				{
					name:  "every required label is required",
					query: "MATCH (s:SeedStart)-[:SEED_LINK]->(e:SeedEnd:SeedRequired {id: 'partial'}) RETURN e.id AS id",
				},
				{
					name:  "a mixed index window keeps only the admitted end",
					query: "MATCH (s:SeedStart)-[:SEED_LINK]->(e:SeedEnd {kind: 'mix'}) RETURN e.id AS id",
					rows:  [][]interface{}{{"visible"}},
				},
				{
					// The same internal node as a start-side indexed seed: the
					// ordinary orientation admits its walk start through
					// admitTraversalStartNodes.
					name:  "ordinary orientation hides the internal start",
					query: "MATCH (s:SeedEnd {id: 'hidden'})-[:SEED_LINK]->(e:SeedStart {ord: 9}) RETURN e.ord AS ord",
				},
				{
					name:  "ordinary orientation keeps the admitted start",
					query: "MATCH (s:SeedEnd:SeedRequired {id: 'outbound'})-[:SEED_LINK]->(e:SeedStart {ord: 10}) RETURN e.ord AS ord",
					rows:  [][]interface{}{{int64(10)}},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					result, err := exec.Execute(ctx, tc.query, nil)
					require.NoError(t, err, tc.query)
					if len(tc.rows) == 0 {
						require.Empty(t, result.Rows, tc.query)
						return
					}
					require.Equal(t, tc.rows, result.Rows, tc.query)
				})
			}
		})
	})
}

// TestTraversalStartAdmissionKeepsSystemNamespacePolicy pins both sides of the
// admission the seeded route now shares. A user database's walk admits no node
// its pattern hides; the system database, which stores NornicDB's own records,
// admits the same node. Statements cannot read a graph on the system database
// (systemDatabaseStatementError), so the privileged side is read through the
// corrected owner itself, traverseFromNode, over a store this test owns.
func TestTraversalStartAdmissionKeepsSystemNamespacePolicy(t *testing.T) {
	ctx := t.Context()
	const pattern = "(s:SeedStart)-[:SEED_LINK]->(e:SeedEnd:SeedRequired {id: 'hidden'})"

	// seed writes the pattern's population into a store directly: no statement
	// runs on the system database, and the fixture stays test-owned.
	seed := func(t *testing.T, store storage.Engine) {
		t.Helper()
		for _, node := range []*storage.Node{
			{ID: "s0", Labels: []string{"SeedStart"}},
			{ID: "hidden", Labels: []string{"SeedEnd", "SeedRequired", "_internal"}, Properties: map[string]interface{}{"id": "hidden"}},
		} {
			_, err := store.CreateNode(ctx, node)
			require.NoError(t, err, node.ID)
		}
		require.NoError(t, store.CreateEdge(ctx, &storage.Edge{ID: "e1", Type: "SEED_LINK", StartNode: "s0", EndNode: "hidden"}))
	}

	walk := func(t *testing.T, executor *StorageExecutor) []PathResult {
		t.Helper()
		matches := executor.parseTraversalPattern(ctx, pattern)
		require.NotNil(t, matches)
		reversed := reverseTraversalMatch(matches)
		require.NotNil(t, reversed)
		end, err := executor.getQueryNode(ctx, executor.storage, "hidden")
		require.NoError(t, err)
		require.NotNil(t, end)
		return executor.traverseFromNode(ctx, end, reversed)
	}

	systemStore := storage.NewNamespacedEngine(newTestMemoryEngine(t), "system")
	seed(t, systemStore)
	system := NewStorageExecutor(systemStore, QueryCachePolicy{MaxEntries: 0})
	require.False(t, shouldHideSystemNodes(systemStore))
	require.Len(t, walk(t, system), 1, "the system database reads the internal node it stores")

	userStore := storage.NewNamespacedEngine(newTestMemoryEngine(t), "seed_admission_route")
	seed(t, userStore)
	user := NewStorageExecutor(userStore, QueryCachePolicy{MaxEntries: 0})
	require.True(t, shouldHideSystemNodes(userStore))
	require.Empty(t, walk(t, user), "a user database's walk starts from no node its pattern hides")
}

// forEachTraversalTransactionMode runs body in the two modes a statement
// executes in: its own auto-commit transaction, and an explicit transaction the
// caller opened. The population is created before either, so both modes read
// the same store.
func forEachTraversalTransactionMode(t *testing.T, exec *StorageExecutor, body func(t *testing.T)) {
	t.Helper()
	for _, explicit := range []bool{false, true} {
		name := "auto_commit"
		if explicit {
			name = "explicit_transaction"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			if !explicit {
				body(t)
				return
			}
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			t.Cleanup(func() {
				if exec.txContext != nil && exec.txContext.active {
					_, _ = exec.Execute(context.Background(), "ROLLBACK", nil)
				}
			})
			body(t)
			_, err = exec.Execute(ctx, "COMMIT", nil)
			require.NoError(t, err)
		})
	}
}
