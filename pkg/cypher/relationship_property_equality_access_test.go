package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The release report's statement: an unlabelled free-end one-hop pattern whose
// relationship is selected by a constrained equality property.
const relationshipEqualityAccessQuery = "MATCH (a)-[r:PD_RELATION {id: $id}]->(b) RETURN r.id, a.id, b.id"

// relationshipEqualityPinnedAuthorityQuery is the same predicate over the
// relationship type index instead of the constrained inline property. It lists
// the type at the reader's cut and filters the value there, so it is the
// authority for what a pinned transaction must see, while the constraint index's
// postings only describe the current state.
const relationshipEqualityPinnedAuthorityQuery = "MATCH (a)-[r:PD_RELATION]->(b) WHERE r.id=$id RETURN r.id,a.id,b.id"

// relationshipEqualityParsers are the two statement parsers the public Execute
// path selects between. Both reach the same relationship seed owner, so every
// case below runs on each.
var relationshipEqualityParsers = []struct {
	name  string
	setup func() func()
}{
	{"nornic", config.WithNornicParser},
	{"antlr", config.WithANTLRParser},
}

// relationshipEqualityAccessFixture is the bounded reproduction of that shape:
// two endpoint nodes, the one relationship the indexed value selects, and the
// unrelated relationships of the same type the case asks for. Regular cases use
// 128 to make the access assertion deterministic: enumerating the type reads and
// filters every one of them, while a relationship property lookup reads the one
// matching relationship. The allocation ladder varies that population instead.
type relationshipEqualityAccessFixture struct {
	exec *StorageExecutor
}

// relationshipEqualityFixtureOptions selects the storage chain and the unrelated
// relationship population one fixture is built with.
type relationshipEqualityFixtureOptions struct {
	unrelatedRelationships int
	durabilityStack        bool
}

func newRelationshipEqualityAccessFixture(t testing.TB, settings relationshipEqualityFixtureOptions) *relationshipEqualityAccessFixture {
	t.Helper()
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	var store storage.Engine = storage.NewNamespacedEngine(base, "pd")
	if settings.durabilityStack {
		wal, err := storage.NewWAL(filepath.Join(t.TempDir(), "wal"), &storage.WALConfig{SyncMode: "none"})
		require.NoError(t, err)
		t.Cleanup(func() { _ = wal.Close() })
		store = storage.NewNamespacedEngine(storage.NewWALEngine(base, wal), "pd")
	}
	return populateRelationshipEqualityFixture(t, store, settings)
}

// populateRelationshipEqualityFixture fills store with the fixture's endpoint
// nodes, its unrelated relationships and the one relationship the indexed value
// selects, and installs the constraint that lets the statement read the index.
func populateRelationshipEqualityFixture(t testing.TB, store storage.Engine, settings relationshipEqualityFixtureOptions) *relationshipEqualityAccessFixture {
	t.Helper()
	ctx := context.Background()
	for _, id := range []string{"n73", "n128561"} {
		_, err := store.CreateNode(ctx, &storage.Node{
			ID:         storage.NodeID(id),
			Labels:     []string{"PDGraphNode"},
			Properties: map[string]interface{}{"id": id, "archived": false},
		})
		require.NoError(t, err)
	}
	createRelationship := func(edgeID, value string) {
		require.NoError(t, store.CreateEdge(ctx, &storage.Edge{
			ID:         storage.EdgeID(edgeID),
			StartNode:  "n73",
			EndNode:    "n128561",
			Type:       "PD_RELATION",
			Properties: map[string]interface{}{"id": value},
		}))
	}
	for i := 0; i < settings.unrelatedRelationships; i++ {
		createRelationship(fmt.Sprintf("unrelated%d", i), fmt.Sprintf("x%d", i))
	}
	createRelationship("target", "r289999")
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	_, err := exec.Execute(ctx, "CREATE CONSTRAINT pd_relation_id IF NOT EXISTS FOR ()-[r:PD_RELATION]-() REQUIRE r.id IS UNIQUE", nil)
	require.NoError(t, err)
	return &relationshipEqualityAccessFixture{exec: exec}
}

// beginRelationshipEqualityTransaction starts the explicit transaction the
// release report measured inside, and rolls it back when the subtest ends.
func beginRelationshipEqualityTransaction(t testing.TB, exec *StorageExecutor) context.Context {
	t.Helper()
	ctx := context.Background()
	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if exec.txContext != nil && exec.txContext.active {
			_, _ = exec.handleRollback(context.Background(), nil)
		}
	})
	return ctx
}

// relationshipEqualityPinnedQueries returns the two statements the pinned
// snapshot controls compare: value reads the constrained inline property, and
// authority reads the same predicate from the relationship type index.
func relationshipEqualityPinnedQueries(t *testing.T, exec *StorageExecutor, ctx context.Context) (value, authority func(string) [][]interface{}) {
	t.Helper()
	statement := func(query string) func(string) [][]interface{} {
		return func(id string) [][]interface{} {
			result, err := exec.Execute(ctx, query, map[string]interface{}{"id": id})
			require.NoError(t, err)
			return result.Rows
		}
	}
	return statement(relationshipEqualityAccessQuery), statement(relationshipEqualityPinnedAuthorityQuery)
}

// TestRelationshipPropertyEqualityReadsTheIndexOfItsConstraint is the release
// defect: with the relationship UNIQUE constraint installed, the statement must
// read the one relationship the index selects — in auto-commit and in an
// explicit read transaction, on each parser — and return that single row, not
// enumerate the type's 128 unrelated relationships.
func TestRelationshipPropertyEqualityReadsTheIndexOfItsConstraint(t *testing.T) {
	for _, parser := range relationshipEqualityParsers {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, transaction := range []struct {
				name     string
				explicit bool
			}{
				{"auto-commit", false},
				{"explicit transaction", true},
			} {
				t.Run(transaction.name, func(t *testing.T) {
					fixture := newRelationshipEqualityAccessFixture(t, relationshipEqualityFixtureOptions{unrelatedRelationships: 128})
					ctx := context.Background()
					if transaction.explicit {
						ctx = beginRelationshipEqualityTransaction(t, fixture.exec)
					}
					result, err := fixture.exec.Execute(ctx, relationshipEqualityAccessQuery, map[string]interface{}{"id": "r289999"})
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{"r289999", "n73", "n128561"}}, result.Rows)
					if transaction.explicit {
						_, err = fixture.exec.Execute(ctx, "ROLLBACK", nil)
						require.NoError(t, err)
					}
				})
			}
		})
	}
}

// TestRelationshipPropertyEqualityThroughTheDurabilityStack runs the release
// statement through the storage chain production uses — Badger under the WAL
// engine under the namespace — so the index and its lookup are proven reachable
// where the reported database spent its 8 seconds.
func TestRelationshipPropertyEqualityThroughTheDurabilityStack(t *testing.T) {
	for _, parser := range relationshipEqualityParsers {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, transaction := range []struct {
				name     string
				explicit bool
			}{
				{"auto-commit", false},
				{"explicit transaction", true},
			} {
				t.Run(transaction.name, func(t *testing.T) {
					fixture := newRelationshipEqualityAccessFixture(t, relationshipEqualityFixtureOptions{unrelatedRelationships: 128, durabilityStack: true})
					ctx := context.Background()
					if transaction.explicit {
						ctx = beginRelationshipEqualityTransaction(t, fixture.exec)
					}
					result, err := fixture.exec.Execute(ctx, relationshipEqualityAccessQuery, map[string]interface{}{"id": "r289999"})
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{"r289999", "n73", "n128561"}}, result.Rows)
				})
			}
		})
	}
}

// TestRelationshipPropertyEqualityTracksChangedAndDeletedValues checks that the
// index never reports a relationship whose indexed value changed or that was
// deleted: a stale posting may only add candidates the pattern then rejects, and
// a moved or removed value must not return the old row.
func TestRelationshipPropertyEqualityTracksChangedAndDeletedValues(t *testing.T) {
	for _, parser := range relationshipEqualityParsers {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			fixture := newRelationshipEqualityAccessFixture(t, relationshipEqualityFixtureOptions{unrelatedRelationships: 128})
			ctx := context.Background()
			query := func(value string) [][]interface{} {
				result, err := fixture.exec.Execute(ctx, relationshipEqualityAccessQuery, map[string]interface{}{"id": value})
				require.NoError(t, err)
				return result.Rows
			}

			require.Equal(t, [][]interface{}{{"r289999", "n73", "n128561"}}, query("r289999"))

			_, err := fixture.exec.Execute(ctx, "MATCH ()-[r:PD_RELATION {id: 'r289999'}]->() SET r.id = 'moved'", nil)
			require.NoError(t, err)
			require.Empty(t, query("r289999"), "a changed value must not answer its old value")
			require.Equal(t, [][]interface{}{{"moved", "n73", "n128561"}}, query("moved"))

			_, err = fixture.exec.Execute(ctx, "MATCH ()-[r:PD_RELATION {id: 'moved'}]->() DELETE r", nil)
			require.NoError(t, err)
			require.Empty(t, query("moved"), "a deleted relationship must not answer its former value")
		})
	}
}

// TestRelationshipPropertyEqualitySeesTransactionPendingWrites checks the
// transaction's own relationship writes against the same index: created,
// updated and deleted relationships answer as that transaction sees them.
func TestRelationshipPropertyEqualitySeesTransactionPendingWrites(t *testing.T) {
	for _, parser := range relationshipEqualityParsers {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			fixture := newRelationshipEqualityAccessFixture(t, relationshipEqualityFixtureOptions{unrelatedRelationships: 128})
			ctx := beginRelationshipEqualityTransaction(t, fixture.exec)
			query := func(value string) [][]interface{} {
				result, err := fixture.exec.Execute(ctx, relationshipEqualityAccessQuery, map[string]interface{}{"id": value})
				require.NoError(t, err)
				return result.Rows
			}

			_, err := fixture.exec.Execute(ctx, "CREATE (:PDGraphNode {id: 'txa'})-[:PD_RELATION {id: 'tx-only'}]->(:PDGraphNode {id: 'txb'})", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"tx-only", "txa", "txb"}}, query("tx-only"), "a pending relationship must be found")

			_, err = fixture.exec.Execute(ctx, "MATCH ()-[r:PD_RELATION {id: 'r289999'}]->() SET r.id = 'tx-moved'", nil)
			require.NoError(t, err)
			require.Empty(t, query("r289999"), "a pending value change must hide the committed value")
			require.Equal(t, [][]interface{}{{"tx-moved", "n73", "n128561"}}, query("tx-moved"))

			_, err = fixture.exec.Execute(ctx, "MATCH ()-[r:PD_RELATION {id: 'tx-only'}]->() DELETE r", nil)
			require.NoError(t, err)
			require.Empty(t, query("tx-only"), "a pending delete must hide the committed relationship")

		})
	}
}

// TestRelationshipPropertyEqualityKeepsCompleteScanFallbacks checks the shapes
// the index cannot answer: they keep the existing type-index access and the
// same rows, so the new access is never a partial answer.
func TestRelationshipPropertyEqualityKeepsCompleteScanFallbacks(t *testing.T) {
	for _, parser := range relationshipEqualityParsers {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, testCase := range []struct {
				name   string
				query  string
				params map[string]interface{}
				rows   [][]interface{}
			}{
				{
					name:   "unconstrained type",
					query:  "MATCH (a)-[r:OTHER_RELATION {id: $id}]->(b) RETURN r.id, a.id, b.id",
					params: map[string]interface{}{"id": "r289999"},
					rows:   [][]interface{}{},
				},
				{
					name:   "unindexed value",
					query:  relationshipEqualityAccessQuery,
					params: map[string]interface{}{"id": "absent"},
					rows:   [][]interface{}{},
				},
				{
					name:   "null value",
					query:  relationshipEqualityAccessQuery,
					params: map[string]interface{}{"id": nil},
					rows:   [][]interface{}{},
				},
				{
					name:   "unindexed inline property",
					query:  "MATCH (a)-[r:PD_RELATION {id: $id, absent: 'x'}]->(b) RETURN r.id, a.id, b.id",
					params: map[string]interface{}{"id": "r289999"},
					rows:   [][]interface{}{},
				},
			} {
				t.Run(testCase.name, func(t *testing.T) {
					fixture := newRelationshipEqualityAccessFixture(t, relationshipEqualityFixtureOptions{unrelatedRelationships: 128})
					ctx := context.Background()
					result, err := fixture.exec.Execute(ctx, testCase.query, testCase.params)
					require.NoError(t, err)
					require.Equal(t, testCase.rows, result.Rows)
				})
			}
		})
	}
}

func TestRelationshipEqualityLookupPreservesPinnedReadAfterPeerUpdate(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		fixture := newRelationshipEqualityAccessFixture(t, relationshipEqualityFixtureOptions{unrelatedRelationships: 128})
		store := fixture.exec.storage
		query := func(id string) [][]interface{} {
			result, err := fixture.exec.Execute(t.Context(), relationshipEqualityAccessQuery, map[string]interface{}{"id": id})
			require.NoError(t, err)
			return result.Rows
		}
		_, err := fixture.exec.Execute(t.Context(), "BEGIN", nil)
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = fixture.exec.Execute(t.Context(), "ROLLBACK", nil) })
		expected := [][]interface{}{{"r289999", "n73", "n128561"}}
		require.Equal(t, expected, query("r289999"))
		before, err := store.GetEdge(t.Context(), "target")
		require.NoError(t, err)
		after := *before
		after.Properties = map[string]interface{}{"id": "peer-moved"}
		require.NoError(t, store.UpdateEdge(t.Context(), storage.EdgeUpdateRequest{Before: before, After: &after}))
		// A pinned type scan remains the authority for the transaction's cut.
		rows, err := fixture.exec.Execute(t.Context(), relationshipEqualityPinnedAuthorityQuery, map[string]interface{}{"id": "r289999"})
		require.NoError(t, err)
		require.Equal(t, expected, rows.Rows)
		require.Equal(t, expected, query("r289999"), "current postings must not erase a pinned value")
		require.Empty(t, query("peer-moved"), "current postings must not introduce a value newer than the pinned read")
	})
}

// TestRelationshipEqualityLookupPreservesPinnedReadAfterPeerDelete is the
// snapshot control for a value the current postings no longer hold: a peer
// deletes the relationship after the transaction's cut. The type scan with the
// same predicate is the authority for that cut and still reads the deleted
// relationship, so the constrained inline property statement must return the
// same row instead of the empty answer the current index alone would give.
func TestRelationshipEqualityLookupPreservesPinnedReadAfterPeerDelete(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		fixture := newRelationshipEqualityAccessFixture(t, relationshipEqualityFixtureOptions{unrelatedRelationships: 128})
		store := fixture.exec.storage
		ctx := beginRelationshipEqualityTransaction(t, fixture.exec)
		value, authority := relationshipEqualityPinnedQueries(t, fixture.exec, ctx)
		expected := [][]interface{}{{"r289999", "n73", "n128561"}}
		require.Equal(t, expected, value("r289999"), "the transaction reads the target before the peer write")

		peer := context.Background()
		require.NoError(t, store.DeleteEdge(peer, "target"))
		gone, err := store.GetEdge(peer, "target")
		require.Error(t, err, "the peer delete committed")
		require.Nil(t, gone)

		require.Equal(t, expected, authority("r289999"), "the pinned type scan is the authority for the transaction's cut")
		require.Equal(t, expected, value("r289999"), "a deleted current posting must not erase a pinned relationship")

		_, err = fixture.exec.Execute(ctx, "ROLLBACK", nil)
		require.NoError(t, err)
		require.Empty(t, value("r289999"), "outside the transaction the deleted relationship is gone")
	})
}

// TestRelationshipEqualityLookupPreservesPinnedReadAfterPeerDeleteAndRecreate
// keeps the population unchanged across the cut: the peer deletes the pinned
// relationship and recreates the indexed value on another relationship of the
// type, so the current postings answer that value with a different row. The
// pinned transaction must keep its own row and must not report the recreated
// one; a reader outside the transaction answers the recreated row.
func TestRelationshipEqualityLookupPreservesPinnedReadAfterPeerDeleteAndRecreate(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		fixture := newRelationshipEqualityAccessFixture(t, relationshipEqualityFixtureOptions{unrelatedRelationships: 128})
		store := fixture.exec.storage
		ctx := beginRelationshipEqualityTransaction(t, fixture.exec)
		value, authority := relationshipEqualityPinnedQueries(t, fixture.exec, ctx)
		expected := [][]interface{}{{"r289999", "n73", "n128561"}}
		recreated := [][]interface{}{{"r289999", "n128561", "n73"}}
		require.Equal(t, expected, value("r289999"), "the transaction reads the target before the peer write")

		peer := context.Background()
		require.NoError(t, store.DeleteEdge(peer, "target"))
		require.NoError(t, store.CreateEdge(peer, &storage.Edge{
			ID:         "recreated",
			StartNode:  "n128561",
			EndNode:    "n73",
			Type:       "PD_RELATION",
			Properties: map[string]interface{}{"id": "r289999"},
		}))

		require.Equal(t, expected, authority("r289999"), "the pinned type scan keeps the relationship the cut saw")
		require.Equal(t, expected, value("r289999"), "a recreated current posting must not replace a pinned relationship")

		_, err := fixture.exec.Execute(ctx, "ROLLBACK", nil)
		require.NoError(t, err)
		require.Equal(t, recreated, value("r289999"), "outside the transaction the recreated relationship answers the value")
	})
}
