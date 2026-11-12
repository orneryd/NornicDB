package cypher

// Public controls for the on-demand visibility procedures' observation ledger:
// kp.suppress and kp.unsuppress report the actual manual-suppression write they
// issue and the canonical search refresh that publishes it, so their
// registrations declare canonical reads. Every control runs
// StorageExecutor.Execute - the entry point Bolt and HTTP use - on both
// configured parsers, and asserts only what the supported contract states:
//
//   - A suppression reports one ManualSuppressionWriteRequest per call the
//     engine received: an accepted call is one hit and one acknowledged row, a
//     refused call is one attempt and no acknowledged row. The record is the
//     request, never a physical write, a final cardinality or a clause count.
//   - A statement whose every read and every request is accounted for leaves
//     no unobserved boundary, so its db hits are measured. A custom callback
//     that actually ran stays unobserved, so declaring these two procedures
//     canonical never certifies a statement that ran one.
//   - A change an explicit transaction defers is written once, at COMMIT, and
//     never on ROLLBACK, and search shows it only after the commit.
//
// A node and a relationship change go through the one owner,
// applyKpSuppression, which records the request and then refreshes through
// refreshSearch.

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/apoc/registry"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A suppression statement reports the actual suppression write it issued: one
// request per call, one acknowledged row each, under one kind, for a node and a
// relationship alike, and its invocation is no longer an unobserved callback.
func TestProfileSuppressionReportsTheActualSuppressionWrite(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newProfileRuntimeTestExecutor(t,
			"CREATE (:Doc {name: 'a'})-[:CITES {name: 'link'}]->(:Doc {name: 'b'})")
		profile := func(t *testing.T, query string) *ExecutionPlan {
			t.Helper()
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			plan := executionPlanFromResult(t, result)
			requireWriteAccountingObserved(t, plan, query)
			require.NotContains(t, plan.Unobserved, "database reads: procedure callback: kp.suppress", query)
			require.NotContains(t, plan.Unobserved, "database reads: procedure callback: kp.unsuppress", query)
			return plan
		}
		for _, step := range []struct {
			name    string
			query   string
			written int64
		}{
			{"node suppressed", "PROFILE MATCH (n:Doc {name: 'a'}) CALL kp.suppress(n)", 1},
			{"node shown again", "PROFILE MATCH (n:Doc {name: 'a'}) WHERE reveal(n) IS NOT NULL CALL kp.unsuppress(n)", 1},
			{"relationship suppressed", "PROFILE MATCH ()-[r:CITES {name: 'link'}]->() CALL kp.suppress(r)", 1},
			{"relationship shown again", "PROFILE MATCH ()-[r:CITES {name: 'link'}]->() WHERE reveal(r) IS NOT NULL CALL kp.unsuppress(r)", 1},
			{"both nodes suppressed by one statement", "PROFILE MATCH (n:Doc) CALL kp.suppress(n)", 2},
		} {
			t.Run(step.name, func(t *testing.T) {
				plan := profile(t, step.query)
				requests := requireRequestRecord(t, plan, profileKindManualSuppressionWriteRequest, step.name)
				require.Equal(t, step.written, requests.DBHits,
					"one suppression request per call the engine received")
				require.Equal(t, step.written, requests.ActualRows,
					"every accepted request is one acknowledged row")
			})
		}
	})
}

// A refused suppression is one attempt at the actual write and no
// acknowledged row: the public statement reports the engine's own failure, and
// the write it refused left the entity visible.
func TestProfileSuppressionRefusalIsOneAttemptAndNoAcknowledgedRow(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		cause := errors.New("suppression persistence rejected")
		inner := storage.NewNamespacedEngine(newTestMemoryEngine(t), "suppression_refusal")
		_, err := inner.CreateNode(t.Context(), &storage.Node{ID: "refused", Labels: []string{"Refused"}})
		require.NoError(t, err)
		exec := NewStorageExecutor(&rejectingSuppressionEngine{Engine: inner, cause: cause})

		_, err = exec.Execute(t.Context(), "PROFILE MATCH (n:Refused) CALL kp.suppress(n)", nil)
		require.ErrorIs(t, err, cause, "PROFILE reports the suppression write's actual failure")

		collector := newProfileRuntimeCollector()
		_, err = exec.Execute(withProfileRuntimeCollector(t.Context(), collector),
			"MATCH (n:Refused) CALL kp.suppress(n)", nil)
		require.ErrorIs(t, err, cause)
		attempts := profileAccessesOfKind(collector, profileKindManualSuppressionWriteRequest)
		require.Len(t, attempts, 1, "the refused write is the statement's one suppression request")
		require.Equal(t, int64(1), attempts[0].Hits, "a refused request is still one attempt")
		require.Zero(t, attempts[0].Rows, "and acknowledged no row")

		visible, err := NewStorageExecutor(inner).Execute(t.Context(), "MATCH (n:Refused) RETURN count(n)", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, visible.Rows, "the refused suppression hid nothing")
	})
}

// Declaring the suppression procedures canonical does not certify a statement
// that actually ran a custom callback: its invocation stays unobserved, while
// the suppression request and the change it reports remain the statement's own.
func TestProfileSuppressionDoesNotCertifyAnActualCustomCallback(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		graph := newLargeEmbeddingGraph(t, 1)
		handler, err := registry.Compile(func(ctx context.Context) (string, error) {
			node, err := graph.store.GetNode(ctx, "doc-0")
			if err != nil {
				return "", err
			}
			return node.Properties["title"].(string), nil
		}, registry.OpaqueReads)
		require.NoError(t, err)
		previous := PluginFunctionLookup
		t.Cleanup(func() { PluginFunctionLookup = previous })
		PluginFunctionLookup = func(name string) (*PluginFunctionHandler, bool) {
			if name == "profile.opaque" {
				return handler, true
			}
			if previous != nil {
				return previous(name)
			}
			return nil, false
		}

		result := graph.run(t,
			"PROFILE MATCH (n:Doc {k:0}) WHERE profile.opaque() IS NOT NULL CALL kp.suppress(n)", nil)
		plan := executionPlanFromResult(t, result)
		require.False(t, plan.TotalDBHitsObserved,
			"a custom callback that actually ran leaves the statement unobserved")
		require.NotEmpty(t, plan.Unobserved)
		requests := requireRequestRecord(t, plan, profileKindManualSuppressionWriteRequest,
			"the suppression issued alongside the custom callback")
		require.Equal(t, int64(1), requests.DBHits)
		require.Equal(t, int64(1), requests.ActualRows)
		require.Equal(t, [][]interface{}{{int64(0)}}, graph.run(t, "MATCH (n:Doc {k:0}) RETURN count(n)", nil).Rows,
			"the suppression the statement reported is the one it applied")
	})
}

// suppressionWriteCounter counts the actual suppression writes an engine
// received and applies each through the namespaced owner it wraps.
type suppressionWriteCounter struct {
	*storage.NamespacedEngine
	writes int
}

func (e *suppressionWriteCounter) GetInnerEngine() storage.Engine { return e.NamespacedEngine }

func (e *suppressionWriteCounter) SetManualSuppression(ctx context.Context, kind storage.StoredEntityKind, id string, suppressed bool) error {
	e.writes++
	return e.NamespacedEngine.SetManualSuppression(ctx, kind, id, suppressed)
}

// A change an explicit transaction defers is one actual write at COMMIT and no
// write at all on ROLLBACK, for a relationship and a node alike, and search
// shows a node's change only once the commit publishes it.
func TestSuppressionChangeIsOneWriteAtTheCommittingTransaction(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		ctx := context.Background()
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "suppression_deferred")
		counter := &suppressionWriteCounter{NamespacedEngine: store}
		exec := NewStorageExecutor(counter)
		run := func(query string) {
			t.Helper()
			_, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
		}
		count := func(query string) int64 {
			t.Helper()
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Len(t, result.Rows, 1, query)
			return result.Rows[0][0].(int64)
		}
		run("CREATE (:Diary {name: 'quokkalicious'})-[:MENTIONS {name: 'quokka-link'}]->(:Diary {name: 'quiet'})")
		svc := search.NewService(store, search.DefaultVectorDimensions, nil)
		t.Cleanup(func() { _ = svc.Close() })
		require.NoError(t, svc.BuildIndexes(ctx))
		exec.SetSearchService(svc)
		hits := func(term string) int {
			t.Helper()
			response, err := svc.Search(ctx, term, nil, search.DefaultSearchOptions())
			require.NoError(t, err)
			return len(response.Results)
		}
		const edgeCount = "MATCH ()-[r:MENTIONS]->() RETURN count(r)"
		require.Equal(t, 1, hits("quokkalicious"))
		require.Equal(t, int64(1), count(edgeCount))

		// A relationship: ROLLBACK writes nothing, COMMIT is the one actual
		// write.
		run("BEGIN")
		run("MATCH ()-[r:MENTIONS {name: 'quokka-link'}]->() CALL kp.suppress(r)")
		require.Zero(t, counter.writes, "a suppression an explicit transaction queued is not written before COMMIT")
		run("ROLLBACK")
		require.Zero(t, counter.writes, "a rolled-back suppression is never written")
		require.Equal(t, int64(1), count(edgeCount), "nor is it applied")

		run("BEGIN")
		run("MATCH ()-[r:MENTIONS {name: 'quokka-link'}]->() CALL kp.suppress(r)")
		run("COMMIT")
		require.Equal(t, 1, counter.writes, "the committed change is one actual suppression write")
		require.Equal(t, int64(0), count(edgeCount))
		require.Equal(t, int64(1), count("MATCH ()-[r:MENTIONS]->() WHERE reveal(r) IS NOT NULL RETURN count(r)"))

		run("BEGIN")
		run("MATCH ()-[r:MENTIONS {name: 'quokka-link'}]->() WHERE reveal(r) IS NOT NULL CALL kp.unsuppress(r)")
		require.Equal(t, 1, counter.writes)
		run("ROLLBACK")
		require.Equal(t, 1, counter.writes, "an unsuppression rolled back is never written")
		require.Equal(t, int64(0), count(edgeCount))

		run("BEGIN")
		run("MATCH ()-[r:MENTIONS {name: 'quokka-link'}]->() WHERE reveal(r) IS NOT NULL CALL kp.unsuppress(r)")
		run("COMMIT")
		require.Equal(t, 2, counter.writes)
		require.Equal(t, int64(1), count(edgeCount), "the committed unsuppression shows the relationship again")

		// A node: the same deferred write, and search sees it only once the
		// commit publishes it.
		run("BEGIN")
		run("MATCH (n:Diary {name: 'quokkalicious'}) CALL kp.suppress(n)")
		require.Equal(t, 2, counter.writes)
		require.Equal(t, 1, hits("quokkalicious"), "a queued suppression is not published to search")
		run("ROLLBACK")
		require.Equal(t, 2, counter.writes)
		require.Equal(t, 1, hits("quokkalicious"))

		run("BEGIN")
		run("MATCH (n:Diary {name: 'quokkalicious'}) CALL kp.suppress(n)")
		run("COMMIT")
		require.Equal(t, 3, counter.writes)
		require.Equal(t, int64(0), count("MATCH (n:Diary {name: 'quokkalicious'}) RETURN count(n)"))
		require.Zero(t, hits("quokkalicious"), "the committing transaction publishes the hidden node to search")
	})
}

// The two suppression procedures declare canonical reads: each reports the
// entity its caller resolved, the write request it issued and the refresh that
// publishes it.
func TestVisibilityProceduresDeclareCanonicalReads(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	for _, name := range []string{"kp.suppress", "kp.unsuppress"} {
		procedure, found := globalProcedureRegistry.Get(name)
		require.True(t, found, name)
		require.Equal(t, cypherfn.CanonicalReads, procedure.reads, name)
		require.Equal(t, ProcedureModeWrite, procedure.Spec.Mode, name)
	}
}
