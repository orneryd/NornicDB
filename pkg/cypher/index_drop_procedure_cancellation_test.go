package cypher

// The public index-drop procedures and the caller's own cancellation.
//
// CALL db.index.fulltext.drop and CALL db.index.vector.drop are registered
// write procedures, so the statement contract opens an implicit transaction for
// them: the statement runs on an executor whose storage is its transaction
// wrapper, the drop edits that statement's isolated schema view
// (transactionStorageWrapper.GetSchema -> BadgerTransaction.Schema), and the
// statement publishes it through the typed commit owner the engine chain
// exposes (StorageExecutor.commitStorageTransaction -> CommitTransaction ->
// BadgerTransaction.CommitContext). Until that terminal publication is admitted
// the statement is request-owned; afterwards the commit owns its own outcome.
//
// The routes that carry the drop are qualified here with the caller's own
// CancelFunc released through public statements only:
//
//   - the implicit native statement (the earlier qualification): cancellation
//     released at the statement's typed commit owner;
//   - an explicit native transaction (BEGIN / COMMIT / ROLLBACK on one
//     executor): the drop is staged in the transaction's own schema view and
//     cancelled at the transaction's typed commit owner during COMMIT, and by a
//     caller that is already canceled when the drop statement arrives;
//   - the composite database's supported constituent route
//     (USE <composite>.<alias> ...), which Fabric plans and runs as a
//     participant transaction: cancellation before the statement, a rejection
//     that follows the drop inside the same statement, and a cancellation of
//     the explicit composite transaction's COMMIT or of a statement inside it.
//     A dynamic graph.byName(..) constituent selection is the same route.
//
// What they establish, as product outcomes:
//
//   - a canceled drop reports its cancellation and publishes nothing: the
//     namespace keeps both of its indexes, and the other namespace keeps its
//     same-named ones;
//   - a canceled drop returns within the approved budget after the cancellation
//     is released, instead of finishing the drop first;
//   - the wire error keeps its existing shape ("commit failed", errors.Is
//     context.Canceled) where that owner produces it, so downstream classifiers
//     see what they saw before;
//   - a healthy retry drops only its own kind, in its own namespace;
//   - a drop whose commit the terminal owner already admitted keeps that
//     outcome, because publication no longer belongs to the request;
//   - an already-canceled caller opens no transaction at all;
//   - an explicit transaction publishes a staged drop only at COMMIT: ROLLBACK
//     keeps it, a canceled COMMIT keeps it, and a canceled statement inside the
//     transaction makes the transaction fail so its staged drop is never
//     published;
//   - on the composite constituent route the drop reaches only its own
//     constituent and kind, and a statement that is rejected after the drop ran
//     (a later failing call, or the one-write-database rule across
//     constituents) publishes nothing on any constituent.
//
// Established limits of the current source (not corrected here; no route turns
// them into a mutation):
//
//   - the private drop helper now retains its caller in mutateSchema and
//     rejects cancellation before schema preparation. Terminal cancellation
//     races below still cannot turn a rejected drop into a live mutation
//     (the drop edits the statement's own isolated schema view, whose
//     schema-mutation lock no other statement holds, and both publication gates
//     reject a canceled publication). The explicit routes cannot either: the
//     staged view is published only by their own terminal owner.
//   - the composite route publishes its participants through the Fabric
//     transaction's own lifetime: participant begin and commit contexts are
//     bound with context.WithoutCancel (fabric_participants.go statementFor,
//     executor_fabric.go ensureLocalShardTxExecutor), so a cancellation that
//     races the participant commit is not released there by any public seam.
//     What is established instead is that every cancellation the route does
//     observe (before the statement, inside the transaction, at COMMIT) is
//     rejected and publishes nothing.
//   - transaction-local SHOW INDEXES visibility is qualified separately by
//     TestIndexDropProcedureFabricExplicitTransactionSeesOwnStagedDrop and
//     TestIndexDropProcedureFabricCommentPrefixStagedVisibility. Their routing
//     correction preserves the cancellation/publication controls in this file.
//   - remote constituents were not part of this cancellation qualification,
//     and the composite root's own rejected target is pinned separately by
//     TestIndexDropProcedureCompositeRootRejection.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txphase"
	"github.com/stretchr/testify/require"
)

// indexDropCancellationBudget is the approved bound on how long the public
// statement may take to return once the caller's cancellation was released.
const indexDropCancellationBudget = 100 * time.Millisecond

const (
	indexDropNamespace      = "dropcancel"
	indexDropOtherNamespace = "dropcancel_other"
	indexDropFulltextName   = "ft_docs"
	indexDropVectorName     = "vec_docs"
	indexDropNamesQuery     = "SHOW INDEXES YIELD name WHERE name IN ['" + indexDropFulltextName + "', '" + indexDropVectorName + "'] RETURN name ORDER BY name"
)

// indexDropCommitBoundary is the typed commit owner between the executor and
// the real engine: it observes the statement's transaction opening and its
// terminal native publication, and releases the case's real CancelFunc exactly
// at that publication. Nothing else about the transaction changes: Begin and
// Commit are the engine's own.
type indexDropCommitBoundary struct {
	*storage.NamespacedEngine
	base     *storage.MemoryEngine
	begins   int
	commits  int
	cancel   context.CancelFunc
	released time.Time
}

func (b *indexDropCommitBoundary) BeginTransaction(ctx context.Context) (*storage.BadgerTransaction, error) {
	b.begins++
	return b.base.BeginTransaction(ctx)
}

func (b *indexDropCommitBoundary) CommitTransaction(ctx context.Context, tx *storage.BadgerTransaction) error {
	b.commits++
	if b.cancel != nil {
		b.cancel()
		b.released = time.Now()
	}
	return tx.CommitContext(ctx)
}

// indexDropFixture is one namespace pair over one engine: the namespace the
// statement under test runs in, and a second namespace holding the same index
// names.
type indexDropFixture struct {
	exec  *StorageExecutor
	owner *indexDropCommitBoundary
	other *StorageExecutor
	base  *storage.MemoryEngine
}

func newIndexDropFixture(t *testing.T) *indexDropFixture {
	t.Helper()
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, indexDropNamespace)
	owner := &indexDropCommitBoundary{NamespacedEngine: store, base: base}
	return &indexDropFixture{
		exec:  NewStorageExecutor(owner),
		owner: owner,
		other: NewStorageExecutor(storage.NewNamespacedEngine(base, indexDropOtherNamespace)),
		base:  base,
	}
}

// indexDropOutside is an executor outside the fixture's statement and outside
// any transaction: it reads the namespace's published state only.
func (f *indexDropFixture) indexDropOutside() *StorageExecutor {
	return NewStorageExecutor(storage.NewNamespacedEngine(f.base, indexDropNamespace))
}

// indexDropCreateBothKinds creates the fixture's fulltext and vector index in
// every namespace, so a drop can be told apart from a drop of another kind or of
// another namespace, and any change the canceled statement made leaves a trace.
// Callers create before arming the boundary, whose counters then describe only
// the statement under test.
func (f *indexDropFixture) indexDropCreateBothKinds(t *testing.T, ctx context.Context) {
	t.Helper()
	for _, exec := range []*StorageExecutor{f.exec, f.other} {
		_, err := exec.Execute(ctx, "CREATE FULLTEXT INDEX "+indexDropFulltextName+" FOR (n:Doc) ON EACH [n.body]", nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, "CREATE VECTOR INDEX "+indexDropVectorName+
			" FOR (n:Doc) ON (n.body) OPTIONS { indexConfig: { `vector.dimensions`: 3, `vector.similarity_function`: 'cosine' } }", nil)
		require.NoError(t, err)
	}
	f.owner.begins, f.owner.commits = 0, 0
}

func indexDropBothKinds() []string {
	return []string{indexDropFulltextName, indexDropVectorName}
}

// TestIndexDropProcedureCancellationPublishesNothing is the request-owned side:
// the caller's cancellation arrives at the terminal commit owner, so the drop
// must report it, publish no schema change, and return inside the budget. The
// healthy retry then drops only its own kind, and the other namespace keeps its
// indexes throughout.
func TestIndexDropProcedureCancellationPublishesNothing(t *testing.T) {
	cases := []struct {
		name, query, otherQuery, dropped string
		remaining                        []string
	}{
		{"fulltext", "CALL db.index.fulltext.drop('" + indexDropFulltextName + "')", "CALL db.index.vector.drop('" + indexDropVectorName + "')", indexDropFulltextName, []string{indexDropVectorName}},
		{"vector", "CALL db.index.vector.drop('" + indexDropVectorName + "')", "CALL db.index.fulltext.drop('" + indexDropFulltextName + "')", indexDropVectorName, []string{indexDropFulltextName}},
	}
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, procedure := range cases {
				t.Run(procedure.name, func(t *testing.T) {
					fixture := newIndexDropFixture(t)
					healthy, healthyCancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer healthyCancel()
					fixture.indexDropCreateBothKinds(t, healthy)

					request, cancel := context.WithCancel(context.Background())
					defer cancel()
					fixture.owner.cancel = cancel
					result, err := fixture.exec.Execute(request, procedure.query, nil)

					require.Error(t, err, "a canceled drop must report its cancellation")
					require.ErrorIs(t, err, context.Canceled)
					require.ErrorIs(t, request.Err(), context.Canceled)
					require.Contains(t, err.Error(), "commit failed",
						"the canceled implicit commit keeps its consumer-pinned wire shape")
					require.Nil(t, result, "a canceled drop must not publish a result")
					require.Equal(t, 1, fixture.owner.commits, "the drop reaches exactly one terminal publication owner")
					require.False(t, fixture.owner.released.IsZero(), "the fixture must release the cancellation at the commit owner")
					returned := time.Since(fixture.owner.released)
					require.LessOrEqual(t, returned, indexDropCancellationBudget,
						"the canceled drop must return within the approved budget")
					t.Logf("canceled %s drop: return=%s begins=%d commits=%d err=%v",
						procedure.name, returned, fixture.owner.begins, fixture.owner.commits, err)

					// No index mutation: the canceled statement published nothing,
					// in its own namespace or in the other one.
					require.Equal(t, indexDropBothKinds(), readIndexNames(t, healthy, fixture.exec, indexDropNamesQuery))
					require.Equal(t, indexDropBothKinds(), readIndexNames(t, healthy, fixture.other, indexDropNamesQuery))

					// Healthy retry drops the kind it names, and only that kind.
					fixture.owner.cancel = nil
					retry, err := fixture.exec.Execute(healthy, procedure.query, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{procedure.dropped, true}}, retry.Rows)
					require.Equal(t, procedure.remaining, readIndexNames(t, healthy, fixture.exec, indexDropNamesQuery))

					// The other kind still drops through its own procedure, and
					// the other namespace keeps both of its indexes.
					other, err := fixture.exec.Execute(healthy, procedure.otherQuery, nil)
					require.NoError(t, err)
					require.Len(t, other.Rows, 1)
					require.Empty(t, readIndexNames(t, healthy, fixture.exec, indexDropNamesQuery))
					require.Equal(t, indexDropBothKinds(), readIndexNames(t, healthy, fixture.other, indexDropNamesQuery))
				})
			}
		})
	}
}

// TestIndexDropProcedureAcceptedCommitPublishes is the accepted side of the same
// boundary: once the terminal owner admitted the commit (the transaction phase
// HTTP statements run under), the publication belongs to the transaction, so a
// cancellation released at that point does not revoke it.
func TestIndexDropProcedureAcceptedCommitPublishes(t *testing.T) {
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			fixture := newIndexDropFixture(t)
			healthy, healthyCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer healthyCancel()
			fixture.indexDropCreateBothKinds(t, healthy)

			request, cancel := context.WithCancel(context.Background())
			defer cancel()
			request, executionCancel := txphase.WithExecutionDeadline(request, 30*time.Second)
			defer executionCancel()
			fixture.owner.cancel = cancel
			result, err := fixture.exec.Execute(request, "CALL db.index.fulltext.drop('"+indexDropFulltextName+"')", nil)

			require.NoError(t, err, "an admitted commit owns its outcome")
			require.ErrorIs(t, request.Err(), context.Canceled)
			require.False(t, fixture.owner.released.IsZero(), "the fixture must release the cancellation at the commit owner")
			require.Equal(t, [][]interface{}{{indexDropFulltextName, true}}, result.Rows)
			require.Equal(t, []string{indexDropVectorName}, readIndexNames(t, healthy, fixture.exec, indexDropNamesQuery))
			require.Equal(t, indexDropBothKinds(), readIndexNames(t, healthy, fixture.other, indexDropNamesQuery))
		})
	}
}

// TestIndexDropProcedurePreCanceledStopsBeforeAnyOwner is the statement gate: a
// caller that is already canceled never opens the drop's transaction at all.
func TestIndexDropProcedurePreCanceledStopsBeforeAnyOwner(t *testing.T) {
	for _, procedure := range []struct {
		name, query string
	}{
		{"fulltext", "CALL db.index.fulltext.drop('" + indexDropFulltextName + "')"},
		{"vector", "CALL db.index.vector.drop('" + indexDropVectorName + "')"},
	} {
		t.Run(procedure.name, func(t *testing.T) {
			fixture := newIndexDropFixture(t)
			healthy, healthyCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer healthyCancel()
			fixture.indexDropCreateBothKinds(t, healthy)

			request, cancel := context.WithCancel(context.Background())
			cancel()
			result, err := fixture.exec.Execute(request, procedure.query, nil)
			require.Error(t, err)
			require.True(t, errors.Is(err, context.Canceled), "got %v", err)
			require.Nil(t, result)
			require.Zero(t, fixture.owner.begins, "a pre-canceled drop opens no transaction")
			require.Equal(t, indexDropBothKinds(), readIndexNames(t, healthy, fixture.exec, indexDropNamesQuery))
		})
	}
}

// TestIndexDropProcedureCancellationControlKeepsNonCancellableContract pins the
// unchanged control the canceled cases must not disturb: without a cancellation
// the same statements drop exactly their own kind and report their rows.
func TestIndexDropProcedureCancellationControlKeepsNonCancellableContract(t *testing.T) {
	fixture := newIndexDropFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fixture.indexDropCreateBothKinds(t, ctx)

	result, err := fixture.exec.Execute(ctx, "CALL db.index.fulltext.drop('"+indexDropFulltextName+"')", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{indexDropFulltextName, true}}, result.Rows)
	require.Equal(t, []string{indexDropVectorName}, readIndexNames(t, ctx, fixture.exec, indexDropNamesQuery))

	result, err = fixture.exec.Execute(ctx, "CALL db.index.vector.drop('"+indexDropVectorName+"')", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{indexDropVectorName, true}}, result.Rows)
	require.Empty(t, readIndexNames(t, ctx, fixture.exec, indexDropNamesQuery))

	_, err = fixture.exec.Execute(ctx, "CALL db.index.vector.drop('"+indexDropVectorName+"')", nil)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "IndexDropFailed"), "got %v", err)
	require.Equal(t, indexDropBothKinds(), readIndexNames(t, ctx, fixture.other, indexDropNamesQuery))
}

// --- Explicit native transactions ---
//
// An explicit transaction is the same schema owner with a longer lifetime: the
// drop stages its change in the transaction's own schema view and the only
// publication is the transaction's COMMIT, which carries the COMMIT statement's
// context (handleCommit -> commitStorageTransaction -> the typed owner). These
// cases drive BEGIN / the drop / COMMIT / ROLLBACK through Execute, so the
// cancellation is the caller's real CancelFunc and arrives while the
// transaction still owns the staged drop.

// TestIndexDropProcedureExplicitTransactionKeepsDropUntilCommit establishes the
// isolation the cancellation cases rely on: inside the transaction the drop is
// visible to its own statements, while nothing is published - the independent
// view of the same namespace still lists both indexes - until COMMIT.
func TestIndexDropProcedureExplicitTransactionKeepsDropUntilCommit(t *testing.T) {
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, procedure := range []struct {
				name, query string
				remaining   []string
			}{
				{"fulltext", "CALL db.index.fulltext.drop('" + indexDropFulltextName + "')", []string{indexDropVectorName}},
				{"vector", "CALL db.index.vector.drop('" + indexDropVectorName + "')", []string{indexDropFulltextName}},
			} {
				t.Run(procedure.name, func(t *testing.T) {
					fixture := newIndexDropFixture(t)
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					fixture.indexDropCreateBothKinds(t, ctx)
					published := fixture.indexDropOutside()

					begun, err := fixture.exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{"Transaction started"}}, begun.Rows)

					dropped, err := fixture.exec.Execute(ctx, procedure.query, nil)
					require.NoError(t, err)
					require.Len(t, dropped.Rows, 1)

					// The transaction sees the drop; no reader outside it does,
					// and nothing reached the schema before COMMIT.
					require.Equal(t, procedure.remaining, readIndexNames(t, ctx, fixture.exec, indexDropNamesQuery))
					require.Equal(t, indexDropBothKinds(), readIndexNames(t, ctx, published, indexDropNamesQuery))
					require.Equal(t, indexDropBothKinds(), readIndexNames(t, ctx, fixture.other, indexDropNamesQuery))

					committed, err := fixture.exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{"Transaction committed"}}, committed.Rows)
					require.Equal(t, procedure.remaining, readIndexNames(t, ctx, published, indexDropNamesQuery))
					require.Equal(t, indexDropBothKinds(), readIndexNames(t, ctx, fixture.other, indexDropNamesQuery))
				})
			}
		})
	}
}

// TestIndexDropProcedureExplicitTransactionRollbackPublishesNothing: a
// transaction that staged the drop and then rolled back publishes nothing, and
// the namespace stays droppable afterwards.
func TestIndexDropProcedureExplicitTransactionRollbackPublishesNothing(t *testing.T) {
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, procedure := range []struct {
				name, query string
			}{
				{"fulltext", "CALL db.index.fulltext.drop('" + indexDropFulltextName + "')"},
				{"vector", "CALL db.index.vector.drop('" + indexDropVectorName + "')"},
			} {
				t.Run(procedure.name, func(t *testing.T) {
					fixture := newIndexDropFixture(t)
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					fixture.indexDropCreateBothKinds(t, ctx)
					published := fixture.indexDropOutside()

					_, err := fixture.exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					_, err = fixture.exec.Execute(ctx, procedure.query, nil)
					require.NoError(t, err)
					rolledBack, err := fixture.exec.Execute(ctx, "ROLLBACK", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{"Transaction rolled back"}}, rolledBack.Rows)

					require.Equal(t, indexDropBothKinds(), readIndexNames(t, ctx, published, indexDropNamesQuery))
					require.Equal(t, indexDropBothKinds(), readIndexNames(t, ctx, fixture.other, indexDropNamesQuery))

					// The route still works after the rollback.
					retry, err := fixture.exec.Execute(ctx, procedure.query, nil)
					require.NoError(t, err)
					require.Len(t, retry.Rows, 1)
					require.Len(t, readIndexNames(t, ctx, published, indexDropNamesQuery), 1)
					require.Equal(t, indexDropBothKinds(), readIndexNames(t, ctx, fixture.other, indexDropNamesQuery))
				})
			}
		})
	}
}

// TestIndexDropProcedureExplicitTransactionCancelledCommitPublishesNothing is
// the explicit transaction's request-owned side: the caller's cancellation is
// released at the transaction's typed commit owner during COMMIT, so the COMMIT
// must report it, publish no schema change, end the transaction, and return
// inside the budget.
func TestIndexDropProcedureExplicitTransactionCancelledCommitPublishesNothing(t *testing.T) {
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, procedure := range []struct {
				name, query, dropped string
			}{
				{"fulltext", "CALL db.index.fulltext.drop('" + indexDropFulltextName + "')", indexDropFulltextName},
				{"vector", "CALL db.index.vector.drop('" + indexDropVectorName + "')", indexDropVectorName},
			} {
				t.Run(procedure.name, func(t *testing.T) {
					fixture := newIndexDropFixture(t)
					healthy, healthyCancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer healthyCancel()
					fixture.indexDropCreateBothKinds(t, healthy)
					published := fixture.indexDropOutside()

					_, err := fixture.exec.Execute(healthy, "BEGIN", nil)
					require.NoError(t, err)
					staged, err := fixture.exec.Execute(healthy, procedure.query, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{procedure.dropped, true}}, staged.Rows)
					require.Equal(t, indexDropBothKinds(), readIndexNames(t, healthy, published, indexDropNamesQuery))

					request, cancel := context.WithCancel(context.Background())
					defer cancel()
					fixture.owner.cancel = cancel
					result, err := fixture.exec.Execute(request, "COMMIT", nil)

					require.Error(t, err, "a canceled COMMIT must report its cancellation")
					require.ErrorIs(t, err, context.Canceled)
					require.ErrorIs(t, request.Err(), context.Canceled)
					require.Contains(t, err.Error(), "commit failed")
					require.Nil(t, result, "a canceled COMMIT must not report a commit")
					require.Equal(t, 1, fixture.owner.commits, "the COMMIT reaches exactly one terminal publication owner")
					require.False(t, fixture.owner.released.IsZero(), "the fixture must release the cancellation at the commit owner")
					returned := time.Since(fixture.owner.released)
					require.LessOrEqual(t, returned, indexDropCancellationBudget,
						"the canceled COMMIT must return within the approved budget")
					t.Logf("canceled %s COMMIT: return=%s begins=%d commits=%d err=%v",
						procedure.name, returned, fixture.owner.begins, fixture.owner.commits, err)

					// Nothing was published, and the transaction is over: no
					// later COMMIT can publish the staged drop.
					fixture.owner.cancel = nil
					require.Equal(t, indexDropBothKinds(), readIndexNames(t, healthy, published, indexDropNamesQuery))
					require.Equal(t, indexDropBothKinds(), readIndexNames(t, healthy, fixture.other, indexDropNamesQuery))
					_, err = fixture.exec.Execute(healthy, "COMMIT", nil)
					require.Error(t, err, "a canceled COMMIT ends the transaction")
					require.Equal(t, indexDropBothKinds(), readIndexNames(t, healthy, published, indexDropNamesQuery))

					// A healthy transaction on the same route still publishes
					// exactly the drop it names.
					_, err = fixture.exec.Execute(healthy, "BEGIN", nil)
					require.NoError(t, err)
					_, err = fixture.exec.Execute(healthy, procedure.query, nil)
					require.NoError(t, err)
					_, err = fixture.exec.Execute(healthy, "COMMIT", nil)
					require.NoError(t, err)
					require.Len(t, readIndexNames(t, healthy, published, indexDropNamesQuery), 1)
					require.Equal(t, indexDropBothKinds(), readIndexNames(t, healthy, fixture.other, indexDropNamesQuery))
				})
			}
		})
	}
}

// TestIndexDropProcedureExplicitTransactionPreCanceledStatementPublishesNothing:
// a caller already canceled when the drop statement is issued inside an open
// transaction gets a rejection, the staged work is not published by the
// following ROLLBACK, and the transaction stays usable for a healthy retry.
func TestIndexDropProcedureExplicitTransactionPreCanceledStatementPublishesNothing(t *testing.T) {
	fixture := newIndexDropFixture(t)
	healthy, healthyCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer healthyCancel()
	fixture.indexDropCreateBothKinds(t, healthy)
	published := fixture.indexDropOutside()

	_, err := fixture.exec.Execute(healthy, "BEGIN", nil)
	require.NoError(t, err)

	request, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := fixture.exec.Execute(request, "CALL db.index.fulltext.drop('"+indexDropFulltextName+"')", nil)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result)
	require.Zero(t, fixture.owner.commits, "a rejected statement inside the transaction publishes nothing itself")
	require.Equal(t, indexDropBothKinds(), readIndexNames(t, healthy, published, indexDropNamesQuery))

	rolledBack, err := fixture.exec.Execute(healthy, "ROLLBACK", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"Transaction rolled back"}}, rolledBack.Rows)
	require.Equal(t, indexDropBothKinds(), readIndexNames(t, healthy, published, indexDropNamesQuery))
	require.Equal(t, indexDropBothKinds(), readIndexNames(t, healthy, fixture.other, indexDropNamesQuery))
}

// --- Supported Fabric constituent routes ---
//
// A composite database is the supported Fabric surface: USE <composite>.<alias>
// selects a constituent, and the statement runs there as a Fabric participant
// transaction whose only publication is the participant's own commit. The
// constituent's live schema manager is the oracle: it shows what was actually
// published, independently of the statement that published it.

const (
	indexDropFabricComposite = "dropfab"
	indexDropFabricShardA    = "dropfab_a"
	indexDropFabricShardB    = "dropfab_b"
	indexDropFabricAliasA    = "a"
	indexDropFabricAliasB    = "b"

	indexDropFabricGraphA = indexDropFabricComposite + "." + indexDropFabricAliasA
	indexDropFabricGraphB = indexDropFabricComposite + "." + indexDropFabricAliasB

	indexDropFabricCreateFT    = "CREATE FULLTEXT INDEX " + indexDropFulltextName + " FOR (n:Doc) ON EACH [n.body]"
	indexDropFabricCreateVec   = "CREATE VECTOR INDEX " + indexDropVectorName + " FOR (n:Doc) ON (n.body) OPTIONS { indexConfig: { `vector.dimensions`: 3, `vector.similarity_function`: 'cosine' } }"
	indexDropFabricDropFT      = "CALL db.index.fulltext.drop('" + indexDropFulltextName + "')"
	indexDropFabricDropVector  = "CALL db.index.vector.drop('" + indexDropVectorName + "')"
	indexDropFabricDropMissing = "CALL db.index.fulltext.drop('missing_index')"
)

// indexDropFabricFixture is a composite database over two local constituents,
// plus the composite session an explicit transaction runs on.
type indexDropFabricFixture struct {
	mgr  *multidb.DatabaseManager
	root *StorageExecutor
	ctx  context.Context
}

func newIndexDropFabricFixture(t *testing.T) *indexDropFabricFixture {
	t.Helper()
	mgr, adapter := createCompositeTestManager(t, indexDropFabricComposite, []multidb.ConstituentRef{
		{Alias: indexDropFabricAliasA, DatabaseName: indexDropFabricShardA, Type: "local", AccessMode: "read_write"},
		{Alias: indexDropFabricAliasB, DatabaseName: indexDropFabricShardB, Type: "local", AccessMode: "read_write"},
	})
	root := newCompositeTestExecutor(t, mgr, indexDropFabricComposite, adapter)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return &indexDropFabricFixture{mgr: mgr, root: root, ctx: ctx}
}

func (f *indexDropFabricFixture) createBothKinds(t *testing.T, exec *StorageExecutor, graph string) {
	t.Helper()
	for _, statement := range []string{indexDropFabricCreateFT, indexDropFabricCreateVec} {
		_, err := exec.Execute(f.ctx, "USE "+graph+" "+statement, nil)
		require.NoError(t, err)
	}
}

// liveNames is the constituent's published index definitions, read from its own
// schema manager rather than through the statement under test.
func (f *indexDropFabricFixture) liveNames(t *testing.T, database string) []string {
	t.Helper()
	store, err := f.mgr.GetStorage(database)
	require.NoError(t, err)
	schema := store.GetSchema()
	names := []string{}
	if _, ok := schema.GetFulltextIndex(indexDropFulltextName); ok {
		names = append(names, indexDropFulltextName)
	}
	if _, ok := schema.GetVectorIndex(indexDropVectorName); ok {
		names = append(names, indexDropVectorName)
	}
	return names
}

// TestIndexDropProcedureFabricConstituentRouteDropsOwnKind is the healthy
// control for the constituent route: the drop publishes in the constituent it
// names, of the kind it names, and the other constituent keeps its same-named
// indexes.
func TestIndexDropProcedureFabricConstituentRouteDropsOwnKind(t *testing.T) {
	fixture := newIndexDropFabricFixture(t)
	fixture.createBothKinds(t, fixture.root, indexDropFabricGraphA)
	fixture.createBothKinds(t, fixture.root, indexDropFabricGraphB)
	require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardA))
	require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardB))

	for _, step := range []struct {
		graph, statement, dropped string
		onA, onB                  []string
	}{
		{indexDropFabricGraphA, indexDropFabricDropFT, indexDropFulltextName, []string{indexDropVectorName}, indexDropBothKinds()},
		{indexDropFabricGraphA, indexDropFabricDropVector, indexDropVectorName, []string{}, indexDropBothKinds()},
		{indexDropFabricGraphB, indexDropFabricDropFT, indexDropFulltextName, []string{}, []string{indexDropVectorName}},
		{indexDropFabricGraphB, indexDropFabricDropVector, indexDropVectorName, []string{}, []string{}},
	} {
		result, err := fixture.root.Execute(fixture.ctx, "USE "+step.graph+" "+step.statement, nil)
		require.NoError(t, err, step.statement)
		require.Equal(t, [][]interface{}{{step.dropped, true}}, result.Rows)
		require.Equal(t, step.onA, fixture.liveNames(t, indexDropFabricShardA), "constituent a after %s", step.statement)
		require.Equal(t, step.onB, fixture.liveNames(t, indexDropFabricShardB), "constituent b after %s", step.statement)
	}

	// The route's own introspection agrees with the constituent's schema.
	shown, err := fixture.root.Execute(fixture.ctx,
		"USE "+indexDropFabricGraphA+" SHOW INDEXES YIELD name WHERE name IN ['"+indexDropFulltextName+"', '"+indexDropVectorName+"'] RETURN name", nil)
	require.NoError(t, err)
	require.Empty(t, shown.Rows)
}

// TestIndexDropProcedureFabricDynamicConstituentRouteDropsOwnKind: graph.byName
// selects the same supported constituent participant route - by literal name and
// by parameter - so the drop reaches only the constituent it resolves, and a
// cancellation before the statement publishes nothing anywhere.
func TestIndexDropProcedureFabricDynamicConstituentRouteDropsOwnKind(t *testing.T) {
	fixture := newIndexDropFabricFixture(t)
	fixture.createBothKinds(t, fixture.root, indexDropFabricGraphA)
	fixture.createBothKinds(t, fixture.root, indexDropFabricGraphB)

	byName, err := fixture.root.Execute(fixture.ctx,
		"USE graph.byName('"+indexDropFabricGraphA+"') "+indexDropFabricDropFT, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{indexDropFulltextName, true}}, byName.Rows)
	require.Equal(t, []string{indexDropVectorName}, fixture.liveNames(t, indexDropFabricShardA))
	require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardB))

	byParameter, err := fixture.root.Execute(fixture.ctx,
		"USE graph.byName($g) "+indexDropFabricDropVector, map[string]interface{}{"g": indexDropFabricGraphB})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{indexDropVectorName, true}}, byParameter.Rows)
	require.Equal(t, []string{indexDropVectorName}, fixture.liveNames(t, indexDropFabricShardA))
	require.Equal(t, []string{indexDropFulltextName}, fixture.liveNames(t, indexDropFabricShardB))

	request, cancel := context.WithCancel(fixture.ctx)
	cancel()
	result, err := fixture.root.Execute(request,
		"USE graph.byName('"+indexDropFabricGraphA+"') "+indexDropFabricDropVector, nil)
	require.Error(t, err, "a canceled dynamic constituent statement must be rejected")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result)
	require.Equal(t, []string{indexDropVectorName}, fixture.liveNames(t, indexDropFabricShardA))
	require.Equal(t, []string{indexDropFulltextName}, fixture.liveNames(t, indexDropFabricShardB))
}

// TestIndexDropProcedureFabricConstituentRouteRejectionPublishesNothing: when a
// composite statement is rejected after the drop has already run, the drop is
// not published - not on the constituent it named, and not on any other.
func TestIndexDropProcedureFabricConstituentRouteRejectionPublishesNothing(t *testing.T) {
	// The control: the same two-call statement shape publishes both drops when
	// nothing is rejected, so the rejected cases below really do roll back a
	// drop that already ran.
	t.Run("control_both_calls_publish", func(t *testing.T) {
		fixture := newIndexDropFabricFixture(t)
		fixture.createBothKinds(t, fixture.root, indexDropFabricGraphA)
		fixture.createBothKinds(t, fixture.root, indexDropFabricGraphB)

		result, err := fixture.root.Execute(fixture.ctx,
			"USE "+indexDropFabricGraphA+" "+indexDropFabricDropFT+" "+indexDropFabricDropVector, nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, []string{}, fixture.liveNames(t, indexDropFabricShardA))
		require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardB))
	})

	for _, procedure := range []struct {
		name, drop, rejected string
	}{
		{
			"later_call_fails",
			indexDropFabricDropFT,
			"USE " + indexDropFabricGraphA + " " + indexDropFabricDropFT + " " + indexDropFabricDropMissing,
		},
		{
			"second_constituent_write_refused",
			indexDropFabricDropVector,
			"USE " + indexDropFabricComposite +
				" CALL { USE " + indexDropFabricGraphA + " " + indexDropFabricDropVector + " RETURN 1 AS w }" +
				" CALL { USE " + indexDropFabricGraphB + " " + indexDropFabricDropFT + " RETURN 1 AS z } RETURN w, z",
		},
	} {
		t.Run(procedure.name, func(t *testing.T) {
			fixture := newIndexDropFabricFixture(t)
			fixture.createBothKinds(t, fixture.root, indexDropFabricGraphA)
			fixture.createBothKinds(t, fixture.root, indexDropFabricGraphB)

			result, err := fixture.root.Execute(fixture.ctx, procedure.rejected, nil)
			require.Error(t, err, "the statement must be rejected")
			require.Nil(t, result)

			require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardA),
				"the rejected statement must publish nothing on constituent a")
			require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardB),
				"the rejected statement must publish nothing on constituent b")

			// The staged drop can still be made afterwards, and publishes.
			retried, err := fixture.root.Execute(fixture.ctx, "USE "+indexDropFabricGraphA+" "+procedure.drop, nil)
			require.NoError(t, err)
			require.Len(t, retried.Rows, 1)
			require.Len(t, fixture.liveNames(t, indexDropFabricShardA), 1)
			require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardB))
		})
	}
}

// TestIndexDropProcedureFabricConstituentRoutePreCanceledPublishesNothing: a
// caller already canceled when the constituent statement is issued gets a
// rejection and no constituent is changed, and a healthy retry still drops only
// what it names.
func TestIndexDropProcedureFabricConstituentRoutePreCanceledPublishesNothing(t *testing.T) {
	for _, procedure := range []struct {
		name, statement, dropped string
		remaining                []string
	}{
		{"fulltext", indexDropFabricDropFT, indexDropFulltextName, []string{indexDropVectorName}},
		{"vector", indexDropFabricDropVector, indexDropVectorName, []string{indexDropFulltextName}},
	} {
		t.Run(procedure.name, func(t *testing.T) {
			fixture := newIndexDropFabricFixture(t)
			fixture.createBothKinds(t, fixture.root, indexDropFabricGraphA)
			fixture.createBothKinds(t, fixture.root, indexDropFabricGraphB)

			request, cancel := context.WithCancel(fixture.ctx)
			cancel()
			result, err := fixture.root.Execute(request, "USE "+indexDropFabricGraphA+" "+procedure.statement, nil)
			require.Error(t, err, "a canceled constituent statement must be rejected")
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, result)
			require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardA))
			require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardB))

			healthy, err := fixture.root.Execute(fixture.ctx, "USE "+indexDropFabricGraphA+" "+procedure.statement, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{procedure.dropped, true}}, healthy.Rows)
			require.Equal(t, procedure.remaining, fixture.liveNames(t, indexDropFabricShardA))
			require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardB))
		})
	}
}

// TestIndexDropProcedureFabricExplicitTransactionRollbackPublishesNothing: a
// composite explicit transaction that staged the drop and rolled back publishes
// nothing, on either constituent.
func TestIndexDropProcedureFabricExplicitTransactionRollbackPublishesNothing(t *testing.T) {
	fixture := newIndexDropFabricFixture(t)
	fixture.createBothKinds(t, fixture.root, indexDropFabricGraphA)
	fixture.createBothKinds(t, fixture.root, indexDropFabricGraphB)
	session := newCompositeTestExecutor(t, fixture.mgr, indexDropFabricComposite, fixture.root.dbManager)

	_, err := session.Execute(fixture.ctx, "BEGIN", nil)
	require.NoError(t, err)
	staged, err := session.Execute(fixture.ctx, "USE "+indexDropFabricGraphA+" "+indexDropFabricDropFT, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{indexDropFulltextName, true}}, staged.Rows)
	require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardA),
		"a staged drop is not published before COMMIT")
	require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardB))

	rolledBack, err := session.Execute(fixture.ctx, "ROLLBACK", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"Transaction rolled back"}}, rolledBack.Rows)
	require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardA))
	require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardB))

	// The route still publishes after the rollback.
	_, err = session.Execute(fixture.ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = session.Execute(fixture.ctx, "USE "+indexDropFabricGraphA+" "+indexDropFabricDropFT, nil)
	require.NoError(t, err)
	_, err = session.Execute(fixture.ctx, "COMMIT", nil)
	require.NoError(t, err)
	require.Equal(t, []string{indexDropVectorName}, fixture.liveNames(t, indexDropFabricShardA))
	require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardB))
}

// TestIndexDropProcedureFabricExplicitTransactionCancelledCommitPublishesNothing:
// the composite explicit transaction's COMMIT is the only publication owner of
// a staged constituent drop, so a canceled COMMIT must be rejected and publish
// nothing; a canceled statement inside the transaction must also leave the
// staged drop unpublished.
func TestIndexDropProcedureFabricExplicitTransactionCancelledCommitPublishesNothing(t *testing.T) {
	t.Run("cancelled_commit", func(t *testing.T) {
		fixture := newIndexDropFabricFixture(t)
		fixture.createBothKinds(t, fixture.root, indexDropFabricGraphA)
		fixture.createBothKinds(t, fixture.root, indexDropFabricGraphB)
		session := newCompositeTestExecutor(t, fixture.mgr, indexDropFabricComposite, fixture.root.dbManager)

		_, err := session.Execute(fixture.ctx, "BEGIN", nil)
		require.NoError(t, err)
		_, err = session.Execute(fixture.ctx, "USE "+indexDropFabricGraphA+" "+indexDropFabricDropFT, nil)
		require.NoError(t, err)
		require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardA))

		request, cancel := context.WithCancel(context.Background())
		defer cancel()
		cancel()
		result, err := session.Execute(request, "COMMIT", nil)
		require.Error(t, err, "a canceled COMMIT must be rejected")
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, result, "a canceled COMMIT must not report a commit")
		require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardA),
			"a canceled COMMIT must publish nothing")
		require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardB))

		// The transaction cannot publish its staged drop afterwards either.
		_, err = session.Execute(fixture.ctx, "COMMIT", nil)
		require.Error(t, err, "the canceled COMMIT ends the transaction")
		require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardA))

		// A healthy transaction on the same route publishes what it names.
		_, err = session.Execute(fixture.ctx, "BEGIN", nil)
		require.NoError(t, err)
		_, err = session.Execute(fixture.ctx, "USE "+indexDropFabricGraphA+" "+indexDropFabricDropFT, nil)
		require.NoError(t, err)
		_, err = session.Execute(fixture.ctx, "COMMIT", nil)
		require.NoError(t, err)
		require.Equal(t, []string{indexDropVectorName}, fixture.liveNames(t, indexDropFabricShardA))
		require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardB))
	})

	t.Run("cancelled_statement_in_transaction", func(t *testing.T) {
		fixture := newIndexDropFabricFixture(t)
		fixture.createBothKinds(t, fixture.root, indexDropFabricGraphA)
		fixture.createBothKinds(t, fixture.root, indexDropFabricGraphB)
		session := newCompositeTestExecutor(t, fixture.mgr, indexDropFabricComposite, fixture.root.dbManager)

		_, err := session.Execute(fixture.ctx, "BEGIN", nil)
		require.NoError(t, err)
		_, err = session.Execute(fixture.ctx, "USE "+indexDropFabricGraphA+" "+indexDropFabricDropFT, nil)
		require.NoError(t, err)

		request, cancel := context.WithCancel(context.Background())
		defer cancel()
		cancel()
		result, err := session.Execute(request, "USE "+indexDropFabricGraphA+" "+indexDropFabricDropVector, nil)
		require.Error(t, err, "a canceled statement must be rejected")
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, result)

		// The transaction the canceled statement failed in cannot commit, so
		// its staged drop is never published.
		_, err = session.Execute(fixture.ctx, "COMMIT", nil)
		require.Error(t, err, "a transaction a statement failed in cannot commit")
		require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardA))
		require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardB))
	})
}
