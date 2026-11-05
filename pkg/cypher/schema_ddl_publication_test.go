package cypher

// Public DDL publication proofs for an ordinary schema statement (PR1667 G1-2).
//
// An autocommit CREATE INDEX is one schema transaction: its declaration, the
// fill of that declaration from the stored population and the publication
// commit are one unit. A reader overlapping the statement therefore sees the
// complete old population or the complete new one, never a declared but
// unfilled index.
//
// The pause below is the fill's own entry point (storage's single documented
// index-fill test hook), so every observation is exact: the test reads while
// the fill is parked at a fixed control point instead of sleeping, and it
// reads a specific expectation instead of a loose bound. The hook is scoped to
// one declaration's identity (index name and label), so it parks exactly the
// declaration under test: a fill of another index or label passes through and
// leaves the armed state untouched. The only budget below is a join guard for
// a signal that never arrives (a lost observation); it never bounds an
// assertion.
//
// These tests install that package-level hook, so they must not call
// t.Parallel.

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// ddlStatementJoinBudget bounds a wait for a control point or for a launched
// statement that a lost observation could otherwise turn into a hung package
// run. It never bounds an assertion: a reached control point is exact, and a
// statement that did not finish inside it is reported as a failure.
const ddlStatementJoinBudget = 10 * time.Second

// ddlFillFailure is the injected failure of an index fill: the DDL must
// publish nothing, and the healthy retry must still work.
var ddlFillFailure = errors.New("index fill failed")

// ddlFillGate parks one composite index fill at its entry point. The canonical
// testfixture PauseGate owns the once-only arm and the idempotent release;
// this adapter owns only which fill consumes them and what that fill returns.
// It stands in for no product behavior.
type ddlFillGate struct {
	*fixtureowner.PauseGate
	indexName string
	label     string
	outcome   atomic.Pointer[error] // what the fill that consumes the arm returns
}

// newDDLFillGate installs the fill hook for indexName/label and restores it
// when the test ends. The release and the restoration are registered here, so
// they run after the cleanups of anything launched later in the test, and
// after the fixture's engine cleanup (which was registered before this gate).
// The constructor is the gate's only arming site: one gate parks exactly one
// fill, so no call site arms or rearms it.
func newDDLFillGate(t *testing.T, indexName, label string) *ddlFillGate {
	t.Helper()
	gate := &ddlFillGate{PauseGate: fixtureowner.NewPauseGate(), indexName: indexName, label: label}
	gate.Armed.Store(true)
	t.Cleanup(storage.SetSchemaIndexFillHookForTesting(gate.observeFill))
	t.Cleanup(gate.ReleaseWrites)
	return gate
}

// observeFill is the installed hook. A fill of another index or label is
// foreign to this gate: it passes straight through and leaves the arm and the
// recorded outcome untouched. Pause reports whether this caller consumed the
// one-shot arm, so only the fill that actually parked can return the recorded
// outcome: a later fill of the same identity, or one that never consumed the
// arm, reports success.
func (g *ddlFillGate) observeFill(indexName, label string) error {
	if indexName != g.indexName || label != g.label {
		return nil
	}
	if !g.Pause(g.Armed, g.Entered) {
		return nil
	}
	if failure := g.outcome.Load(); failure != nil {
		return *failure
	}
	return nil
}

// failNextFill makes the fill that consumes the arm return err; nil clears an
// earlier outcome.
func (g *ddlFillGate) failNextFill(err error) {
	if err == nil {
		g.outcome.Store(nil)
		return
	}
	g.outcome.Store(&err)
}

// describe names the declaration this gate is scoped to.
func (g *ddlFillGate) describe() string {
	return fmt.Sprintf("index %q on label %q", g.indexName, g.label)
}

// ddlLaunchedStatement is one statement running on its own goroutine. Its join
// is registered when it is launched, so a failed assertion still releases a
// parked fill and waits for the statement's actual return before the hook is
// restored and the fixture's engine is closed.
type ddlLaunchedStatement struct {
	what     string
	cancel   context.CancelFunc
	returned chan struct{} // closed after Execute has actually returned
	err      error         // the statement's outcome; readable once returned is closed
	joined   bool
}

// launchDDLStatement runs query on its own goroutine and closes returned when
// Execute has actually returned: the returned channel is the join, not the
// buffered outcome. The registered cleanup releases the gate first (so a
// parked fill can never stay parked behind a failed assertion), cancels the
// statement's own context, and then joins the actual return, before the gate's
// own cleanups and the engine cleanup run.
func launchDDLStatement(t *testing.T, gate *ddlFillGate, exec *StorageExecutor, ctx context.Context, query string) *ddlLaunchedStatement {
	t.Helper()
	statementCtx, cancel := context.WithCancel(ctx)
	statement := &ddlLaunchedStatement{
		what:     "the " + gate.describe() + " statement",
		cancel:   cancel,
		returned: make(chan struct{}),
	}
	go func() {
		defer close(statement.returned)
		_, statement.err = exec.Execute(statementCtx, query, nil)
	}()
	t.Cleanup(func() {
		gate.ReleaseWrites()
		statement.cancel()
		statement.join(t)
	})
	return statement
}

// wait waits for the statement's actual return. Only an observed return marks
// the statement joined and makes its outcome readable; a timeout leaves both
// untouched, so a still-running statement is never reported as joined.
func (s *ddlLaunchedStatement) wait(t *testing.T) bool {
	t.Helper()
	if s.joined {
		return true
	}
	select {
	case <-s.returned:
		s.joined = true
		return true
	case <-time.After(ddlStatementJoinBudget):
		return false
	}
}

// join waits for the statement's actual return. A statement that does not
// return inside the join guard is reported as a failure.
func (s *ddlLaunchedStatement) join(t *testing.T) {
	t.Helper()
	if !s.wait(t) {
		t.Errorf("%s did not return within %s", s.what, ddlStatementJoinBudget)
	}
}

// await collects the launched statement's outcome under the join guard. It is
// idempotent: a statement already joined returns its recorded outcome instead
// of waiting again.
func (s *ddlLaunchedStatement) await(t *testing.T) error {
	t.Helper()
	if !s.wait(t) {
		t.Fatalf("timed out waiting for %s to return", s.what)
	}
	return s.err
}

// runParked launches one statement, waits for this gate's fill to park, runs
// observe at that point, releases the fill and returns the statement's
// outcome. A gate parks exactly one fill, so a reused gate is rejected before
// the statement is launched: rearming it would park a second fill behind an
// already-closed release. A statement that returns without reaching the armed
// fill is reported as an unconsumed arm instead of waiting for a control point
// that cannot arrive.
func (g *ddlFillGate) runParked(t *testing.T, exec *StorageExecutor, ctx context.Context, query string, observe func()) error {
	t.Helper()
	require.True(t, g.Armed.Load(),
		"%s: runParked needs the constructor-armed gate; it parks exactly one fill and is never rearmed", g.describe())
	statement := launchDDLStatement(t, g, exec, ctx, query)
	select {
	case <-g.Entered:
	case <-statement.returned:
		t.Fatalf("the statement returned (%v) without reaching the armed %s fill", statement.err, g.describe())
	case <-time.After(ddlStatementJoinBudget):
		t.Fatalf("timed out waiting for the armed %s fill", g.describe())
	}
	require.False(t, g.Armed.Load(), "the parked fill left the one-shot arm armed")
	if observe != nil {
		observe()
	}
	g.ReleaseWrites()
	return statement.await(t)
}

// ddlPublicationFixture is one test engine stack: the native engine the test
// seeds and inspects, and the namespace-scoped storage the executor uses.
type ddlPublicationFixture struct {
	engine storage.Engine
	store  storage.Engine
}

func newDDLPublicationFixture(t *testing.T, backend string) *ddlPublicationFixture {
	t.Helper()
	var engine storage.Engine
	switch backend {
	case "memory":
		engine = newTestMemoryEngine(t)
	case "badger":
		badgerEngine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(filepath.Join(t.TempDir(), "data")))
		require.NoError(t, err)
		t.Cleanup(func() { _ = badgerEngine.Close() })
		engine = badgerEngine
	default:
		t.Fatalf("unknown storage backend %q", backend)
	}
	return &ddlPublicationFixture{engine: engine, store: storage.NewNamespacedEngine(engine, "ddl")}
}

func (f *ddlPublicationFixture) executor() *StorageExecutor {
	return NewStorageExecutor(f.store)
}

// seedDDLPopulation creates one node per value through the public statement
// path, so the population is ordinary stored data.
func seedDDLPopulation(t *testing.T, exec *StorageExecutor, label, property string, values ...string) {
	t.Helper()
	for _, value := range values {
		_, err := exec.Execute(context.Background(),
			fmt.Sprintf("CREATE (n:%s {%s: $value})", label, property),
			map[string]any{"value": value})
		require.NoError(t, err)
	}
}

// ddlEqualityValues returns the sorted property values an equality MATCH
// returns. alias keeps every statement text distinct, so the executor's
// per-statement result cache can never serve an earlier population to a later
// assertion.
func ddlEqualityValues(t *testing.T, exec *StorageExecutor, label, property, value, alias string) []string {
	t.Helper()
	result, err := exec.Execute(context.Background(),
		fmt.Sprintf("MATCH (n:%s) WHERE n.%s = $value RETURN n.%s AS %s", label, property, property, alias),
		map[string]any{"value": value})
	require.NoError(t, err)
	values := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		require.Len(t, row, 1)
		name, ok := row[0].(string)
		require.Truef(t, ok, "property value is not a string: %#v", row[0])
		values = append(values, name)
	}
	sort.Strings(values)
	return values
}

// ddlPublishedCompositeIndex reports whether a schema holds a composite index
// with this name.
func ddlPublishedCompositeIndex(schema *storage.SchemaManager, name string) bool {
	if schema == nil {
		return false
	}
	index, ok := schema.GetCompositeIndex(name)
	return ok && index != nil
}

// ddlIndexValueOwnerIDs returns the node IDs a schema's filled equality index
// holds for one value: the index's own content, not a scan.
func ddlIndexValueOwnerIDs(t *testing.T, store storage.Engine, label, property, value string) []storage.NodeID {
	t.Helper()
	schema := store.GetSchema()
	require.NotNil(t, schema)
	ids, err := schema.PropertyIndexLookup(context.Background(), label, property, value)
	require.NoError(t, err)
	return ids
}

// ddlSuppressor exposes the storage's stored-entity suppression capability.
type ddlSuppressor interface {
	SetManualSuppression(context.Context, storage.StoredEntityKind, string, bool) error
}

// TestOrdinarySchemaDDLStatementContract pins the classification the executor
// routes on: an ordinary schema statement is a write with a transaction, while
// catalog commands and schema introspection keep their own contract.
func TestOrdinarySchemaDDLStatementContract(t *testing.T) {
	for _, body := range []string{
		"CREATE INDEX ix FOR (n:L) ON (n.p)",
		"CREATE RANGE INDEX ix FOR (n:L) ON (n.p)",
		"CREATE VECTOR INDEX ix FOR (n:L) ON (n.p)",
		"CREATE FULLTEXT INDEX ix FOR (n:L) ON EACH [n.p]",
		"CREATE CONSTRAINT c FOR (n:L) REQUIRE n.p IS UNIQUE",
		"DROP INDEX ix IF EXISTS",
		"DROP CONSTRAINT c IF EXISTS",
	} {
		contract, err := readStatementExecutionContract(t.Context(), body)
		require.NoError(t, err, body)
		require.True(t, contract.writes, body)
		require.True(t, contract.transaction, body)
		require.True(t, isOrdinarySchemaDDL(t.Context(), body), body)
	}
	for _, body := range []string{"SHOW INDEXES", "SHOW CONSTRAINTS"} {
		contract, err := readStatementExecutionContract(t.Context(), body)
		require.NoError(t, err, body)
		require.False(t, contract.writes, body)
		require.False(t, contract.transaction, body)
	}
	// Catalog commands keep their own owner: they are metadata commands, not
	// ordinary schema DDL, whatever their own contract reports.
	for _, body := range []string{"CREATE DATABASE d", "DROP DATABASE d", "CREATE ALIAS a FOR DATABASE d", "DROP ALIAS a IF EXISTS"} {
		require.False(t, isOrdinarySchemaDDL(t.Context(), body), body)
		require.True(t, isMetadataCommand(t.Context(), body, metadataCatalogExecution), body)
	}
}

// TestCreateIndexPublishesCompletePopulation parks the fill of an autocommit
// CREATE INDEX and reads the same population concurrently: the declaration and
// its fill are invisible until they commit together, so the reader sees the
// complete old population (the label scan) and, after the commit, the complete
// population through the filled index. An empty or partial answer fails.
func TestCreateIndexPublishesCompletePopulation(t *testing.T) {
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			forEachParser(t, func(t *testing.T) {
				fixture := newDDLPublicationFixture(t, backend)
				const label, property, index = "PausedPerson", "name", "ix_paused_population"
				gate := newDDLFillGate(t, index, label)
				creator, reader := fixture.executor(), fixture.executor()
				seedDDLPopulation(t, creator, label, property, "alice", "alice", "alice", "bob", "bob", "carol")

				require.Equal(t, []string{"alice", "alice", "alice"},
					ddlEqualityValues(t, reader, label, property, "alice", "before"))
				require.False(t, ddlPublishedCompositeIndex(fixture.store.GetSchema(), index))

				err := gate.runParked(t, creator, context.Background(),
					"CREATE INDEX "+index+" FOR (n:"+label+") ON (n."+property+")",
					func() {
						// The fill is parked: the schema transaction has declared
						// the index on its own view and has published nothing.
						require.False(t, ddlPublishedCompositeIndex(fixture.store.GetSchema(), index),
							"a declared index must not be visible before its fill commits")
						require.Equal(t, []string{"alice", "alice", "alice"},
							ddlEqualityValues(t, reader, label, property, "alice", "paused_alice"))
						require.Equal(t, []string{"bob", "bob"},
							ddlEqualityValues(t, reader, label, property, "bob", "paused_bob"))
						require.Equal(t, []string{"carol"},
							ddlEqualityValues(t, reader, label, property, "carol", "paused_carol"))
						require.Empty(t, ddlEqualityValues(t, reader, label, property, "missing", "paused_missing"),
							"a paused declaration returned rows for a value the population does not hold")
					})
				require.NoError(t, err)

				require.True(t, ddlPublishedCompositeIndex(fixture.store.GetSchema(), index))
				require.Len(t, ddlIndexValueOwnerIDs(t, fixture.store, label, property, "alice"), 3,
					"the published index does not hold every stored alice")
				require.Len(t, ddlIndexValueOwnerIDs(t, fixture.store, label, property, "bob"), 2)
				require.Len(t, ddlIndexValueOwnerIDs(t, fixture.store, label, property, "carol"), 1)
				require.Equal(t, []string{"alice", "alice", "alice"},
					ddlEqualityValues(t, reader, label, property, "alice", "published_alice"))
				require.Equal(t, []string{"bob", "bob"},
					ddlEqualityValues(t, reader, label, property, "bob", "published_bob"))
				require.Equal(t, []string{"carol"},
					ddlEqualityValues(t, reader, label, property, "carol", "published_carol"))
			})
		})
	}
}

// TestCreateIndexFillFailurePublishesNoSchema fails the fill: the statement
// fails, no index is published, reads stay complete, and the healthy retry
// declares and fills the index. The retry also shows the arm is one-shot: its
// fill of the same identity passes through unparked.
func TestCreateIndexFillFailurePublishesNoSchema(t *testing.T) {
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newDDLPublicationFixture(t, backend)
			const label, property, index = "FailedFillPerson", "name", "ix_failed_fill"
			gate := newDDLFillGate(t, index, label)
			exec := fixture.executor()
			seedDDLPopulation(t, exec, label, property, "alice", "alice", "bob")
			statement := "CREATE INDEX " + index + " FOR (n:" + label + ") ON (n." + property + ")"

			gate.failNextFill(ddlFillFailure)
			err := gate.runParked(t, exec, context.Background(), statement, nil)
			require.ErrorContains(t, err, ddlFillFailure.Error())

			require.False(t, ddlPublishedCompositeIndex(fixture.store.GetSchema(), index),
				"a failed fill left a declared index behind")
			require.Equal(t, []string{"alice", "alice"},
				ddlEqualityValues(t, exec, label, property, "alice", "failed_alice"))
			require.Equal(t, []string{"bob"},
				ddlEqualityValues(t, exec, label, property, "bob", "failed_bob"))

			// The healthy retry is a new statement with the same text.
			gate.failNextFill(nil)
			_, err = exec.Execute(context.Background(), statement, nil)
			require.NoError(t, err)
			require.True(t, ddlPublishedCompositeIndex(fixture.store.GetSchema(), index))
			require.Len(t, ddlIndexValueOwnerIDs(t, fixture.store, label, property, "alice"), 2)
			require.Equal(t, []string{"alice", "alice"},
				ddlEqualityValues(t, exec, label, property, "alice", "retried_alice"))
		})
	}
}

// TestCreateIndexCancellationPublishesNoSchema cancels the caller while the
// fill is parked: the statement reports the cancellation, publishes nothing,
// leaks no partial schema, and the same statement succeeds when retried.
func TestCreateIndexCancellationPublishesNoSchema(t *testing.T) {
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newDDLPublicationFixture(t, backend)
			const label, property, index = "CancelledFillPerson", "name", "ix_cancelled_fill"
			gate := newDDLFillGate(t, index, label)
			exec := fixture.executor()
			seedDDLPopulation(t, exec, label, property, "alice", "alice", "bob")
			statement := "CREATE INDEX " + index + " FOR (n:" + label + ") ON (n." + property + ")"

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			err := gate.runParked(t, exec, ctx, statement, func() {
				require.False(t, ddlPublishedCompositeIndex(fixture.store.GetSchema(), index))
				cancel()
			})
			require.ErrorIs(t, err, context.Canceled)

			require.False(t, ddlPublishedCompositeIndex(fixture.store.GetSchema(), index),
				"a cancelled fill left a declared index behind")
			require.Equal(t, []string{"alice", "alice"},
				ddlEqualityValues(t, exec, label, property, "alice", "cancelled_alice"))

			_, err = exec.Execute(context.Background(), statement, nil)
			require.NoError(t, err)
			require.True(t, ddlPublishedCompositeIndex(fixture.store.GetSchema(), index))
			require.Len(t, ddlIndexValueOwnerIDs(t, fixture.store, label, property, "alice"), 2)
		})
	}
}

// TestCreateIndexCommitRejectionPublishesNoSchema advances the database's
// commit sequence while a fill is parked, so the schema transaction's commit
// is rejected by its own snapshot check. The rejection publishes no schema,
// every stored node stays readable, and the retry fills the index from the
// complete population.
func TestCreateIndexCommitRejectionPublishesNoSchema(t *testing.T) {
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newDDLPublicationFixture(t, backend)
			const label, property, index = "RejectedCommitPerson", "name", "ix_rejected_commit"
			gate := newDDLFillGate(t, index, label)
			exec := fixture.executor()
			seedDDLPopulation(t, exec, label, property, "alice", "alice", "bob")

			err := gate.runParked(t, exec, context.Background(),
				"CREATE INDEX "+index+" FOR (n:"+label+") ON (n."+property+")",
				func() {
					// A concurrent entity write changes the namespace's commit
					// sequence: the parked schema transaction's snapshot is stale.
					_, writeErr := exec.Execute(context.Background(),
						"CREATE (:RejectedCommitPerson {name: 'dave'})", nil)
					require.NoError(t, writeErr)
				})
			require.Error(t, err, "a schema transaction whose snapshot changed must not commit")
			t.Logf("rejected schema commit: %v", err)
			require.False(t, ddlPublishedCompositeIndex(fixture.store.GetSchema(), index),
				"a rejected commit left a declared index behind")
			require.Equal(t, []string{"alice", "alice"},
				ddlEqualityValues(t, exec, label, property, "alice", "rejected_alice"))
			require.Equal(t, []string{"dave"},
				ddlEqualityValues(t, exec, label, property, "dave", "rejected_dave"))

			_, err = exec.Execute(context.Background(),
				"CREATE INDEX "+index+" FOR (n:"+label+") ON (n."+property+")", nil)
			require.NoError(t, err)
			require.True(t, ddlPublishedCompositeIndex(fixture.store.GetSchema(), index))
			require.Len(t, ddlIndexValueOwnerIDs(t, fixture.store, label, property, "alice"), 2)
			require.Len(t, ddlIndexValueOwnerIDs(t, fixture.store, label, property, "dave"), 1,
				"the retried fill missed the write that rejected the first commit")
		})
	}
}

// TestDropIndexPublishesAtCommit keeps a DROP INDEX in an explicit
// transaction: readers see the complete population through the index the whole
// time it is only staged, and the complete population by label scan once the
// removal commits.
func TestDropIndexPublishesAtCommit(t *testing.T) {
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newDDLPublicationFixture(t, backend)
			exec := fixture.executor()
			const label, property, index = "DroppedIndexPerson", "name", "ix_dropped_index"
			seedDDLPopulation(t, exec, label, property, "alice", "alice", "bob")
			_, err := exec.Execute(context.Background(),
				"CREATE INDEX "+index+" FOR (n:"+label+") ON (n."+property+")", nil)
			require.NoError(t, err)
			require.True(t, ddlPublishedCompositeIndex(fixture.store.GetSchema(), index))
			require.Len(t, ddlIndexValueOwnerIDs(t, fixture.store, label, property, "alice"), 2)

			_, err = exec.Execute(context.Background(), "BEGIN", nil)
			require.NoError(t, err)
			_, err = exec.Execute(context.Background(), "DROP INDEX "+index, nil)
			require.NoError(t, err)

			// Staged: the committed schema and its index are unchanged.
			require.True(t, ddlPublishedCompositeIndex(fixture.store.GetSchema(), index),
				"a staged DROP INDEX removed the committed index")
			require.Equal(t, []string{"alice", "alice"},
				ddlEqualityValues(t, exec, label, property, "alice", "staged_drop_alice"))

			_, err = exec.Execute(context.Background(), "COMMIT", nil)
			require.NoError(t, err)
			require.False(t, ddlPublishedCompositeIndex(fixture.store.GetSchema(), index))
			require.Equal(t, []string{"alice", "alice"},
				ddlEqualityValues(t, exec, label, property, "alice", "committed_drop_alice"),
				"the committed DROP INDEX lost stored nodes")
			require.Equal(t, []string{"bob"},
				ddlEqualityValues(t, exec, label, property, "bob", "committed_drop_bob"))

			_, err = exec.Execute(context.Background(), "DROP INDEX "+index+" IF EXISTS", nil)
			require.NoError(t, err, "DROP INDEX IF EXISTS on a missing index must be a no-op")
		})
	}
}

// TestCreateIndexIFNotExistsIsNoOp : the canonical duplicate rule changes
// nothing for an index that already carries this name, and the existing index
// keeps serving the complete population.
func TestCreateIndexIFNotExistsIsNoOp(t *testing.T) {
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newDDLPublicationFixture(t, backend)
			exec := fixture.executor()
			const label, property, index = "IdempotentPerson", "name", "ix_idempotent"
			seedDDLPopulation(t, exec, label, property, "alice", "alice", "bob")
			statement := "CREATE INDEX " + index + " IF NOT EXISTS FOR (n:" + label + ") ON (n." + property + ")"
			_, err := exec.Execute(context.Background(), statement, nil)
			require.NoError(t, err)
			ownerIDs := ddlIndexValueOwnerIDs(t, fixture.store, label, property, "alice")
			require.Len(t, ownerIDs, 2)

			_, err = exec.Execute(context.Background(), statement, nil)
			require.NoError(t, err)
			require.Equal(t, ownerIDs, ddlIndexValueOwnerIDs(t, fixture.store, label, property, "alice"),
				"the repeated declaration changed the index content")
			require.Equal(t, []string{"alice", "alice"},
				ddlEqualityValues(t, exec, label, property, "alice", "idempotent_alice"))
			require.Equal(t, []string{"bob"},
				ddlEqualityValues(t, exec, label, property, "bob", "idempotent_bob"))
		})
	}
}

// TestCreateIndexFillsFromStoredHiddenNodes : the fill reads the stored
// population, so a node the visibility layer hides from user reads is in the
// published index and is found through it once it is shown again.
func TestCreateIndexFillsFromStoredHiddenNodes(t *testing.T) {
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newDDLPublicationFixture(t, backend)
			exec := fixture.executor()
			const label, property, index = "HiddenPopulationPerson", "name", "ix_hidden_population"
			seedDDLPopulation(t, exec, label, property, "alice", "alice", "bob")

			stored, err := storage.GetStoredNodesByLabel(context.Background(), fixture.engine, label)
			require.NoError(t, err)
			hiddenID := storage.NodeID("")
			for _, node := range stored {
				if value, _ := node.Properties[property].(string); value == "bob" {
					hiddenID = node.ID
				}
			}
			require.NotEmpty(t, hiddenID)
			suppressor, ok := fixture.engine.(ddlSuppressor)
			require.True(t, ok, "the fixture engine must support stored-entity suppression")
			require.NoError(t, suppressor.SetManualSuppression(context.Background(), storage.StoredEntityNodeKind, string(hiddenID), true))

			require.Equal(t, []string{"alice", "alice"},
				ddlEqualityValues(t, exec, label, property, "alice", "hidden_alice"))
			require.Empty(t, ddlEqualityValues(t, exec, label, property, "bob", "hidden_bob"),
				"the fixture must hide bob from user reads")

			_, err = exec.Execute(context.Background(),
				"CREATE INDEX "+index+" FOR (n:"+label+") ON (n."+property+")", nil)
			require.NoError(t, err)
			require.Len(t, ddlIndexValueOwnerIDs(t, fixture.store, label, property, "alice"), 2)
			require.Len(t, ddlIndexValueOwnerIDs(t, fixture.store, label, property, "bob"), 1,
				"the fill skipped a stored node the visibility layer hides")

			require.NoError(t, suppressor.SetManualSuppression(context.Background(), storage.StoredEntityNodeKind, string(hiddenID), false))
			require.Equal(t, []string{"bob"},
				ddlEqualityValues(t, exec, label, property, "bob", "revealed_bob"))
		})
	}
}

// TestCreateIndexFillsFromBufferedWritePopulation : with write-behind on, a
// node acknowledged into the buffer but not yet flushed is stored population.
// The fill must take it, so the published index finds it after the buffer
// drained.
func TestCreateIndexFillsFromBufferedWritePopulation(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:             filepath.Join(t.TempDir(), "data"),
		WriteBehind:         true,
		WriteBehindInterval: time.Hour, // nothing flushes during the declaration
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	store := storage.NewNamespacedEngine(engine, "ddl")
	exec := NewStorageExecutor(store)
	const label, property, index = "BufferedPopulationPerson", "name", "ix_buffered_population"
	seedDDLPopulation(t, exec, label, property, "alice", "alice", "bob")
	require.Equal(t, []string{"alice", "alice"},
		ddlEqualityValues(t, exec, label, property, "alice", "buffered_alice"))

	_, err = exec.Execute(context.Background(),
		"CREATE INDEX "+index+" FOR (n:"+label+") ON (n."+property+")", nil)
	require.NoError(t, err)
	require.True(t, ddlPublishedCompositeIndex(store.GetSchema(), index))

	require.NoError(t, engine.FlushWriteBehind(context.Background()))
	require.Len(t, ddlIndexValueOwnerIDs(t, store, label, property, "alice"), 2,
		"the fill missed a node that was still buffered")
	require.Len(t, ddlIndexValueOwnerIDs(t, store, label, property, "bob"), 1)
	require.Equal(t, []string{"alice", "alice"},
		ddlEqualityValues(t, exec, label, property, "alice", "flushed_alice"))
}

// TestSchemaIndexFillGateIgnoresForeignFills is the negative control of the
// seam's identity scope: an armed gate neither parks, fails nor spends its arm
// on a fill of another index or another label, and the declaration it is
// scoped to still parks and publishes.
func TestSchemaIndexFillGateIgnoresForeignFills(t *testing.T) {
	const label, otherLabel, property = "ArmedIdentityPerson", "OtherIdentityPerson", "name"
	const index, otherIndex = "ix_armed_identity", "ix_other_identity"

	for _, scenario := range []struct {
		name      string
		statement string
	}{
		{name: "another index name", statement: "CREATE INDEX " + otherIndex + " FOR (n:" + label + ") ON (n." + property + ")"},
		{name: "another label", statement: "CREATE INDEX " + index + " FOR (n:" + otherLabel + ") ON (n." + property + ")"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			fixture := newDDLPublicationFixture(t, "memory")
			gate := newDDLFillGate(t, index, label)
			exec := fixture.executor()
			seedDDLPopulation(t, exec, label, property, "alice", "bob")
			require.True(t, gate.Armed.Load(), "the constructor must arm the gate")

			_, err := exec.Execute(context.Background(), scenario.statement, nil)
			require.NoError(t, err, "a foreign fill must not be parked or failed")

			select {
			case <-gate.Entered:
				t.Fatal("a foreign fill consumed the gate's arm")
			default:
			}
			require.True(t, gate.Armed.Load(), "a foreign fill spent the gate's arm")
		})
	}

	t.Run("the armed declaration still parks", func(t *testing.T) {
		fixture := newDDLPublicationFixture(t, "badger")
		gate := newDDLFillGate(t, index, label)
		exec := fixture.executor()
		seedDDLPopulation(t, exec, label, property, "alice", "alice", "bob")

		err := gate.runParked(t, exec, context.Background(),
			"CREATE INDEX "+index+" FOR (n:"+label+") ON (n."+property+")",
			func() {
				require.False(t, ddlPublishedCompositeIndex(fixture.store.GetSchema(), index))
				require.Equal(t, []string{"alice", "alice"},
					ddlEqualityValues(t, exec, label, property, "alice", "armed_alice"))
			})
		require.NoError(t, err)
		require.True(t, ddlPublishedCompositeIndex(fixture.store.GetSchema(), index))
		require.Len(t, ddlIndexValueOwnerIDs(t, fixture.store, label, property, "alice"), 2)
	})
}
