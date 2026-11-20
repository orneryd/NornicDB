// NOR-312 participant-failure scope: BEGIN failure, participant access
// failure on both statement parsers, and registration refusal at the
// canonical participant owner. Healthy controls and post-failure state checks
// run with each case.
package cypher

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// beginFailingConstituent is a constituent whose transaction cannot begin. It
// keeps the real engine for every other operation, so the only failure under
// test is the participant's BEGIN.
type beginFailingConstituent struct {
	storage.Engine
	failure error
}

func (e *beginFailingConstituent) BeginTransaction(context.Context, storage.BeginRequest) (*storage.BadgerTransaction, error) {
	return nil, e.failure
}

// constituentEngineManager hands the named databases the engines a statement
// resolves through GetStorageForUse; every other name stays canonical. This is
// the production seam a composite statement's USE and participant resolution
// use, so the injected failure occurs on the public path.
type constituentEngineManager struct {
	*testDatabaseManagerAdapter
	engines map[string]storage.Engine
}

func (m *constituentEngineManager) GetStorageForUse(name, token string) (interface{}, error) {
	if engine, ok := m.engines[name]; ok {
		return engine, nil
	}
	return m.testDatabaseManagerAdapter.GetStorageForUse(name, token)
}

func forEachStatementParser(t *testing.T, run func(t *testing.T)) {
	t.Helper()
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			require.Equal(t, parser, config.GetParserType())
			run(t)
		})
	}
}

// compositeParticipantReadShapes are the affected explicit read families: a
// union read, an aggregate over two participants, and a single-participant
// property read. A read-only statement opens no participant transaction, so
// these shapes prove access failure, not BEGIN failure.
var compositeParticipantReadShapes = []struct{ name, query string }{
	{"union", "CALL { USE comp.one MATCH (n:Evidence) RETURN n.value AS value UNION ALL USE comp.two MATCH (n:Evidence) RETURN n.value AS value } RETURN value"},
	{"aggregate", "CALL { USE comp.one MATCH (n:Evidence) RETURN count(n) AS a } CALL { USE comp.two MATCH (n:Evidence) RETURN count(n) AS b } RETURN a + b AS total"},
	{"single_use", "CALL { USE comp.two MATCH (n:Evidence) RETURN n.value AS value } RETURN value"},
}

// compositeParticipantTransactionShapes are statements that open a participant
// transaction: a write followed by the second constituent's count (test-matrix
// row 1) and a direct write to the second constituent.
var compositeParticipantTransactionShapes = []struct{ name, query string }{
	{"write_one_count_two", "CALL { USE comp.one CREATE (:Evidence {value: $run}) RETURN 1 AS w } CALL { USE comp.two MATCH (n:Evidence) RETURN count(n) AS c } RETURN w, c"},
	{"write_two", "CALL { USE comp.two CREATE (:Evidence {value: $run}) RETURN 1 AS w } RETURN w"},
}

func seededCompositeFixture(t *testing.T) *fabricAtomicityFixture {
	t.Helper()
	f := newFabricAtomicityFixture(t)
	for _, seed := range []struct{ database, value string }{{"one", "one"}, {"two", "two"}} {
		engine, err := f.mgr.GetStorage(seed.database)
		require.NoError(t, err)
		_, err = NewStorageExecutor(engine).Execute(t.Context(), "CREATE (:Evidence {value: '"+seed.value+"'})", nil)
		require.NoError(t, err)
	}
	return f
}

func evidenceCount(t *testing.T, f *fabricAtomicityFixture, database string) int64 {
	t.Helper()
	engine, err := f.mgr.GetStorage(database)
	require.NoError(t, err)
	result, err := NewStorageExecutor(engine).Execute(t.Context(), "MATCH (n:Evidence) RETURN count(n) AS c", nil)
	require.NoError(t, err, "constituent %s must stay readable", database)
	return result.Rows[0][0].(int64)
}

// requireNoRunWrites asserts the run value never reached either constituent.
func requireNoRunWrites(t *testing.T, f *fabricAtomicityFixture, run string) {
	t.Helper()
	for _, database := range []string{"one", "two"} {
		engine, err := f.mgr.GetStorage(database)
		require.NoError(t, err)
		result, err := NewStorageExecutor(engine).Execute(t.Context(),
			"MATCH (n:Evidence {value: $run}) RETURN count(n) AS c", map[string]interface{}{"run": run})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows, "constituent %s keeps nothing of the failed statement", database)
	}
}

func valuesOf(result *ExecuteResult) []string {
	values := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		if len(row) == 0 {
			continue
		}
		value, _ := row[0].(string)
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

// TestCompositeParticipantBeginFailureFailsPublicCount is test-matrix row 1 on
// both statement parsers: a two-constituent composite whose second BEGIN
// fails must fail the statement instead of returning a partial count, and it
// must leave both constituents and the participant lifecycle intact.
func TestCompositeParticipantBeginFailureFailsPublicCount(t *testing.T) {
	forEachStatementParser(t, func(t *testing.T) {
		f := seededCompositeFixture(t)
		healthy := newCompositeTestExecutor(t, f.mgr, "comp", &testDatabaseManagerAdapter{manager: f.mgr})

		joined, err := healthy.Execute(t.Context(), compositeParticipantTransactionShapes[0].query,
			map[string]interface{}{"run": "healthy-join"})
		require.NoError(t, err, "healthy control keeps the write and the second constituent's count")
		require.Equal(t, [][]interface{}{{int64(1), int64(1)}}, joined.Rows)
		written, err := healthy.Execute(t.Context(), compositeParticipantTransactionShapes[1].query,
			map[string]interface{}{"run": "healthy-two"})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, written.Rows)
		oneBefore, twoBefore := evidenceCount(t, f, "one"), evidenceCount(t, f, "two")
		require.Equal(t, int64(2), oneBefore)
		require.Equal(t, int64(2), twoBefore)

		beginFailure := errors.New("second participant could not begin")
		two, err := f.mgr.GetStorage("two")
		require.NoError(t, err)
		failingEngine := &beginFailingConstituent{Engine: two, failure: beginFailure}
		adapter := &constituentEngineManager{
			testDatabaseManagerAdapter: &testDatabaseManagerAdapter{manager: f.mgr},
			engines:                    map[string]storage.Engine{"two": failingEngine, "comp.two": failingEngine},
		}
		failing := newCompositeTestExecutor(t, f.mgr, "comp", adapter)
		for _, shape := range compositeParticipantTransactionShapes {
			t.Run("begin_failure/"+shape.name, func(t *testing.T) {
				run := "failed-" + shape.name
				result, err := failing.Execute(t.Context(), shape.query, map[string]interface{}{"run": run})
				require.Error(t, err, "a failed participant BEGIN cannot be reported as a successful partial read")
				require.ErrorIs(t, err, beginFailure, "the original BEGIN failure must reach the caller")
				require.Nil(t, result, "no partial rows for a statement whose participant could not begin")
				requireNoRunWrites(t, f, run)
			})
		}
		require.Equal(t, oneBefore, evidenceCount(t, f, "one"), "a failed statement keeps none of its writes")
		require.Equal(t, twoBefore, evidenceCount(t, f, "two"))

		// Participant lifecycle: a constituent that failed to begin stays usable
		// by the next statement.
		again, err := healthy.Execute(t.Context(), compositeParticipantTransactionShapes[1].query,
			map[string]interface{}{"run": "after-failure"})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}}, again.Rows)
		require.Equal(t, twoBefore+1, evidenceCount(t, f, "two"), "the next healthy statement commits normally")
	})
}

// TestCompositeParticipantAccessFailureOnBothParsers closes the parser gap in
// composite_access_failure_test.go: the existing test proves the union and
// aggregate families only on the configured parser. The participant manager
// here denies the second constituent during resolution.
func TestCompositeParticipantAccessFailureOnBothParsers(t *testing.T) {
	forEachStatementParser(t, func(t *testing.T) {
		f := seededCompositeFixture(t)
		healthy := newCompositeTestExecutor(t, f.mgr, "comp", &testDatabaseManagerAdapter{manager: f.mgr})
		accessFailure := errors.New("second participant was denied")
		adapter := &failingCompositeAccessManager{
			testDatabaseManagerAdapter: &testDatabaseManagerAdapter{manager: f.mgr},
			failure:                    accessFailure,
		}
		failing := newCompositeTestExecutor(t, f.mgr, "comp", adapter)
		for _, shape := range compositeParticipantReadShapes {
			t.Run(shape.name, func(t *testing.T) {
				healthyResult, err := healthy.Execute(t.Context(), shape.query, nil)
				require.NoError(t, err)
				require.NotEmpty(t, healthyResult.Rows)
				result, err := failing.Execute(t.Context(), shape.query, nil)
				require.Error(t, err)
				require.ErrorIs(t, err, accessFailure, "the participant access failure reaches the caller")
				require.Nil(t, result)
			})
		}
	})
}

// TestCompositeReadOnlyStatementDoesNotBeginParticipants records a concrete
// limit of test-matrix row 1: a read-only composite statement opens no
// participant transaction, so a constituent whose BEGIN would fail does not
// affect it. BEGIN-failure coverage therefore needs a statement that opens a
// participant transaction, as above.
func TestCompositeReadOnlyStatementDoesNotBeginParticipants(t *testing.T) {
	forEachStatementParser(t, func(t *testing.T) {
		f := seededCompositeFixture(t)
		two, err := f.mgr.GetStorage("two")
		require.NoError(t, err)
		beginFailure := errors.New("second participant could not begin")
		engine := &beginFailingConstituent{Engine: two, failure: beginFailure}
		adapter := &constituentEngineManager{
			testDatabaseManagerAdapter: &testDatabaseManagerAdapter{manager: f.mgr},
			engines:                    map[string]storage.Engine{"two": engine, "comp.two": engine},
		}
		exec := newCompositeTestExecutor(t, f.mgr, "comp", adapter)
		result, err := exec.Execute(t.Context(), compositeParticipantReadShapes[0].query, nil)
		require.NoError(t, err, "a read-only statement needs no participant transaction")
		require.Equal(t, []string{"one", "two"}, valuesOf(result))
	})
}

// TestCompositeParticipantRegistrationFailureIsReported is test-matrix row 3
// at the canonical owner (cypherFabricExecutor.ensureLocalShardTxExecutor).
// Registration through an ended coordinator must return a failure, must not
// publish the participant, and must roll back the BEGIN it already opened.
// A later, healthy participant creation with an open coordinator proves the
// failure did not leak a transaction or an executor.
func TestCompositeParticipantRegistrationFailureIsReported(t *testing.T) {
	ctx := context.Background()
	base := newTestMemoryEngine(t)
	engine := &ownershipBeginCapture{Engine: base, begin: base.BeginTransaction}
	t.Cleanup(func() {
		if engine.started != nil && engine.started.IsActive() {
			require.NoError(t, engine.started.Rollback())
		}
	})
	runningTransactions.mu.RLock()
	before := make(map[uint64]bool, len(runningTransactions.byNumber))
	for id := range runningTransactions.byNumber {
		before[id] = true
	}
	runningTransactions.mu.RUnlock()
	t.Cleanup(func() {
		runningTransactions.mu.RLock()
		var issued []*runningTransaction
		for id, entry := range runningTransactions.byNumber {
			if !before[id] {
				issued = append(issued, entry)
			}
		}
		runningTransactions.mu.RUnlock()
		for _, entry := range issued {
			runningTransactions.end(entry)
		}
	})

	tx := fabric.NewFabricTransaction("nor-312-registration")
	sub, err := tx.GetOrOpen("local:one", false)
	require.NoError(t, err)
	dispatcher := &cypherFabricExecutor{base: NewStorageExecutor(engine), autoCommit: true, transaction: tx}

	require.NoError(t, tx.Rollback(ctx, nil), "end the coordinator before the participant registers")
	participant, err := dispatcher.ensureLocalShardTxExecutor(ctx, sub, "one", engine)
	require.Nil(t, participant, "a refused registration cannot report a participant")
	require.Error(t, err, "registration refusal is a failure, not a silent success")
	require.NotNil(t, engine.started, "the participant BEGIN was already opened")
	require.False(t, engine.started.IsActive(), "the opened BEGIN is rolled back when registration fails")
	require.Empty(t, dispatcher.localShardTxExecutors(), "a refused participant is not published")

	// Healthy control: the same engine and shard register on an open owner.
	open := fabric.NewFabricTransaction("nor-312-registration-open")
	openSub, err := open.GetOrOpen("local:one", false)
	require.NoError(t, err)
	healthy := &cypherFabricExecutor{base: NewStorageExecutor(engine), autoCommit: true, transaction: open}
	registered, err := healthy.ensureLocalShardTxExecutor(ctx, openSub, "one", engine)
	require.NoError(t, err)
	require.NotNil(t, registered)
	require.Len(t, healthy.localShardTxExecutors(), 1)
	require.NoError(t, open.Rollback(ctx, nil))
}
