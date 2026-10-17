package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// apocTriggerHarness is a storage engine with a user database "nornic" and
// the system database, their executors, and the engine's trigger runtime.
type apocTriggerHarness struct {
	t       *testing.T
	base    *storage.BadgerEngine
	engine  storage.Engine // the user database's storage
	system  *StorageExecutor
	db      *StorageExecutor
	runtime *TriggerRuntime
}

func newApocTriggerHarness(t *testing.T, enabled bool) *apocTriggerHarness {
	t.Helper()
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	return newApocTriggerHarnessOn(t, base, enabled)
}

func newApocTriggerHarnessOn(t *testing.T, base *storage.BadgerEngine, enabled bool) *apocTriggerHarness {
	t.Helper()
	h := &apocTriggerHarness{t: t, base: base}
	h.engine = storage.NewNamespacedEngine(base, "nornic")
	h.db = NewStorageExecutor(h.engine)
	h.system = NewStorageExecutor(storage.NewNamespacedEngine(base, "system"))
	h.startRuntime(enabled)
	t.Cleanup(func() {
		h.runtime.Close()
		_ = base.Close()
	})
	return h
}

// startRuntime starts the engine's trigger runtime, as a server start does.
func (h *apocTriggerHarness) startRuntime(enabled bool) {
	h.t.Helper()
	runtime, err := NewTriggerRuntime(context.Background(), TriggerRuntimeConfig{
		Engine: h.base,
		System: storage.NewNamespacedEngine(h.base, "system"),
		ExecutorFor: func(database string) (*StorageExecutor, error) {
			if database == "nornic" {
				return h.db, nil
			}
			return nil, fmt.Errorf("database %s not found", database)
		},
		DatabaseExists: func(database string) bool { return database == "nornic" || database == "system" },
		Enabled:        enabled,
	})
	require.NoError(h.t, err)
	h.runtime = runtime
}

// install installs a trigger on nornic from the system database.
func (h *apocTriggerHarness) install(name, statement string, selector, config map[string]any) *ExecuteResult {
	h.t.Helper()
	result, err := h.system.Execute(context.Background(),
		"CALL apoc.trigger.install('nornic', $name, $statement, $selector, $config)",
		map[string]any{"name": name, "statement": statement, "selector": selector, "config": config})
	require.NoError(h.t, err)
	return result
}

func (h *apocTriggerHarness) run(query string, params map[string]any) *ExecuteResult {
	h.t.Helper()
	result, err := h.db.Execute(context.Background(), query, params)
	require.NoError(h.t, err, query)
	return result
}

func (h *apocTriggerHarness) count(query string) int64 {
	h.t.Helper()
	result := h.run(query, nil)
	require.Len(h.t, result.Rows, 1, query)
	return toInt64(result.Rows[0][0])
}

// apocStampTrigger stamps every :Doc node the transaction created, gave the
// label Doc to, or assigned a name to: the consumer's shape.
const apocStampTrigger = `WITH $createdNodes + coalesce($assignedLabels['Doc'], []) + [e IN coalesce($assignedNodeProperties['name'], []) | e.node] AS touched
UNWIND touched AS n
WITH DISTINCT n
WHERE n:Doc
SET n.stamp = $trigger`

// A before trigger fires on every Cypher write route, in the writing
// transaction: auto-commit (CREATE, MERGE … SET n = map, SET n += map, a
// dynamic label that adds the watched label, a 500-node UNWIND MERGE batch),
// an explicit transaction, apoc.cypher.doIt (a write procedure called with
// no write keyword in the statement), apoc.periodic.iterate, CALL { } IN
// TRANSACTIONS, and a direct engine write.
func TestApocTriggerBeforeFiresOnEveryWriteRoute(t *testing.T) {
	routes := []struct {
		name    string
		setup   string
		write   func(h *apocTriggerHarness)
		stamped int64
	}{
		{name: "auto-commit CREATE", write: func(h *apocTriggerHarness) {
			h.run("CREATE (:Doc {name: 'a'})", nil)
		}, stamped: 1},
		{name: "MERGE … SET n = map", write: func(h *apocTriggerHarness) {
			h.run("MERGE (n:Doc {id: 1}) SET n = $props", map[string]any{"props": map[string]any{"id": int64(1), "name": "b"}})
		}, stamped: 1},
		{name: "SET n += map on an existing node", setup: "CREATE (:Doc {id: 2, name: 'old'})", write: func(h *apocTriggerHarness) {
			h.run("MATCH (n:Doc {id: 2}) SET n += $props", map[string]any{"props": map[string]any{"name": "c"}})
		}, stamped: 1},
		{name: "dynamic label adds the watched label", setup: "CREATE (:Other {id: 3, name: 'd'})", write: func(h *apocTriggerHarness) {
			h.run("MATCH (n:Other {id: 3}) SET n:$($labels)", map[string]any{"labels": []any{"Doc"}})
		}, stamped: 1},
		{name: "UNWIND batch of 500 MERGE", write: func(h *apocTriggerHarness) {
			h.run("UNWIND range(1, 500) AS i MERGE (n:Doc {id: i}) SET n.name = 'n' + toString(i)", nil)
		}, stamped: 500},
		{name: "explicit transaction", write: func(h *apocTriggerHarness) {
			session := NewStorageExecutor(h.engine)
			ctx := context.Background()
			_, err := session.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			_, err = session.Execute(ctx, "CREATE (:Doc {name: 'e'})", nil)
			require.NoError(t, err)
			_, err = session.Execute(ctx, "COMMIT", nil)
			require.NoError(t, err)
		}, stamped: 1},
		{name: "write procedure with no write keyword", write: func(h *apocTriggerHarness) {
			h.run("CALL apoc.cypher.doIt($q, {}) YIELD value RETURN value", map[string]any{"q": "CREATE (:Doc {name: 'f'})"})
		}, stamped: 1},
		{name: "apoc.periodic.iterate", write: func(h *apocTriggerHarness) {
			h.run("CALL apoc.periodic.iterate('UNWIND range(1, 3) AS i RETURN i', 'CREATE (:Doc {name: toString(i)})', {}) YIELD batches RETURN batches", nil)
		}, stamped: 3},
		{name: "CALL IN TRANSACTIONS", write: func(h *apocTriggerHarness) {
			h.run("UNWIND range(1, 4) AS i CALL { WITH i CREATE (:Doc {name: toString(i)}) } IN TRANSACTIONS OF 2 ROWS", nil)
		}, stamped: 4},
		{name: "direct engine write", write: func(h *apocTriggerHarness) {
			_, err := h.engine.CreateNode(&storage.Node{ID: "direct", Labels: []string{"Doc"}, Properties: map[string]any{"name": "g"}})
			require.NoError(t, err)
		}, stamped: 1},
	}
	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			h := newApocTriggerHarness(t, true)
			if route.setup != "" {
				h.run(route.setup, nil)
			}
			h.install("stamp", apocStampTrigger, nil, nil)
			route.write(h)
			require.Equal(t, route.stamped, h.count("MATCH (n:Doc) WHERE n.stamp = 'stamp' RETURN count(n)"))
			require.Zero(t, h.count("MATCH (n:Doc) WHERE n.stamp IS NULL RETURN count(n)"))
		})
	}
}

// With write-behind buffering on, an auto-commit write is acknowledged
// before it reaches Badger; its before trigger still ran in its commit.
func TestApocTriggerBeforeFiresUnderWriteBehind(t *testing.T) {
	base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{InMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	h := newApocTriggerHarnessOn(t, base, true)
	h.install("stamp", apocStampTrigger, nil, nil)
	h.run("CREATE (:Doc {name: 'a'})", nil)
	require.Equal(t, int64(1), h.count("MATCH (n:Doc) WHERE n.stamp = 'stamp' RETURN count(n)"))
}

// The consumer's trigger: watched-label nodes whose created or assigned
// properties include given keys, or that gained the label, get a derived
// key from their final values, in the same transaction. Its own key write
// doesn't fire it again: it ran once per commit, and change capture shows
// the write once, in the same transaction's events.
func TestApocTriggerDerivedKeyRunsOncePerCommitAndShowsInChangeEvents(t *testing.T) {
	h := newApocTriggerHarness(t, true)
	h.base.SetChangeCapture("nornic", storage.ChangeCaptureFull)
	h.run("CREATE (:Counter {runs: 0})", nil)
	statement := `MATCH (c:Counter) SET c.runs = c.runs + 1
WITH count(*) AS ignored
UNWIND $keys AS k
UNWIND coalesce($assignedNodeProperties[k], []) AS e
WITH collect(e.node) AS touched
UNWIND touched + coalesce($assignedLabels[$label], []) AS n
WITH DISTINCT n
WHERE $label IN labels(n)
SET n.name_key = toLower(n.name)`
	h.install("name_key", statement, map[string]any{"phase": "before"},
		map[string]any{"params": map[string]any{"keys": []any{"name"}, "label": "Doc"}})

	// The statement's later SET is the final value the trigger reads.
	h.run("CREATE (n:Doc {name: 'First'}) SET n.name = 'Final'", nil)
	result := h.run("MATCH (n:Doc) RETURN n.name_key", nil)
	require.Equal(t, [][]interface{}{{"final"}}, result.Rows)
	require.Equal(t, int64(1), h.count("MATCH (c:Counter) RETURN c.runs"))

	events, err := h.base.ChangeEvents("nornic", 0, 0, 0)
	require.NoError(t, err)
	var docEvents []storage.ChangeEvent
	for _, event := range events {
		if event.Node && len(event.Labels) > 0 && event.Labels[0] == "Doc" {
			docEvents = append(docEvents, event)
		}
	}
	require.Len(t, docEvents, 1, "the trigger's write is part of the node's one create event")
	require.Equal(t, "final", docEvents[0].After.Properties["name_key"])
	last := events[len(events)-1].CommitSeq
	require.Equal(t, last, docEvents[0].CommitSeq, "same transaction as the counter's update")

	// A node gaining the watched label gets its key; an unrelated property
	// change does not run the key write.
	h.run("CREATE (:Other {name: 'Later'})", nil)
	h.run("MATCH (n:Other) SET n:Doc", nil)
	require.Equal(t, int64(1), h.count("MATCH (n:Doc {name_key: 'later'}) RETURN count(n)"))
	h.run("MATCH (n:Doc {name: 'Final'}) SET n.other = 1, n.name_key = 'kept'", nil)
	require.Equal(t, int64(1), h.count("MATCH (n:Doc {name_key: 'kept'}) RETURN count(n)"))
}

// apocTriggerLog installs a before trigger that records what it was given
// on a :Log node, and returns a reader of the last log.
func apocTriggerLog(h *apocTriggerHarness, name, statement string) func() map[string]any {
	h.install(name, statement, nil, nil)
	return func() map[string]any {
		h.t.Helper()
		result := h.run("MATCH (l:Log) RETURN properties(l) ORDER BY l.at DESC LIMIT 1", nil)
		require.Len(h.t, result.Rows, 1)
		return result.Rows[0][0].(map[string]any)
	}
}

// The transaction-state parameters have APOC's shapes and the kernel's
// semantics.
func TestApocTriggerTransactionStateParameters(t *testing.T) {
	h := newApocTriggerHarness(t, true)
	h.run("CREATE (:Seed {k: 'x', gone: 1})-[:R {w: 1}]->(:Seed {k: 'y'})", nil)
	// The trigger's own :Log node is not in its pass's parameters (they were
	// taken before it ran), nor in a later transaction's.
	last := apocTriggerLog(h, "log", `OPTIONAL MATCH (prior:Log)
WITH count(prior) AS seq
CREATE (:Log {
  at: seq,
  created: size($createdNodes),
  deleted: size($deletedNodes),
  createdRels: size($createdRelationships),
  deletedRels: size($deletedRelationships),
  assignedLabels: keys($assignedLabels),
  removedLabels: keys($removedLabels),
  assignedKeys: keys($assignedNodeProperties),
  removedKeys: keys($removedNodeProperties),
  oldKNull: size([e IN coalesce($assignedNodeProperties['k'], []) WHERE e.old IS NULL]),
  oldK: [e IN coalesce($assignedNodeProperties['k'], []) WHERE e.old IS NOT NULL | e.old],
  newK: [e IN coalesce($assignedNodeProperties['k'], []) | e.new],
  oldGone: [e IN coalesce($removedNodeProperties['gone'], []) | e.old],
  relKeys: keys($assignedRelationshipProperties),
  relRemoved: keys($removedRelationshipProperties),
  txId: $transactionId,
  commitTime: $commitTime,
  trigger: $trigger,
  app: coalesce($metaData.app, '')
})`)

	// Update: changed and removed keys, old and new values; -1 before the
	// commit; the transaction's metadata.
	ctx := WithTransactionMetadata(context.Background(), map[string]any{"app": "importer"})
	_, err := h.db.Execute(ctx, "MATCH (n:Seed {k: 'x'}) SET n.k = 'z' REMOVE n.gone SET n:Extra", nil)
	require.NoError(t, err)
	log := last()
	require.Equal(t, int64(0), toInt64(log["created"]))
	require.Equal(t, []any{"Extra"}, log["assignedLabels"])
	require.Equal(t, []any{"k"}, log["assignedKeys"])
	require.Equal(t, []any{"gone"}, log["removedKeys"])
	require.Equal(t, []any{"x"}, log["oldK"])
	require.Equal(t, []any{"z"}, log["newK"])
	require.Equal(t, []any{int64(1)}, log["oldGone"])
	require.Equal(t, int64(-1), toInt64(log["txId"]))
	require.Equal(t, int64(-1), toInt64(log["commitTime"]))
	require.Equal(t, "log", log["trigger"])
	require.Equal(t, "importer", log["app"])

	// Setting a property to the value it has, or a label the node has, is
	// no assignment (the marker makes sure the transaction changed
	// something, so the trigger runs).
	h.run("MATCH (n:Seed {k: 'z'}) SET n.k = 'z', n:Seed CREATE (:Marker)", nil)
	log = last()
	require.Equal(t, []any{"Marker"}, log["assignedLabels"])
	require.Empty(t, log["assignedKeys"])

	// Created: every property assigned with old null, every label assigned.
	h.run("CREATE (:Fresh {k: 'new'})", nil)
	log = last()
	require.Equal(t, int64(1), toInt64(log["created"]))
	require.Equal(t, []any{"Fresh"}, log["assignedLabels"])
	require.Equal(t, int64(1), toInt64(log["oldKNull"]))
	require.Empty(t, log["oldK"])
	require.Equal(t, []any{"new"}, log["newK"])

	// Created and deleted in one transaction: in nothing.
	h.run("CREATE (n:Ephemeral {k: 'e'}) DELETE n", nil)
	log = last()
	require.Equal(t, int64(0), toInt64(log["created"]))
	require.Equal(t, int64(0), toInt64(log["deleted"]))

	// DETACH DELETE: the node, its relationship, its labels and properties
	// removed.
	h.run("MATCH (n:Seed {k: 'z'}) DETACH DELETE n", nil)
	log = last()
	require.Equal(t, int64(1), toInt64(log["deleted"]))
	require.Equal(t, int64(1), toInt64(log["deletedRels"]))
	require.ElementsMatch(t, []any{"Seed", "Extra"}, log["removedLabels"])
	require.Contains(t, log["removedKeys"], "k")
	require.Contains(t, log["relRemoved"], "w")
}

// config.params are merged over the transaction's parameters, and $trigger
// is the trigger's name; every trigger of a pass gets the same parameters,
// taken before any of them ran, and runs in name order.
func TestApocTriggerParamsOverrideAndSharedSnapshot(t *testing.T) {
	h := newApocTriggerHarness(t, true)
	h.install("a_first", "UNWIND $createdNodes AS n CREATE (:Made {by: $trigger, label: $label})", nil,
		map[string]any{"params": map[string]any{"label": "custom"}})
	h.install("b_second", "CREATE (:Seen {created: size($createdNodes)})", nil, nil)
	h.install("c_override", "CREATE (:Overridden {created: size($createdNodes)})", nil,
		map[string]any{"params": map[string]any{"createdNodes": []any{}}})
	h.run("CREATE (:Doc)", nil)
	result := h.run("MATCH (m:Made) RETURN m.by, m.label", nil)
	require.Equal(t, [][]interface{}{{"a_first", "custom"}}, result.Rows)
	// b_second ran after a_first, and its $createdNodes is still the
	// pass's: the Doc, not a_first's :Made.
	require.Equal(t, int64(1), h.count("MATCH (s:Seen) RETURN s.created"))
	// c_override's config params replaced $createdNodes.
	require.Equal(t, int64(0), h.count("MATCH (o:Overridden) RETURN o.created"))
}

// A failing before trigger fails the commit with TransactionHookFailed:
// neither the write nor any trigger's write is stored. Every trigger ran,
// and the error names each one that failed.
func TestApocTriggerFailureRollsTheTransactionBack(t *testing.T) {
	h := newApocTriggerHarness(t, true)
	h.install("a_breaks", "UNWIND $createdNodes AS n SET n.x = 1 / 0", nil, nil)
	h.install("b_writes", "CREATE (:SideEffect)", nil, nil)
	h.install("c_breaks", "WITH 1 AS one RETURN one / 0", nil, nil)
	_, err := h.db.Execute(context.Background(), "CREATE (:Doc {name: 'x'})", nil)
	require.Error(t, err)
	requireStatusCode(t, err, "Neo.ClientError.Transaction.TransactionHookFailed")
	require.Contains(t, err.Error(), "a_breaks=")
	require.Contains(t, err.Error(), "c_breaks=")
	require.NotContains(t, err.Error(), "b_writes=")
	require.Zero(t, h.count("MATCH (n) WHERE n:Doc OR n:SideEffect RETURN count(n)"))

	// Explicit transaction: the COMMIT fails and nothing is stored.
	session := NewStorageExecutor(h.engine)
	ctx := context.Background()
	_, err = session.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = session.Execute(ctx, "CREATE (:Doc {name: 'y'})", nil)
	require.NoError(t, err)
	_, err = session.Execute(ctx, "COMMIT", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "a_breaks=")
	require.Zero(t, h.count("MATCH (n:Doc) RETURN count(n)"))
}

// after triggers run after the commit, in a new transaction tagged
// apoc.trigger, with the commit's transaction ID and time; that
// transaction's commit runs before triggers once and no after trigger, so
// after triggers don't cascade. afterAsync runs on a worker. rollback runs
// after a rollback.
func TestApocTriggerAfterAfterAsyncAndRollbackPhases(t *testing.T) {
	h := newApocTriggerHarness(t, true)
	h.install("stamp", apocStampTrigger, nil, nil)
	h.install("after", "UNWIND $createdNodes AS n WITH n WHERE n:Doc CREATE (:Doc:After {name: 'after', tx: $transactionId, at: $commitTime})",
		map[string]any{"phase": "after"}, nil)
	h.install("async", "UNWIND $createdNodes AS n WITH n WHERE n:Doc AND NOT n:After CREATE (:Async {tx: $transactionId})",
		map[string]any{"phase": "afterAsync"}, nil)
	h.install("rolled", "CREATE (:RolledBack {created: size($createdNodes), tx: $transactionId})",
		map[string]any{"phase": "rollback"}, nil)

	h.run("CREATE (:Doc {name: 'x'})", nil)
	result := h.run("MATCH (a:After) RETURN a.tx, a.at, a.stamp", nil)
	require.Len(t, result.Rows, 1, "the after trigger's own Doc doesn't fire it again")
	require.Greater(t, toInt64(result.Rows[0][0]), int64(0))
	require.Greater(t, toInt64(result.Rows[0][1]), int64(0))
	require.Equal(t, "stamp", result.Rows[0][2], "before triggers run on the after trigger's transaction")

	deadline := time.Now().Add(10 * time.Second)
	for h.count("MATCH (a:Async) RETURN count(a)") == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	require.Equal(t, int64(1), h.count("MATCH (a:Async) RETURN count(a)"))

	session := NewStorageExecutor(h.engine)
	ctx := context.Background()
	_, err := session.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = session.Execute(ctx, "CREATE (:Doc {name: 'gone'})", nil)
	require.NoError(t, err)
	_, err = session.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
	result = h.run("MATCH (r:RolledBack) RETURN r.created, r.tx", nil)
	require.Equal(t, [][]interface{}{{int64(1), int64(-1)}}, result.Rows)
	require.Zero(t, h.count("MATCH (n:Doc {name: 'gone'}) RETURN count(n)"))
}

// install replaces by name and is idempotent; stop and start pause and
// resume; a re-install resumes a stopped trigger (APOC sets paused false);
// show and list report them; drop and dropAll remove them; triggers are
// kept in the system database, so a new runtime over the same engine (a
// restart) loads them, paused state included.
func TestApocTriggerManagementAndPersistence(t *testing.T) {
	h := newApocTriggerHarness(t, true)
	installed := h.install("stamp", apocStampTrigger, nil, map[string]any{"params": map[string]any{"n": int64(5)}})
	require.Equal(t, apocTriggerInfoColumns, installed.Columns)
	require.Equal(t, [][]interface{}{{"stamp", apocStampTrigger, nil, map[string]interface{}{"n": int64(5)}, true, false}}, installed.Rows)
	h.install("stamp", apocStampTrigger, nil, map[string]any{"params": map[string]any{"n": int64(5)}})
	shown, err := h.system.Execute(context.Background(), "CALL apoc.trigger.show('nornic')", nil)
	require.NoError(t, err)
	require.Len(t, shown.Rows, 1, "re-installing the same trigger keeps one")

	_, err = h.system.Execute(context.Background(), "CALL apoc.trigger.stop('nornic', 'stamp')", nil)
	require.NoError(t, err)
	require.Nil(t, h.base.CommitHook("nornic"), "a database whose triggers are all stopped has no hook")
	h.run("CREATE (:Doc {name: 'paused'})", nil)
	require.Equal(t, int64(1), h.count("MATCH (n:Doc) WHERE n.stamp IS NULL RETURN count(n)"))

	// Restart: the stopped trigger stays stopped.
	h.runtime.Close()
	h.startRuntime(true)
	h.run("CREATE (:Doc {name: 'still paused'})", nil)
	require.Equal(t, int64(2), h.count("MATCH (n:Doc) WHERE n.stamp IS NULL RETURN count(n)"))

	// A re-install resumes it, as APOC's install does.
	h.install("stamp", apocStampTrigger, nil, map[string]any{"params": map[string]any{"n": int64(5)}})
	h.run("CREATE (:Doc {name: 'resumed'})", nil)
	require.Equal(t, int64(1), h.count("MATCH (n:Doc {stamp: 'stamp'}) RETURN count(n)"))

	// Restart again: it fires, and its params keep their integer type.
	h.runtime.Close()
	h.startRuntime(true)
	listed := h.run("CALL apoc.trigger.list() YIELD name, params, paused RETURN name, params, paused", nil)
	require.Equal(t, [][]interface{}{{"stamp", map[string]interface{}{"n": int64(5)}, false}}, listed.Rows)
	h.run("CREATE (:Doc {name: 'after restart'})", nil)
	require.Equal(t, int64(2), h.count("MATCH (n:Doc {stamp: 'stamp'}) RETURN count(n)"))

	// Replace by name with another statement.
	h.install("stamp", "UNWIND $createdNodes AS n SET n.stamp = 'v2'", nil, nil)
	h.run("CREATE (:Doc {name: 'v2'})", nil)
	require.Equal(t, int64(1), h.count("MATCH (n:Doc {stamp: 'v2'}) RETURN count(n)"))

	dropped, err := h.system.Execute(context.Background(), "CALL apoc.trigger.drop('nornic', 'stamp')", nil)
	require.NoError(t, err)
	require.Len(t, dropped.Rows, 1)
	require.Equal(t, false, dropped.Rows[0][4])
	require.Nil(t, h.base.CommitHook("nornic"))
	none, err := h.system.Execute(context.Background(), "CALL apoc.trigger.drop('nornic', 'stamp')", nil)
	require.NoError(t, err)
	require.Empty(t, none.Rows)

	h.install("x", "RETURN 1", nil, nil)
	h.install("y", "RETURN 1", nil, nil)
	all, err := h.system.Execute(context.Background(), "CALL apoc.trigger.dropAll('nornic') YIELD name RETURN name", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"x"}, {"y"}}, all.Rows)
	h.runtime.Close()
	h.startRuntime(true)
	require.Empty(t, h.runtime.list("nornic"))
}

// The deprecated session-database forms: add returns the replaced trigger
// then the new one, remove returns the removed one (or its name alone),
// pause and resume fail for a trigger that isn't installed.
func TestApocTriggerDeprecatedSessionProcedures(t *testing.T) {
	h := newApocTriggerHarness(t, true)
	added := h.run("CALL apoc.trigger.add('t', 'RETURN 1', {})", nil)
	require.Len(t, added.Rows, 1)
	replaced := h.run("CALL apoc.trigger.add('t', 'RETURN 2', {phase: 'after'})", nil)
	require.Len(t, replaced.Rows, 2)
	require.Equal(t, []interface{}{"t", "RETURN 1", map[string]interface{}{}, map[string]interface{}{}, false, false}, replaced.Rows[0])
	require.Equal(t, true, replaced.Rows[1][4])
	paused := h.run("CALL apoc.trigger.pause('t')", nil)
	require.Equal(t, true, paused.Rows[0][5])
	_, err := h.db.Execute(context.Background(), "CALL apoc.trigger.pause('missing')", nil)
	requireStatusCode(t, err, "Neo.ClientError.Procedure.ProcedureCallFailed")
	removed := h.run("CALL apoc.trigger.remove('t')", nil)
	require.Equal(t, false, removed.Rows[0][4])
	missing := h.run("CALL apoc.trigger.remove('t')", nil)
	require.Equal(t, [][]interface{}{{"t", nil, nil, nil, false, false}}, missing.Rows)
	require.Empty(t, h.run("CALL apoc.trigger.removeAll()", nil).Rows)
}

// The procedures' refusals: disabled triggers, a system procedure on a user
// database, the system database or an unknown one as the target, a
// statement that doesn't parse, a phase that doesn't exist; and the admin
// entitlement every procedure needs.
func TestApocTriggerProcedureRefusals(t *testing.T) {
	disabled := newApocTriggerHarness(t, false)
	_, err := disabled.system.Execute(context.Background(), "CALL apoc.trigger.install('nornic', 't', 'RETURN 1', {})", nil)
	requireStatusCode(t, err, "Neo.ClientError.Procedure.ProcedureCallFailed")
	require.Contains(t, err.Error(), "Triggers have not been enabled")
	_, err = disabled.db.Execute(context.Background(), "CALL apoc.trigger.list()", nil)
	require.Error(t, err)

	h := newApocTriggerHarness(t, true)
	for _, refused := range []struct{ database, query, contains string }{
		{"nornic", "CALL apoc.trigger.install('nornic', 't', 'RETURN 1', {})", "system database"},
		{"system", "CALL apoc.trigger.install('system', 't', 'RETURN 1', {})", "user databases"},
		{"system", "CALL apoc.trigger.install('nope', 't', 'RETURN 1', {})", "'nope' does not exist"},
		{"system", "CALL apoc.trigger.install('nornic', 't', 'MATCH (n RETURN n', {})", "invalid statement"},
		{"system", "CALL apoc.trigger.install('nornic', 't', 'RETURN 1', {phase: 'BEFORE'})", "invalid trigger phase"},
	} {
		exec := h.system
		if refused.database == "nornic" {
			exec = h.db
		}
		_, err := exec.Execute(context.Background(), refused.query, nil)
		require.Error(t, err, refused.query)
		require.Contains(t, err.Error(), refused.contains, refused.query)
	}
	require.Empty(t, h.runtime.list("nornic"))

	notAdmin := WithPermissionChecker(context.Background(), func(permission string) bool { return permission != "admin" })
	for _, query := range []string{
		"CALL apoc.trigger.install('nornic', 't', 'RETURN 1', {})",
		"CALL apoc.trigger.show('nornic')",
	} {
		_, err := h.system.Execute(notAdmin, query, nil)
		var denied *PermissionDeniedError
		require.ErrorAs(t, err, &denied, query)
	}
	_, err = h.db.Execute(notAdmin, "CALL apoc.trigger.list()", nil)
	var denied *PermissionDeniedError
	require.ErrorAs(t, err, &denied)
}

// A before trigger runs with the writer's rights, as APOC runs it in the
// writer's transaction: a trigger statement that needs a right the writer
// lacks fails, and the writer's write is rolled back.
func TestApocTriggerBeforeRunsWithTheWritersRights(t *testing.T) {
	h := newApocTriggerHarness(t, true)
	h.install("needs_admin", "CALL apoc.trigger.list() YIELD name RETURN count(name)", nil, nil)
	writer := WithPermissionChecker(context.Background(), func(permission string) bool { return permission == "read" || permission == "write" })
	_, err := h.db.Execute(writer, "CREATE (:Doc)", nil)
	requireStatusCode(t, err, "Neo.ClientError.Transaction.TransactionHookFailed")
	require.Zero(t, h.count("MATCH (n:Doc) RETURN count(n)"))
	// An administrator's write passes.
	h.run("CREATE (:Doc)", nil)
	require.Equal(t, int64(1), h.count("MATCH (n:Doc) RETURN count(n)"))
}

// Dropping a database deletes its triggers, so a database created later
// with the same name doesn't inherit them; with no trigger the database has
// no commit hook and its direct writes stay direct.
func TestApocTriggerDropDatabaseAndNoHookWithoutTriggers(t *testing.T) {
	h := newApocTriggerHarness(t, true)
	require.Nil(t, h.base.CommitHook("nornic"))
	h.install("stamp", apocStampTrigger, nil, nil)
	require.NotNil(t, h.base.CommitHook("nornic"))
	require.NoError(t, h.runtime.DropDatabase("nornic"))
	require.Nil(t, h.base.CommitHook("nornic"))
	h.runtime.Close()
	h.startRuntime(true)
	require.Empty(t, h.runtime.list("nornic"))
}
