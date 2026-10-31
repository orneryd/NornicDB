package cypher

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newCDCExecutor(t *testing.T, mode storage.ChangeCaptureMode) (*StorageExecutor, storage.Engine) {
	t.Helper()
	store := newTestServerLikeNamespacedStore(t)
	capture, ok := storage.ChangeCaptureOf(store)
	require.True(t, ok)
	capture.SetChangeCapture("test", mode)
	return NewStorageExecutor(store), store
}

func cdcRows(t *testing.T, exec *StorageExecutor, from string, selectors ...map[string]interface{}) []map[string]interface{} {
	t.Helper()
	params := map[string]interface{}{"from": from, "selectors": []interface{}{}}
	for _, selector := range selectors {
		params["selectors"] = append(params["selectors"].([]interface{}), selector)
	}
	result, err := exec.Execute(context.Background(), "CALL db.cdc.query($from, $selectors)", params)
	require.NoError(t, err)
	rows := make([]map[string]interface{}, len(result.Rows))
	for index, row := range result.Rows {
		rows[index] = map[string]interface{}{"id": row[0], "txId": row[1], "seq": row[2], "metadata": row[3], "event": row[4]}
	}
	return rows
}

func cdcID(t *testing.T, exec *StorageExecutor, procedure string) string {
	t.Helper()
	result, err := exec.Execute(context.Background(), "CALL "+procedure+"() YIELD id RETURN id", nil)
	require.NoError(t, err)
	return result.Rows[0][0].(string)
}

func eventOf(row map[string]interface{}) map[string]interface{} {
	return row["event"].(map[string]interface{})
}

func stateOf(row map[string]interface{}, side string) map[string]interface{} {
	state, _ := eventOf(row)["state"].(map[string]interface{})[side].(map[string]interface{})
	return state
}

// Every committed change of a capturing database is an event in Neo4j's
// format, in commit order (#937): creates, updates and deletes of nodes and
// relationships through auto-commit and explicit transactions, the
// relationships a DETACH DELETE removes before their node, several changes
// of one transaction under one txId ordered by seq, nothing for a rolled-back
// transaction, and the cursor resumes after any event.
func TestChangeDataCaptureFull(t *testing.T) {
	exec, _ := newCDCExecutor(t, storage.ChangeCaptureFull)
	ctx := context.Background()
	start := cdcID(t, exec, "db.cdc.current")
	require.Empty(t, cdcRows(t, exec, start))

	_, err := exec.Execute(ctx, "CREATE (:Person {name: 'Ann', age: 30})", nil)
	require.NoError(t, err)
	rows := cdcRows(t, exec, start)
	require.Len(t, rows, 1)
	created := eventOf(rows[0])
	require.Equal(t, "n", created["eventType"])
	require.Equal(t, "c", created["operation"])
	require.Equal(t, []interface{}{"Person"}, created["labels"])
	require.Nil(t, created["state"].(map[string]interface{})["before"])
	require.Equal(t, map[string]interface{}{"name": "Ann", "age": int64(30)}, stateOf(rows[0], "after")["properties"])
	require.Equal(t, "FULL", rows[0]["metadata"].(map[string]interface{})["captureMode"])
	require.IsType(t, time.Time{}, rows[0]["metadata"].(map[string]interface{})["txCommitTime"])
	afterCreate := rows[0]["id"].(string)

	_, err = exec.Execute(ctx, "MATCH (p:Person {name: 'Ann'}) SET p.age = 31, p:Employee", nil)
	require.NoError(t, err)
	rows = cdcRows(t, exec, afterCreate)
	require.Len(t, rows, 1)
	require.Equal(t, "u", eventOf(rows[0])["operation"])
	require.Equal(t, map[string]interface{}{"name": "Ann", "age": int64(30)}, stateOf(rows[0], "before")["properties"])
	require.Equal(t, map[string]interface{}{"name": "Ann", "age": int64(31)}, stateOf(rows[0], "after")["properties"])
	require.ElementsMatch(t, []interface{}{"Person", "Employee"}, stateOf(rows[0], "after")["labels"])

	// One transaction: two changes, one txId, ordered by seq.
	_, err = exec.Execute(ctx, "MATCH (p:Person {name: 'Ann'}) CREATE (p)-[:KNOWS {since: 2020}]->(:Person {name: 'Bob'})", nil)
	require.NoError(t, err)
	afterUpdate := rows[0]["id"].(string)
	rows = cdcRows(t, exec, afterUpdate)
	require.Len(t, rows, 2)
	require.Equal(t, rows[0]["txId"], rows[1]["txId"])
	require.Equal(t, []interface{}{int64(0), int64(1)}, []interface{}{rows[0]["seq"], rows[1]["seq"]})
	relationship := eventOf(rows[1])
	if eventOf(rows[0])["eventType"] == "r" {
		relationship = eventOf(rows[0])
	}
	require.Equal(t, "KNOWS", relationship["type"])
	require.ElementsMatch(t, []interface{}{"Person", "Employee"}, relationship["start"].(map[string]interface{})["labels"])
	require.Equal(t, []interface{}{"Person"}, relationship["end"].(map[string]interface{})["labels"])

	// Resuming after the first event of a transaction returns the rest.
	require.Len(t, cdcRows(t, exec, rows[0]["id"].(string)), 1)

	// A rolled-back transaction leaves nothing.
	beforeRollback := cdcID(t, exec, "db.cdc.current")
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Person {name: 'Ghost'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
	require.Empty(t, cdcRows(t, exec, beforeRollback))

	// DETACH DELETE: the relationship's delete, then the node's, with their
	// full state before.
	_, err = exec.Execute(ctx, "MATCH (p:Person {name: 'Ann'}) DETACH DELETE p", nil)
	require.NoError(t, err)
	rows = cdcRows(t, exec, beforeRollback)
	require.Len(t, rows, 2)
	require.Equal(t, "r", eventOf(rows[0])["eventType"])
	require.Equal(t, "d", eventOf(rows[0])["operation"])
	require.Equal(t, map[string]interface{}{"since": int64(2020)}, stateOf(rows[0], "before")["properties"])
	require.Equal(t, "n", eventOf(rows[1])["eventType"])
	require.Equal(t, "d", eventOf(rows[1])["operation"])
	require.Equal(t, map[string]interface{}{"name": "Ann", "age": int64(31)}, stateOf(rows[1], "before")["properties"])
	require.Nil(t, eventOf(rows[1])["state"].(map[string]interface{})["after"])

	// Everything since the start, in order; with a limit, in parts.
	require.Len(t, cdcRows(t, exec, start), 6)
	first, err := exec.Execute(ctx, "CALL db.cdc.query($from, [], 4) YIELD id RETURN id", map[string]interface{}{"from": start})
	require.NoError(t, err)
	require.Len(t, first.Rows, 4)
	rest := cdcRows(t, exec, first.Rows[3][0].(string))
	require.Len(t, rest, 2)
	_, err = exec.Execute(ctx, "CALL db.cdc.query($from, [], -1)", map[string]interface{}{"from": start})
	require.Error(t, err)
	require.Equal(t, start, cdcID(t, exec, "db.cdc.earliest"))
}

// DIFF records only the changed properties and labels of an update; creates
// and deletes keep their whole state.
func TestChangeDataCaptureDiff(t *testing.T) {
	exec, _ := newCDCExecutor(t, storage.ChangeCaptureDiff)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Doc {id: 1, title: 'a', body: 'x'})", nil)
	require.NoError(t, err)
	afterCreate := cdcID(t, exec, "db.cdc.current")
	_, err = exec.Execute(ctx, "MATCH (d:Doc {id: 1}) SET d.title = 'b' REMOVE d.body SET d:Read", nil)
	require.NoError(t, err)
	rows := cdcRows(t, exec, afterCreate)
	require.Len(t, rows, 1)
	require.Equal(t, map[string]interface{}{"title": "a", "body": "x"}, stateOf(rows[0], "before")["properties"])
	require.Equal(t, map[string]interface{}{"title": "b"}, stateOf(rows[0], "after")["properties"])
	require.Equal(t, []interface{}{"Read"}, stateOf(rows[0], "after")["labels"])
	require.ElementsMatch(t, []interface{}{"Doc", "Read"}, eventOf(rows[0])["labels"])
}

// Events carry the entity's key-constraint values as Neo4j's do: a node's per
// label from its NODE KEY constraints, a relationship's as a list from its
// RELATIONSHIP KEY constraints, and each endpoint's; from the whole state, so
// a DIFF update that doesn't change the key has it too, and a key selector
// finds that update. A node without a key constraint has none.
func TestChangeDataCaptureKeys(t *testing.T) {
	exec, _ := newCDCExecutor(t, storage.ChangeCaptureDiff)
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE CONSTRAINT k_ctx FOR (n:Context) REQUIRE (n.id) IS NODE KEY",
		"CREATE CONSTRAINT k_link FOR ()-[r:LINK]-() REQUIRE (r.k) IS RELATIONSHIP KEY",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	from := cdcID(t, exec, "db.cdc.current")
	for _, statement := range []string{
		"CREATE (:Context {id: 'ctx', name: 'a'}), (:Plain {name: 'p'})",
		"MATCH (c:Context {id: 'ctx'}) SET c.name = 'b'",
		"MATCH (c:Context {id: 'ctx'}), (p:Plain) CREATE (c)-[:LINK {k: 7, note: 'x'}]->(p)",
		"MATCH (c:Context {id: 'ctx'}) DETACH DELETE c",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	rows := cdcRows(t, exec, from)
	require.Len(t, rows, 6)
	contextKeys := map[string]interface{}{"Context": []interface{}{map[string]interface{}{"id": "ctx"}}}
	var operations []string
	for _, row := range rows {
		event := eventOf(row)
		operations = append(operations, event["eventType"].(string)+event["operation"].(string))
		if event["eventType"] == "r" {
			require.Equal(t, []interface{}{map[string]interface{}{"k": int64(7)}}, event["keys"])
			require.Equal(t, contextKeys, event["start"].(map[string]interface{})["keys"])
			require.Equal(t, map[string]interface{}{}, event["end"].(map[string]interface{})["keys"])
			continue
		}
		if labels := event["labels"].([]interface{}); labels[0] == "Plain" {
			require.Equal(t, map[string]interface{}{}, event["keys"])
			continue
		}
		require.Equal(t, contextKeys, event["keys"], event["operation"])
	}
	require.Equal(t, []string{"nc", "nc", "nu", "rc", "rd", "nd"}, operations)

	updates := cdcRows(t, exec, from, map[string]interface{}{"select": "n", "key": map[string]interface{}{"id": "ctx"}, "operation": "u"})
	require.Len(t, updates, 1)
	require.Equal(t, map[string]interface{}{"name": "b"}, stateOf(updates[0], "after")["properties"])
}

// A client's transaction metadata (WithTransactionMetadata, from Bolt's
// tx_metadata) is the transaction's metadata, for an explicit transaction
// from BEGIN and for an auto-commit statement, and a txMetadata selector
// finds it. Metadata too large for a transaction
// fails the BEGIN and leaves no transaction open.
func TestChangeDataCaptureClientTransactionMetadata(t *testing.T) {
	exec, _ := newCDCExecutor(t, storage.ChangeCaptureFull)
	ctx := context.Background()
	from := cdcID(t, exec, "db.cdc.current")
	_, err := exec.Execute(WithTransactionMetadata(ctx, map[string]any{"writer": "explicit"}), "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Doc {id: 1})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	_, err = exec.Execute(WithTransactionMetadata(ctx, map[string]any{"writer": "auto"}), "CREATE (:Doc {id: 2})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(WithTransactionMetadata(ctx, nil), "CREATE (:Doc {id: 3})", nil)
	require.NoError(t, err)

	rows := cdcRows(t, exec, from)
	require.Len(t, rows, 3)
	for index, want := range []map[string]interface{}{{"writer": "explicit"}, {"writer": "auto"}, {}} {
		require.Equal(t, want, rows[index]["metadata"].(map[string]interface{})["txMetadata"], index)
	}
	require.Len(t, cdcRows(t, exec, from, map[string]interface{}{"select": "e", "txMetadata": map[string]interface{}{"writer": "auto"}}), 1)

	large := map[string]any{"blob": strings.Repeat("x", 4096)}
	_, err = exec.Execute(WithTransactionMetadata(ctx, large), "BEGIN", nil)
	require.Error(t, err)
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.Error(t, err, "no transaction is open")
	_, err = exec.Execute(WithTransactionMetadata(ctx, large), "CREATE (:Doc {id: 4})", nil)
	require.Error(t, err)
}

// CALL tx.setMetaData(…) before MATCH … SET gives the transaction its
// metadata, so the change event carries it (the
// void procedure keeping its row is the fix this PR stacks on).
func TestChangeDataCaptureSetMetaDataBeforeAClause(t *testing.T) {
	exec, _ := newCDCExecutor(t, storage.ChangeCaptureFull)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Context {id: 'ctx', name: 'a'})", nil)
	require.NoError(t, err)
	from := cdcID(t, exec, "db.cdc.current")
	// In order: the read after the update sees it.
	for _, step := range []struct {
		query string
		want  [][]interface{}
	}{
		{"CALL tx.setMetaData({writer: 'w'}) MATCH (c:Context {id: 'ctx'}) SET c.name = 'c' RETURN c.name", [][]interface{}{{"c"}}},
		{"CALL db.awaitIndexes() MATCH (c:Context {id: 'ctx'}) RETURN c.name AS name", [][]interface{}{{"c"}}},
		{"CALL db.awaitIndexes() RETURN 1 AS one", [][]interface{}{{int64(1)}}},
	} {
		result, err := exec.Execute(ctx, step.query, nil)
		require.NoError(t, err, step.query)
		require.Equal(t, step.want, result.Rows, step.query)
	}
	rows := cdcRows(t, exec, from)
	require.Len(t, rows, 1)
	require.Equal(t, map[string]interface{}{"writer": "w"}, rows[0]["metadata"].(map[string]interface{})["txMetadata"])
	require.Equal(t, map[string]interface{}{"id": "ctx", "name": "c"}, stateOf(rows[0], "after")["properties"])
}

// Selectors filter as Neo4j's do: OR across the list, AND within one.
func TestChangeDataCaptureSelectors(t *testing.T) {
	exec, _ := newCDCExecutor(t, storage.ChangeCaptureFull)
	ctx := context.Background()
	start := cdcID(t, exec, "db.cdc.current")
	_, err := exec.Execute(ctx, "CREATE (a:A {k: 1})-[:R {w: 1}]->(b:B {k: 2})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (a:A) SET a.k = 5", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (b:B) SET b.other = 1", nil)
	require.NoError(t, err)
	count := func(selectors ...map[string]interface{}) int {
		return len(cdcRows(t, exec, start, selectors...))
	}
	require.Equal(t, 5, count())
	require.Equal(t, 4, count(map[string]interface{}{"select": "n"}))
	require.Equal(t, 1, count(map[string]interface{}{"select": "r", "type": "R"}))
	require.Equal(t, 3, count(map[string]interface{}{"select": "e", "operation": "c"}))
	require.Equal(t, 2, count(map[string]interface{}{"select": "n", "labels": []interface{}{"A"}}))
	require.Equal(t, 2, count(map[string]interface{}{"select": "n", "changesTo": []interface{}{"k"}, "operation": "c"}))
	require.Equal(t, 1, count(map[string]interface{}{"select": "n", "changesTo": []interface{}{"k"}, "operation": "u"}))
	require.Equal(t, 1, count(map[string]interface{}{"select": "n", "key": map[string]interface{}{"k": int64(5)}}))
	require.Equal(t, 1, count(map[string]interface{}{"select": "r", "start": map[string]interface{}{"select": "n", "labels": []interface{}{"A"}}}))
	require.Equal(t, 3, count(map[string]interface{}{"select": "n", "operation": "u"}, map[string]interface{}{"select": "r"}))
	require.Equal(t, 0, count(map[string]interface{}{"select": "e", "executingUser": "alice"}))
	for _, selector := range []map[string]interface{}{
		{"select": "x"}, {"select": "e", "labels": []interface{}{"A"}}, {"select": "n", "operation": "z"},
		{"select": "r", "start": map[string]interface{}{"select": "r"}}, {"select": "n", "changesTo": "k"},
	} {
		_, err := exec.Execute(ctx, "CALL db.cdc.query($from, [$selector])", map[string]interface{}{"from": start, "selector": selector})
		requireStatusCode(t, err, "Neo.ClientError.Statement.ArgumentError")
	}
}

// Identifiers are local to their database and must lie between
// db.cdc.earliest() and db.cdc.current(); a database without capture fails
// with Neo4j's Disabled error.
func TestChangeDataCaptureIdentifiers(t *testing.T) {
	exec, store := newCDCExecutor(t, storage.ChangeCaptureFull)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:P {n: 1})", nil)
	require.NoError(t, err)
	current := cdcID(t, exec, "db.cdc.current")
	require.Empty(t, cdcRows(t, exec, ""))

	for id, code := range map[string]string{
		"not-an-id": "Neo.ClientError.ChangeDataCapture.InvalidIdentifier",
		encodeChangeID(exec.databaseChangeTag("other"), changePosition{commitSeq: 1}): "Neo.ClientError.ChangeDataCapture.InvalidIdentifier",
		encodeChangeID(exec.databaseChangeTag("test"), changePosition{commitSeq: 99}): "Neo.TransientError.ChangeDataCapture.FutureIdentifier",
	} {
		_, err := exec.Execute(ctx, "CALL db.cdc.query($from)", map[string]interface{}{"from": id})
		require.Error(t, err, id)
		requireStatusCode(t, err, code)
	}

	// Retention: events of transactions committed before the cutoff go, but
	// the last transaction's stay, where db.cdc.current() stands; an
	// identifier older than db.cdc.earliest() is invalid.
	capture, _ := storage.ChangeCaptureOf(store)
	start := encodeChangeID(exec.databaseChangeTag("test"), changePosition{commitSeq: 0, seq: ^uint32(0)})
	require.Len(t, cdcRows(t, exec, start), 1)
	_, err = exec.Execute(ctx, "CREATE (:P {n: 2}), (:P {n: 3})", nil)
	require.NoError(t, err)
	latest := cdcID(t, exec, "db.cdc.current")
	type pruner interface {
		PruneChangeEvents(namespace string, cutoff time.Time) (int, error)
	}
	pruned, err := capture.(pruner).PruneChangeEvents("test", time.Now().Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, pruned)
	_, err = exec.Execute(ctx, "CALL db.cdc.query($from)", map[string]interface{}{"from": start})
	requireStatusCode(t, err, "Neo.ClientError.ChangeDataCapture.InvalidIdentifier")
	require.Equal(t, current, cdcID(t, exec, "db.cdc.earliest"))
	require.Len(t, cdcRows(t, exec, current), 2)
	require.Equal(t, latest, cdcID(t, exec, "db.cdc.current"))

	capture.SetChangeCapture("test", storage.ChangeCaptureOff)
	for _, statement := range []string{"CALL db.cdc.current()", "CALL db.cdc.earliest()", "CALL db.cdc.query()"} {
		_, err := exec.Execute(ctx, statement, nil)
		requireStatusCode(t, err, "Neo.DatabaseError.ChangeDataCapture.Disabled")
	}
}

// Writes that bypass a Cypher transaction are captured through one too:
// direct engine writes (MCP, Qdrant, import) and the async CREATE route.
func TestChangeDataCaptureDirectWrites(t *testing.T) {
	exec, store := newCDCExecutor(t, storage.ChangeCaptureFull)
	start := cdcID(t, exec, "db.cdc.current")
	_, err := store.CreateNode(&storage.Node{ID: "direct", Labels: []string{"D"}, Properties: map[string]interface{}{"v": int64(1)}})
	require.NoError(t, err)
	require.NoError(t, store.UpdateNode(storage.NodeUpdateRequest{After: &storage.Node{ID: "direct", Labels: []string{"D"}, Properties: map[string]interface{}{"v": int64(2)}}}))
	require.NoError(t, store.DeleteNode("direct"))
	rows := cdcRows(t, exec, start)
	require.Len(t, rows, 3)
	require.Equal(t, []interface{}{"c", "u", "d"}, []interface{}{eventOf(rows[0])["operation"], eventOf(rows[1])["operation"], eventOf(rows[2])["operation"]})
	require.NotEqual(t, rows[0]["txId"], rows[1]["txId"])

	// Three single CREATE statements: three transactions.
	before := cdcID(t, exec, "db.cdc.current")
	for range 3 {
		_, err := exec.Execute(context.Background(), "CREATE (:Batch {x: 1})", nil)
		require.NoError(t, err)
	}
	rows = cdcRows(t, exec, before)
	require.Len(t, rows, 3)
	require.NotEqual(t, rows[0]["txId"], rows[2]["txId"])
}

// Selector fields that narrow by element ID, type, endpoint, transaction
// metadata, user and key (a deleted entity's key is its state before), the
// selector errors, the limit's errors, and a backlog longer than a page.
func TestChangeDataCaptureSelectorFields(t *testing.T) {
	exec, store := newCDCExecutor(t, storage.ChangeCaptureFull)
	ctx := context.Background()
	start := cdcID(t, exec, "db.cdc.current")
	_, err := exec.Execute(ctx, "CREATE (a:A {k: 1})-[:R {w: 1}]->(b:B {k: 2})", nil)
	require.NoError(t, err)
	capture, _ := storage.ChangeCaptureOf(store)
	engine := capture.(*storage.BadgerEngine)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetMetadata(map[string]interface{}{"app": "pd", "n": int64(1)}))
	_, err = tx.CreateNode(&storage.Node{ID: "test:m", Labels: []string{"M"}, Properties: map[string]any{"k": int64(3)}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	_, err = exec.Execute(ctx, "MATCH (b:B) DETACH DELETE b", nil)
	require.NoError(t, err)

	rows := cdcRows(t, exec, start)
	require.Len(t, rows, 6) // a, b, r, m; r and b deleted
	nodeA := eventOf(rows[0])["elementId"].(string)
	count := func(selectors ...map[string]interface{}) int {
		return len(cdcRows(t, exec, start, selectors...))
	}
	require.Equal(t, 1, count(map[string]interface{}{"select": "n", "elementId": nodeA}))
	require.Equal(t, 0, count(map[string]interface{}{"select": "r", "type": "OTHER"}))
	require.Equal(t, 2, count(map[string]interface{}{"select": "r", "end": map[string]interface{}{"select": "n", "labels": []interface{}{"B"}}}))
	require.Equal(t, 0, count(map[string]interface{}{"select": "r", "end": map[string]interface{}{"select": "n", "labels": []interface{}{"A"}}}))
	require.Equal(t, 2, count(map[string]interface{}{"select": "r", "start": map[string]interface{}{"select": "n", "elementId": nodeA}}))
	require.Equal(t, 0, count(map[string]interface{}{"select": "r", "start": map[string]interface{}{"select": "n", "elementId": "none"}}))
	require.Equal(t, 1, count(map[string]interface{}{"select": "e", "txMetadata": map[string]interface{}{"app": "pd", "n": int64(1)}}))
	require.Equal(t, 0, count(map[string]interface{}{"select": "e", "txMetadata": map[string]interface{}{"app": "other"}}))
	require.Equal(t, 0, count(map[string]interface{}{"select": "e", "authenticatedUser": "alice"}))
	require.Equal(t, 2, count(map[string]interface{}{"select": "n", "key": map[string]interface{}{"k": int64(2)}}), "the create and the delete")

	for _, selectors := range []interface{}{
		"not a list", []interface{}{"not a map"}, []interface{}{map[string]interface{}{"select": "n", "labels": []interface{}{1}}},
		[]interface{}{map[string]interface{}{"select": "r", "end": map[string]interface{}{"select": "r"}}},
		[]interface{}{map[string]interface{}{"select": "e", "comment": "not a field"}},
	} {
		_, err := exec.Execute(ctx, "CALL db.cdc.query($from, $selectors)", map[string]interface{}{"from": start, "selectors": selectors})
		requireStatusCode(t, err, "Neo.ClientError.Statement.ArgumentError")
	}
	_, err = exec.Execute(ctx, "CALL db.cdc.query($from, [], 'x')", map[string]interface{}{"from": start})
	require.Error(t, err)

	_, err = exec.Execute(ctx, "UNWIND range(1, 1001) AS i CREATE (:P {i: i})", nil)
	require.NoError(t, err)
	require.Len(t, cdcRows(t, exec, start), 1007)
}

// cdcValue reads a stored number of any width as INTEGER or FLOAT, in lists
// and maps too.
func TestCDCValue(t *testing.T) {
	for _, value := range []interface{}{int8(1), int16(1), int32(1), int(1), uint8(1), uint16(1), uint32(1), uint64(1), uint(1)} {
		require.Equal(t, int64(1), cdcValue(value))
	}
	require.Equal(t, uint64(1<<63), cdcValue(uint64(1<<63)))
	require.Equal(t, float64(0.5), cdcValue(float32(0.5)))
	require.Equal(t, []interface{}{int64(1)}, cdcValue([]interface{}{int8(1)}))
	require.Equal(t, map[string]interface{}{"k": int64(1)}, cdcValue(map[string]interface{}{"k": uint8(1)}))
	require.Equal(t, "s", cdcValue("s"))
}
