package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A change feed reduced to its net effect in one statement: db.cdc.query
// pages, each changed element's current id read back by elementId (a
// relationship's between the ends its event names, with those ends), the
// properties each change set, changed or removed (keys of after and before;
// a DIFF update states only those), and per kind the distinct ids of the
// elements whose change is of that kind, plus every changed element and
// link end but the reader's own cursor. No procedure is needed for it: the
// tests pin the statement's results and the benchmark its cost.

// cdcReductionSelectors are the changes the reduction reads.
var cdcReductionSelectors = []interface{}{
	map[string]interface{}{"select": "n", "labels": []interface{}{"Record"}},
	map[string]interface{}{"select": "n", "labels": []interface{}{"Topic"}},
	map[string]interface{}{"select": "n", "labels": []interface{}{"Person"}},
	map[string]interface{}{"select": "n", "labels": []interface{}{"Article"}, "changesTo": []interface{}{"active_version"}},
	map[string]interface{}{"select": "n", "labels": []interface{}{"Revision"}},
	map[string]interface{}{"select": "n", "labels": []interface{}{"Checkpoint"}},
	map[string]interface{}{"select": "r", "type": "LINK"},
	map[string]interface{}{"select": "r", "type": "CORRECTS"},
}

const cdcReductionUpdated = "x.operation <> 'd' AND 'updated_at' IN x.changed"

// cdcReductionStatement reduces one page of changes after $cursor.
const cdcReductionStatement = `
CALL db.cdc.query($cursor, $selectors, $limit) YIELD id, metadata, event
WITH id, metadata.txCommitTime AS at, event,
     coalesce(event.state.before.properties, {}) AS before,
     coalesce(event.state.after.properties, {}) AS after
CALL (event) {
  WITH event WHERE event.eventType = 'n'
  OPTIONAL MATCH (n) WHERE elementId(n) = event.elementId
  RETURN n.id AS found, n.origin AS origin, [] AS ends
  UNION ALL
  WITH event WHERE event.eventType = 'r'
  OPTIONAL MATCH (a) WHERE elementId(a) = event.start.elementId
  OPTIONAL MATCH (b) WHERE elementId(b) = event.end.elementId
  OPTIONAL MATCH (a)-[r]->(b) WHERE elementId(r) = event.elementId
  RETURN r.id AS found, null AS origin, [x IN [a.id, b.id] WHERE x IS NOT NULL] AS ends
}
WITH collect({id: id, at: at, operation: event.operation, type: event.type,
              labels: coalesce(event.labels, []), ends: ends,
              element: coalesce(found, after.id, before.id),
              origin: coalesce(origin, after.origin, before.origin),
              changed: [key IN keys(after) WHERE NOT key IN keys(before) OR after[key] <> before[key]]
                       + [key IN keys(before) WHERE NOT key IN keys(after)]}) AS rows
RETURN size(rows) AS read, last(rows).id AS cursor, toString(last(rows).at) AS read_through,
       apoc.coll.toSet([x IN rows WHERE 'Topic' IN x.labels AND ` + cdcReductionUpdated + ` | x.element]) AS topics,
       apoc.coll.toSet([x IN rows WHERE 'Person' IN x.labels AND ` + cdcReductionUpdated + ` AND x.origin IN ['manual', 'import'] | x.element]) AS people,
       apoc.coll.toSet([x IN rows WHERE 'Person' IN x.labels AND any(key IN x.changed WHERE key IN ['match_key', 'alt_keys']) | x.element]) AS matching,
       apoc.coll.toSet([x IN rows WHERE 'Person' IN x.labels AND x.operation = 'c' AND x.origin = 'generated' | x.element]) AS created,
       apoc.coll.toSet([x IN rows WHERE 'Revision' IN x.labels AND ` + cdcReductionUpdated + ` | x.element]) AS revisions,
       apoc.coll.toSet([x IN rows WHERE x.type = 'LINK' AND ` + cdcReductionUpdated + ` | x.element]) AS links,
       apoc.coll.toSet([x IN rows WHERE 'Article' IN x.labels AND 'active_version' IN x.changed | x.element]) AS selections,
       apoc.coll.toSet([element IN reduce(every = [], x IN [x IN rows WHERE NOT 'Checkpoint' IN x.labels]
                                          | every + [x.element] + x.ends)
                        WHERE element IS NOT NULL]) AS elements`

// cdcReduction runs the statement from cursor and returns its row by column.
func cdcReduction(t testing.TB, exec *StorageExecutor, cursor string, limit int) map[string]interface{} {
	t.Helper()
	result, err := exec.Execute(context.Background(), cdcReductionStatement, map[string]interface{}{"cursor": cursor, "selectors": cdcReductionSelectors, "limit": limit})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	row := map[string]interface{}{}
	for index, column := range result.Columns {
		row[column] = result.Rows[0][index]
	}
	return row
}

// TestCDCReductionStatement is the reduction's acceptance: creates, DIFF
// updates that add, change and remove properties, a deleted relationship
// and nodes (their ids from the before state; r0 changes only by its
// delete), relationship ends, the
// cursor's own change left out of the elements, a second pass that reads
// only the cursor write, and an expired cursor (InvalidIdentifier).
func TestCDCReductionStatement(t *testing.T) {
	exec, store := newCDCExecutor(t, storage.ChangeCaptureDiff)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Record {id: 'r0'})", nil)
	require.NoError(t, err)
	start := cdcID(t, exec, "db.cdc.current")
	for _, statement := range []string{
		`CREATE (:Topic {id: 'c1', updated_at: 1}), (:Person {id: 'u1', origin: 'manual', match_key: 'a', updated_at: 1}),
		        (:Person {id: 'g1', origin: 'generated'}), (:Article {id: 'd1', active_version: 'v1'}),
		        (:Record {id: 'r1', note: 'x'}), (:Record {id: 'r2'}), (:Record {id: 'r3'}), (:Other {id: 'o1'})`,
		`MATCH (a:Record {id: 'r1'}), (b:Record {id: 'r2'}), (c:Record {id: 'r3'})
		 CREATE (a)-[:LINK {id: 'l1', origin: 'manual', updated_at: 1}]->(b), (c)-[:LINK {id: 'l2', origin: 'generated'}]->(a)`,
		`MATCH (c:Topic {id: 'c1'}), (u:Person {id: 'u1'}), (d:Article {id: 'd1'}), (r:Record {id: 'r1'})-[l:LINK]->()
		 SET c.updated_at = 2, u.match_key = 'b', d.active_version = 'v2', l.updated_at = 2 REMOVE r.note`,
		"MATCH (d:Article {id: 'd1'}) SET d.title = 'unselected'",
		"MATCH (r:Record) WHERE r.id IN ['r0', 'r3'] DETACH DELETE r",
		"MERGE (f:Checkpoint {id: 'checkpoint'}) SET f.position = 1",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err)
	}
	first := cdcReduction(t, exec, start, 5000)
	// Seven selected creates, two links, five updates, the deleted link and
	// two nodes, and the cursor; not the Other node or the Article's title.
	require.Equal(t, int64(18), first["read"])
	require.Equal(t, []interface{}{"c1"}, first["topics"])
	require.Equal(t, []interface{}{"u1"}, first["people"])
	require.Equal(t, []interface{}{"u1"}, first["matching"])
	require.Equal(t, []interface{}{"g1"}, first["created"])
	require.Equal(t, []interface{}{}, first["revisions"])
	require.Equal(t, []interface{}{"l1"}, first["links"])
	require.Equal(t, []interface{}{"d1"}, first["selections"])
	require.ElementsMatch(t, []interface{}{"c1", "u1", "g1", "d1", "r1", "r2", "r3", "l1", "l2", "r0"}, first["elements"])
	cursor := first["cursor"].(string)
	require.Empty(t, cdcRows(t, exec, cursor)) // the last change read
	readThrough, err := time.Parse(time.RFC3339Nano, first["read_through"].(string))
	require.NoError(t, err)
	require.WithinDuration(t, time.Now(), readThrough, time.Minute)

	// Paged: the same elements over pages of 4, each from the last cursor.
	elements := map[interface{}]bool{}
	for from, read := start, int64(0); read < 18; {
		page := cdcReduction(t, exec, from, 4)
		read += page["read"].(int64)
		from = page["cursor"].(string)
		for _, element := range page["elements"].([]interface{}) {
			elements[element] = true
		}
	}
	require.Len(t, elements, 10)

	// The next pass reads only the cursor's own write.
	_, err = exec.Execute(ctx, "MERGE (f:Checkpoint {id: 'checkpoint'}) SET f.position = 2", nil)
	require.NoError(t, err)
	second := cdcReduction(t, exec, cursor, 5000)
	require.Equal(t, int64(1), second["read"])
	require.Equal(t, []interface{}{}, second["elements"])
	require.Empty(t, cdcRows(t, exec, second["cursor"].(string)))
	// Nothing after it: no row read, no cursor.
	none := cdcReduction(t, exec, second["cursor"].(string), 5000)
	require.Equal(t, int64(0), none["read"])
	require.Nil(t, none["cursor"])

	// A cursor older than the retained feed is expired.
	capture, _ := storage.ChangeCaptureOf(store)
	type pruner interface {
		PruneChangeEvents(namespace string, cutoff time.Time) (int, error)
	}
	_, err = capture.(pruner).PruneChangeEvents("test", time.Now().Add(time.Hour))
	require.NoError(t, err)
	_, err = exec.Execute(ctx, cdcReductionStatement, map[string]interface{}{"cursor": start, "selectors": cdcReductionSelectors, "limit": 5000})
	requireStatusCode(t, err, "Neo.ClientError.ChangeDataCapture.InvalidIdentifier")
}
