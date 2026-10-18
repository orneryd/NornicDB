//go:build rule73probe

package cypher

// Rule 73 self-attack for #1412 (probe; not product code). For each member
// of the extracted inventory reachable from a statement, three checks on the
// current head:
//   over: under a limit below what the member holds live, the statement
//         fails with Neo.TransientError.General.TransactionMemoryLimit;
//   near: under a limit of about 1.6x what it holds live (plus a small
//         baseline), it succeeds with the no-limit rows (a double count or a
//         charge kept past its structure fails this);
//   zero: both pools are empty after each run.
// It never stops at a failure: every member reports one line
//   R73 <member> over=<ok|FAIL …> near=<ok|FAIL …|n/a> zero=<ok|FAIL>.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type zzProbe struct {
	member string
	query  string
	params map[string]interface{}
	live   int    // entities the member holds live
	near   bool   // whether a near-limit run discriminates (rows don't dominate)
	tx     bool   // run inside BEGIN … ROLLBACK
	setup  func() // per run (for statements that write)
}

func zzPoolsZero(database string) bool {
	global, used := TransactionMemoryPoolUsage(database)
	return global == 0 && used == 0
}

func zzRun(executor *StorageExecutor, p zzProbe, limit int64) ([][]interface{}, error) {
	ctx := context.Background()
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: limit})
	if p.setup != nil {
		saved := TransactionMemoryLimits{TransactionMax: limit}
		ConfigureTransactionMemory(0, TransactionMemoryLimits{})
		p.setup()
		ConfigureTransactionMemory(0, saved)
	}
	if p.tx {
		if _, err := executor.Execute(ctx, "BEGIN", nil); err != nil {
			return nil, err
		}
		defer func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) }()
	}
	result, err := executor.Execute(ctx, p.query, p.params)
	if err != nil {
		return nil, err
	}
	return result.Rows, nil
}

func TestZZRule73Attack(t *testing.T) {
	resetTransactionMemory(t)
	store := newTransactionMemoryEngine(t)
	executor := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	ctx := context.Background()
	exec := func(query string) {
		ConfigureTransactionMemory(0, TransactionMemoryLimits{})
		_, err := executor.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
	// Data.
	exec("UNWIND range(1, 40000) AS i CREATE (:Bulk {p: i % 2, k: i, text: 'attack searchable text'})")
	exec("UNWIND range(1, 30000) AS i CREATE (:Idx {p: 1, k: i})")
	exec("CREATE INDEX attack_idx_p FOR (n:Idx) ON (n.p)")
	exec("CREATE (:Hub {k: 0})")
	for i := 0; i < 30000; i++ {
		id := storage.NodeID(fmt.Sprintf("leaf-%d", i))
		_, err := store.CreateNode(&storage.Node{ID: id, Labels: []string{"Leaf"}, Properties: map[string]interface{}{"k": int64(i)}})
		require.NoError(t, err)
	}
	hub, err := executor.Execute(ctx, "MATCH (h:Hub) RETURN id(h) AS id", nil)
	require.NoError(t, err)
	hubID := storage.NodeID(fmt.Sprint(hub.Rows[0][0]))
	if node, err := store.GetNode(hubID); err != nil || node == nil {
		// The id() of a namespaced node is its user ID.
		hubID = storage.NodeID(strings.TrimPrefix(string(hubID), "nornic:"))
	}
	for i := 0; i < 30000; i++ {
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("fan-%d", i)), StartNode: hubID, EndNode: storage.NodeID(fmt.Sprintf("leaf-%d", i)), Type: "FAN", Properties: map[string]interface{}{"p": int64(1)}}))
	}
	_, err = store.CreateNode(&storage.Node{ID: "pair-a", Labels: []string{"Pair"}, Properties: map[string]interface{}{"k": int64(0)}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "pair-b", Labels: []string{"Pair"}, Properties: map[string]interface{}{"k": int64(1)}})
	require.NoError(t, err)
	for i := 0; i < 20000; i++ {
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("pair-%d", i)), StartNode: "pair-a", EndNode: "pair-b", Type: "PAIR", Properties: map[string]interface{}{"p": int64(i + 100)}}))
	}
	for _, typ := range []string{"LEFT", "RIGHT"} {
		for i := 0; i < 10000; i++ {
			require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("%s-%d", typ, i)), StartNode: "pair-a", EndNode: "pair-b", Type: typ, Properties: map[string]interface{}{"p": int64(1)}}))
		}
	}
	exec("CREATE FULLTEXT INDEX attack_text FOR (n:Bulk) ON EACH [n.text]")
	delCount := 0
	freshDel := func(label string, n int) func() {
		return func() {
			delCount++
			exec(fmt.Sprintf("MATCH (n:%s) DETACH DELETE n", label))
			exec(fmt.Sprintf("UNWIND range(1, %d) AS i CREATE (:%s {k: i})", n, label))
		}
	}
	rows := make([]interface{}, 0, 20000)
	for i := 0; i < 20000; i++ {
		rows = append(rows, map[string]interface{}{"a": int64(i), "b": int64(i)})
	}

	probes := []zzProbe{
		// Scans and seeks (pipeline collectors, index results).
		{member: "S/P pipeline label scan candidates (collectNodesWithStreamingProjection, heldNodeList)", query: "UNWIND [1] AS i MATCH (n:Bulk {p: i}) RETURN count(*) AS n", live: 20000, near: true},
		{member: "P index seek candidates + ID sets (lookupPatternCandidatesUsingPropertyIndex)", query: "UNWIND [1] AS i MATCH (n:Idx {p: i}) RETURN count(*) AS n", live: 30000, near: true},
		{member: "P index seek, WHERE equality (tryCollectNodesFromPropertyIndexAnyOf)", query: "UNWIND [1] AS i MATCH (n:Idx) WHERE n.p = i RETURN count(*) AS n", live: 30000, near: true},
		{member: "P index seek IN list (tryCollectNodesFromPropertyIndexIn*)", query: "MATCH (n:Idx) WHERE n.p IN $ps RETURN count(*) AS n", params: map[string]interface{}{"ps": []interface{}{int64(1), int64(2)}}, live: 30000, near: true},
		{member: "P index seek, explicit transaction (propertyIndexLookup merge)", query: "MATCH (n:Idx {p: 1}) RETURN n.p AS p LIMIT 1", live: 30000, near: true, tx: true},
		{member: "P label scan, explicit transaction (transactionStorageWrapper reads)", query: "UNWIND [1] AS i MATCH (n:Bulk {p: i}) RETURN count(*) AS n", live: 20000, near: true, tx: true},
		// MERGE.
		{member: "P MERGE label-scan candidates (findMergeNodesScanned)", query: "MERGE (n:Bulk {p: 0}) RETURN count(*) AS n", live: 40000, near: true},
		{member: "S MERGE relationship identity cache (relationshipMergeIdentityCache.entries)", query: "UNWIND [0, 1] AS p WITH p ORDER BY p MATCH (a:Pair {k: 0}) MATCH (b:Pair {k: 1}) MERGE (a)-[r:PAIR {p: p}]->(b) RETURN count(*) AS n", live: 20000, near: true},
		// DELETE.
		{member: "P DELETE … LIMIT candidates (collectDeleteWithLimitCandidates)", query: "MATCH (n:DelA) WITH n LIMIT 10 DELETE n", live: 40000, near: true, setup: freshDel("DelA", 40000)},
		{member: "S deleted-entity sets (deletedEntities) + DETACH DELETE", query: "MATCH (n:DelB) DETACH DELETE n", live: 40000, near: false, setup: freshDel("DelB", 40000)},
		// Traversal.
		{member: "P relationship-type seeds (relationshipTypeSeedNodes)", query: "MATCH (a)-[r:LEFT|RIGHT {p: -1}]->(b) RETURN b", live: 10000, near: true},
		{member: "P undirected type count (countUndirectedRelationshipMatches)", query: "MATCH (a)-[:FAN]-(b) RETURN count(*) AS n", live: 30000, near: true},
		{member: "P fast single-hop aggregation (getEdgesByTypeFast, batchGetNodesFast)", query: "MATCH (h:Hub)-[:FAN]->(l:Leaf) RETURN h.k AS k, count(l) AS n", live: 60000, near: true},
		{member: "S traversal paths/nodeCache (TraversalContext) one hop from a hub", query: "MATCH (h:Hub)-[:FAN*1..1]->(l) RETURN count(*) AS n", live: 30000, near: false},
		{member: "S path search adjacency cache + frontier (pathSearch.edges, pathSearchWalkSide)", query: "MATCH (a:Leaf {k: 1}), (b:Leaf {k: 2}) MATCH p = shortestPath((a)-[:FAN*..2]-(b)) RETURN length(p) AS l", live: 30000, near: true},
		{member: "R reviewed one-node read: existence check on a hub (evaluateRelationshipPatternInWhere / traversalEdges)", query: "MATCH (h:Hub) WHERE (h)-[:FAN]->(:Leaf {k: -5}) RETURN count(*) AS n", live: 30000, near: true},
		{member: "R reviewed one-node read: degree count (nodeDegreeCount)", query: "MATCH (h:Hub) RETURN COUNT { (h)-[:FAN]->() } AS n", live: 30000, near: true},
		{member: "S pattern comprehension over a hub", query: "MATCH (h:Hub) RETURN size([(h)-[:FAN]->(l) | l.k]) AS n", live: 30000, near: false},
		{member: "S quantified path state (quantifiedPathState) from a hub", query: "MATCH (h:Hub) MATCH p = (h)(()-[:FAN]->())*(x) RETURN count(*) AS n", live: 30000, near: false},
		// Optional single node (temporal viewport loader).
		{member: "P temporal viewport loader (loadNodesWithTemporalViewport) via OPTIONAL MATCH", query: "MATCH (h:Hub) OPTIONAL MATCH (b:Bulk {p: -1}) RETURN count(*) AS n", live: 40000, near: true},
		// Procedures and APOC.
		{member: "P db.labels (AllNodes)", query: "CALL db.labels() YIELD label RETURN count(*) AS n", live: 130000, near: true},
		{member: "P db.relationshipTypes (AllEdges)", query: "CALL db.relationshipTypes() YIELD relationshipType RETURN count(*) AS n", live: 70000, near: true},
		{member: "P db.propertyKeys (AllNodes + AllEdges)", query: "CALL db.propertyKeys() YIELD propertyKey RETURN count(*) AS n", live: 200000, near: true},
		{member: "P db.schema.nodeTypeProperties", query: "CYPHER 5 CALL db.schema.nodeTypeProperties() YIELD nodeType RETURN count(*) AS n", live: 130000, near: true},
		{member: "P db.stats.retrieve GRAPH COUNTS", query: "CALL db.stats.retrieve('GRAPH COUNTS') YIELD data RETURN count(*) AS n", live: 200000, near: true},
		{member: "P fulltext queryNodes scan", query: "CALL db.index.fulltext.queryNodes('attack_text', 'attack') YIELD node RETURN count(*) AS n", live: 130000, near: false},
		{member: "P APOC algorithm input (algorithmNodes, wcc)", query: "CALL apoc.algo.wcc('Leaf') YIELD node RETURN count(*) AS n", live: 30000, near: false},
		{member: "S APOC path expansion from a hub (apocExpansion)", query: "MATCH (h:Hub) CALL apoc.path.expand(h, 'FAN>', null, 1, 1) YIELD path RETURN count(*) AS n", live: 30000, near: false},
		// Batch prefetch (context value).
		{member: "C batch prefetch map (executeUnwindRelationshipMergeBatch)", query: "UNWIND $rows AS row MATCH (a:Leaf {k: row.a}) MATCH (b:Leaf {k: row.b}) MERGE (a)-[r:SELF {i: row.a}]->(b) RETURN count(*) AS n", params: map[string]interface{}{"rows": rows}, live: 20000, near: false},
	}
	only := os.Getenv("R73_PART")
	for index, p := range probes {
		if only == "1" && index >= 15 || only == "2" && index < 15 {
			continue
		}
		want, err := zzRun(executor, p, 0)
		if err != nil {
			t.Logf("R73\t%s\tbaseline error: %v", p.member, err)
			continue
		}
		var report []string
		// over: half of what the member holds live.
		overLimit := int64(p.live) * entityReferenceBytes / 2
		if overLimit < 64<<10 {
			overLimit = 64 << 10
		}
		_, err = zzRun(executor, p, overLimit)
		switch {
		case err != nil && (strings.Contains(err.Error(), "TransactionMemoryLimit") || strings.Contains(err.Error(), "db.memory.transaction.max threshold reached")):
			report = append(report, "over=ok")
		case err != nil && memoryErrorCode(err) == "Neo.TransientError.General.TransactionMemoryLimit":
			report = append(report, "over=ok")
		case err != nil:
			report = append(report, "over=FAIL(other error: "+err.Error()+")")
		default:
			report = append(report, fmt.Sprintf("over=FAIL(no error at %d bytes)", overLimit))
		}
		zero := zzPoolsZero("nornic")
		if p.near {
			nearLimit := int64(p.live)*entityReferenceBytes*16/10 + 256<<10
			got, err := zzRun(executor, p, nearLimit)
			switch {
			case err != nil:
				report = append(report, fmt.Sprintf("near=FAIL(%d bytes: %v)", nearLimit, err))
			case fmt.Sprint(got) != fmt.Sprint(want):
				report = append(report, "near=FAIL(rows differ)")
			default:
				report = append(report, "near=ok")
			}
			zero = zero && zzPoolsZero("nornic")
		} else {
			report = append(report, "near=n/a")
		}
		if zero {
			report = append(report, "zero=ok")
		} else {
			report = append(report, "zero=FAIL")
		}
		t.Logf("R73\t%s\t%s", p.member, strings.Join(report, " "))
	}
}

func memoryErrorCode(err error) string {
	if coded, ok := err.(interface{ Code() string }); ok {
		return coded.Code()
	}
	return ""
}
