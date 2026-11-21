package cypher

// v1.5.2 close-out lane L4: behaviour register rows (answers, counts, caches,
// text.locate cost, CDC page cost, index readiness, MERGE re-publication).

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// H-227: a later comma-MATCH property map reads a variable bound by an earlier
// pattern of the same MATCH, and the write that follows reaches its node.
func TestCloseoutL4H227CommaMatchPropertyReference(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for name, write := range map[string]string{
			"set":    "MATCH (a:A227 {id: 1}), (b:B227 {ref: a.key}) SET b.hit = true RETURN count(b) AS n",
			"create": "MATCH (a:A227 {id: 1}), (b:B227 {ref: a.key}) CREATE (a)-[:LINK227]->(b) RETURN count(b) AS n",
			"merge":  "MATCH (a:A227 {id: 1}), (b:B227 {ref: a.key}) MERGE (a)-[:LINK227]->(b) RETURN count(b) AS n",
		} {
			exec := NewStorageExecutor(l4MemStore(t, "l4"))
			l4Run(t, exec, "CREATE (:A227 {id: 1, key: 'k1'}), (:B227 {ref: 'k1'}), (:B227 {ref: 'k2'})", nil)
			rows := l4Run(t, exec, write, nil)
			var effect [][]any
			if name == "set" {
				effect = l4Run(t, exec, "MATCH (b:B227) WHERE b.hit = true RETURN b.ref", nil)
			} else {
				effect = l4Run(t, exec, "MATCH (:A227)-[:LINK227]->(b:B227) RETURN b.ref", nil)
			}
			ok := len(rows) == 1 && fmt.Sprint(rows[0][0]) == "1" && len(effect) == 1 && fmt.Sprint(effect[0][0]) == "k1"
			l4Report(t, "H-227", "comma-match-"+name, ok, map[string]any{"rows": rows, "effect": effect})
		}
	})
}

// H-232: after the relationships are purged, every count agrees with MATCH:
// the statement counts, the stored type and total counts, and after a
// restart of the disk stack.
func TestCloseoutL4H232RelationshipCountAfterPurge(t *testing.T) {
	purges := map[string][]string{
		"delete-relationships":    {"MATCH ()-[r:T232]->() DELETE r"},
		"detach-delete-all":       {"MATCH (n) DETACH DELETE n"},
		"explicit-tx-delete-rels": {"BEGIN", "MATCH ()-[r:T232]->() DELETE r", "COMMIT"},
	}
	counts := func(t *testing.T, exec *StorageExecutor, engine storage.Engine) map[string]any {
		typed, errTyped := engine.EdgeCountByType(context.Background(), "T232")
		total, errTotal := engine.EdgeCount(context.Background())
		return map[string]any{
			"matchTyped":   fmt.Sprint(l4Run(t, exec, "MATCH ()-[r:T232]->() RETURN count(r)", nil)[0][0]),
			"matchAll":     fmt.Sprint(l4Run(t, exec, "MATCH ()-[r]->() RETURN count(r)", nil)[0][0]),
			"matchListed":  fmt.Sprint(len(l4Run(t, exec, "MATCH ()-[r:T232]->() RETURN r", nil))),
			"storedTyped":  fmt.Sprint(typed, errTyped),
			"storedTotal":  fmt.Sprint(total, errTotal),
		}
	}
	zero := func(c map[string]any) bool {
		return c["matchTyped"] == "0" && c["matchAll"] == "0" && c["matchListed"] == "0" && c["storedTyped"] == "0 <nil>" && c["storedTotal"] == "0 <nil>"
	}
	forEachParser(t, func(t *testing.T) {
		for purge, statements := range purges {
			engine := l4MemStore(t, "l4")
			exec := NewStorageExecutor(engine)
			l4Run(t, exec, "UNWIND range(1, 7) AS i CREATE (:P232 {i: i})-[:T232]->(:Q232 {i: i})", nil)
			before := counts(t, exec, engine)
			for _, statement := range statements {
				l4Run(t, exec, statement, nil)
			}
			after := counts(t, exec, engine)
			l4Report(t, "H-232", "memory/"+purge, before["matchTyped"] == "7" && zero(after), map[string]any{"before": before, "after": after})

			dir := t.TempDir()
			disk := l4OpenDisk(t, dir)
			exec = NewStorageExecutor(disk.engine)
			l4Run(t, exec, "UNWIND range(1, 7) AS i CREATE (:P232 {i: i})-[:T232]->(:Q232 {i: i})", nil)
			for _, statement := range statements {
				l4Run(t, exec, statement, nil)
			}
			after = counts(t, exec, disk.engine)
			disk.close()
			reopened := l4OpenDisk(t, dir)
			exec = NewStorageExecutor(reopened.engine)
			restarted := counts(t, exec, reopened.engine)
			l4Report(t, "H-232", "disk-restart/"+purge, zero(after) && zero(restarted), map[string]any{"after": after, "afterRestart": restarted})
		}
	})
}

// H-77: a read served by an executor's caches agrees with what was committed
// since: by another executor on the same database, in an explicit
// transaction, and not after a rollback.
func TestCloseoutL4H77CachedReadsFollowCommits(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		engine := l4MemStore(t, "l4")
		reader, writer := NewStorageExecutor(engine), NewStorageExecutor(engine)
		l4Run(t, reader, "CREATE (:C77 {id: 1, v: 1})", nil)
		value := func() string { return fmt.Sprint(l4Run(t, reader, "MATCH (n:C77 {id: 1}) RETURN n.v", nil)[0][0]) }
		count := func() string { return fmt.Sprint(l4Run(t, reader, "MATCH (n:C77) RETURN count(n)", nil)[0][0]) }
		steps := map[string]string{}
		value()
		count()
		steps["warm"] = value() + "/" + count()
		l4Run(t, writer, "MATCH (n:C77 {id: 1}) SET n.v = 2", nil)
		l4Run(t, writer, "CREATE (:C77 {id: 2, v: 0})", nil)
		steps["other-executor-commit"] = value() + "/" + count()
		l4Run(t, writer, "BEGIN", nil)
		l4Run(t, writer, "MATCH (n:C77 {id: 1}) SET n.v = 3", nil)
		l4Run(t, writer, "COMMIT", nil)
		steps["other-executor-explicit-commit"] = value() + "/" + count()
		l4Run(t, reader, "BEGIN", nil)
		l4Run(t, reader, "MATCH (n:C77 {id: 1}) SET n.v = 4", nil)
		l4Run(t, reader, "ROLLBACK", nil)
		steps["own-rollback"] = value() + "/" + count()
		l4Run(t, reader, "MATCH (n:C77 {id: 2}) DELETE n", nil)
		steps["own-delete"] = value() + "/" + count()
		ok := steps["warm"] == "1/1" && steps["other-executor-commit"] == "2/2" && steps["other-executor-explicit-commit"] == "3/2" && steps["own-rollback"] == "3/2" && steps["own-delete"] == "3/1"
		l4Report(t, "H-77", "cached-reads-follow-commits", ok, map[string]any{"steps": steps})
	})
}

// H-144 (I54): text.locate's cost follows the range it searches, not the
// whole text, also when one statement locates many quotes in the same text.
func TestCloseoutL4H144TextLocateRangeCost(t *testing.T) {
	text := func(size int) string {
		var b strings.Builder
		for b.Len() < 50_000 {
			b.WriteString("Filler words on a line of text.\n")
		}
		b.WriteString("The quoted passage is here.\n")
		for b.Len() < size {
			b.WriteString("Filler words on a line of text.\n")
		}
		return b.String()
	}
	quotes := make([]any, 30)
	for i := range quotes {
		quotes[i] = "quoted passage"
	}
	statements := map[string]string{
		"one-quote-range": "RETURN text.locate($text, 'quoted passage', {from: 0, to: 100000}) AS span",
		"one-quote-line":  "RETURN text.locate($text, 'quoted passage', {line: 1500, from: 0, to: 100000}) AS span",
		"30-quotes-param": "UNWIND $quotes AS q RETURN text.locate($text, q, {from: 0, to: 100000}) AS span",
		"30-quotes-bound": "WITH $text AS t UNWIND $quotes AS q RETURN text.locate(t, q, {from: 0, to: 100000}) AS span",
	}
	exec := NewStorageExecutor(l4MemStore(t, "l4"))
	for name, statement := range statements {
		times := map[int]float64{}
		for _, size := range []int{100_000, 500_000} {
			params := map[string]any{"text": text(size), "quotes": quotes}
			best := time.Duration(-1)
			var rows [][]any
			for run := 0; run < 5; run++ {
				exec.ClearQueryCaches()
				start := time.Now()
				rows = l4Run(t, exec, statement, params)
				if elapsed := time.Since(start); best < 0 || elapsed < best {
					best = elapsed
				}
			}
			require.NotEmpty(t, rows)
			require.NotNil(t, rows[0][0], statement)
			times[size] = float64(best.Microseconds()) / 1000
		}
		ratio := times[500_000] / l4Max(times[100_000], 0.05)
		l4Report(t, "H-144", name, ratio < 2.5, map[string]any{"ms100k": times[100_000], "ms500k": times[500_000], "ratio": ratio})
	}
}

// H-389: a CDC page of 1,000 events costs the same whether 1,000 or 16,000
// earlier relationship changes precede it.
func TestCloseoutL4H389CDCPageCost(t *testing.T) {
	times := map[int]float64{}
	for _, links := range []int{1000, 16000} {
		exec, _ := newCDCExecutor(t, storage.ChangeCaptureFull)
		l4Run(t, exec, "CREATE (:Hub389 {id: 'hub'})", nil)
		l4Batch(t, exec, links, "MATCH (h:Hub389 {id: 'hub'}) UNWIND range($start, $end - 1) AS k CREATE (h)-[:LINK389 {k: k}]->(:Leaf389 {k: k})")
		from := cdcID(t, exec, "db.cdc.current")
		l4Run(t, exec, "UNWIND range(1, 1000) AS k CREATE (:Member389 {k: k})", nil)
		best := time.Duration(-1)
		var rows [][]any
		for run := 0; run < 3; run++ {
			exec.ClearQueryCaches()
			start := time.Now()
			rows = l4Run(t, exec, "CALL db.cdc.query($from, [], 1000) YIELD id RETURN count(id) AS n", map[string]any{"from": from})
			if elapsed := time.Since(start); best < 0 || elapsed < best {
				best = elapsed
			}
		}
		times[links] = float64(best.Microseconds()) / 1000
		if len(rows) != 1 || fmt.Sprint(rows[0][0]) != "1000" {
			l4Report(t, "H-389", "cdc-page-rows", false, map[string]any{"links": links, "rows": rows})
		}
	}
	ratio := times[16000] / l4Max(times[1000], 0.05)
	l4Report(t, "H-389", "cdc-page-cost", ratio < 3, map[string]any{"ms1000links": times[1000], "ms16000links": times[16000], "ratio": ratio})
}

// H-382: while a vector index's build runs (a restart rebuilding it), SHOW
// INDEXES reports it POPULATING and db.awaitIndexes / db.awaitIndex return
// only once it is ONLINE (Neo4j's behaviour).
func TestCloseoutL4H382IndexReadinessDuringStartupBuild(t *testing.T) {
	for _, await := range []string{"CALL db.awaitIndexes(120)", "CALL db.awaitIndex('l4vec382', 120)"} {
		engine := l4MemStore(t, "l4")
		exec := NewStorageExecutor(engine)
		first := search.NewService(engine, 3, nil)
		t.Cleanup(func() { _ = first.Close() })
		require.NoError(t, first.BuildIndexes(context.Background()))
		exec.SetSearchService(first)
		l4Run(t, exec, "CREATE VECTOR INDEX l4vec382 FOR (n:Doc382) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
		l4Batch(t, exec, 60000, "UNWIND range($start, $end - 1) AS k CREATE (:Doc382 {id: k, embedding: [toFloat(k % 7) + 1.0, 1.0, 0.5]})")

		// Restart: a new service builds while statements arrive.
		restarted := NewStorageExecutor(engine)
		service := search.NewService(engine, 3, nil)
		t.Cleanup(func() { _ = service.Close() })
		restarted.SetSearchService(service)
		done := make(chan error, 1)
		go func() { done <- service.BuildIndexes(context.Background()) }()
		deadline := time.Now().Add(10 * time.Second)
		for !service.BuildInProgress() && !service.IsReady() && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		buildingAtShow := service.BuildInProgress() && !service.IsReady()
		showRows := l4Run(t, restarted, "SHOW INDEXES YIELD name, state WHERE name = 'l4vec382' RETURN state", nil)
		state := ""
		if len(showRows) == 1 {
			state = fmt.Sprint(showRows[0][0])
		}
		buildingAtAwait := service.BuildInProgress() && !service.IsReady()
		start := time.Now()
		_, awaitErr := restarted.Execute(context.Background(), await, nil)
		awaitMillis := time.Since(start).Milliseconds()
		readyAfterAwait := service.IsReady()
		buildErr := <-done
		stateAfter := fmt.Sprint(l4Run(t, restarted, "SHOW INDEXES YIELD name, state WHERE name = 'l4vec382' RETURN state", nil))
		detail := map[string]any{"await": await, "buildingAtShow": buildingAtShow, "stateDuringBuild": state, "buildingAtAwait": buildingAtAwait,
			"awaitMillis": awaitMillis, "awaitError": fmt.Sprint(awaitErr), "readyAfterAwait": readyAfterAwait, "buildError": fmt.Sprint(buildErr), "stateAfterBuild": stateAfter}
		if !buildingAtShow {
			detail["note"] = "build finished before SHOW INDEXES ran; state check not decisive"
		}
		l4Report(t, "H-382", "show-indexes-during-build/"+await, !buildingAtShow || state == "POPULATING", detail)
		if buildingAtAwait {
			l4Report(t, "H-382", "await-waits-for-online/"+await, awaitErr == nil && readyAfterAwait, detail)
		} else {
			l4Report(t, "H-382", "await-not-decisive/"+await, true, detail)
		}
	}
}

// H-417 (1): re-publishing existing keys with UNWIND ... MERGE ... ON MATCH
// SET (a list-membership test on the matched node) costs the same per row at
// 1,000 and at 8,000 rows.
func TestCloseoutL4H417MergeRepublicationPerRowCost(t *testing.T) {
	statement := "UNWIND $nodes AS node MERGE (n:PDGraphNode:PDEntity {id: node.id}) ON CREATE SET n.created = node.v ON MATCH SET n.v = node.v, n.carried = n.id IN $carried SET n.seen = true"
	carried := make([]any, 100)
	for i := range carried {
		carried[i] = fmt.Sprintf("e%d", i*7)
	}
	forEachParser(t, func(t *testing.T) {
		perRow := map[int]float64{}
		for _, size := range []int{1000, 8000} {
			exec := NewStorageExecutor(l4MemStore(t, "l4"))
			l4Run(t, exec, "CREATE CONSTRAINT l4_entity_id FOR (n:PDGraphNode) REQUIRE n.id IS UNIQUE", nil)
			l4Batch(t, exec, size, "UNWIND range($start, $end - 1) AS k CREATE (:PDGraphNode:PDEntity {id: 'e' + toString(k), v: 0})")
			nodes := make([]any, size)
			for i := range nodes {
				nodes[i] = map[string]any{"id": fmt.Sprintf("e%d", i), "v": int64(i)}
			}
			best := time.Duration(-1)
			for run := 0; run < 2; run++ {
				start := time.Now()
				l4Run(t, exec, statement, map[string]any{"nodes": nodes, "carried": carried})
				if elapsed := time.Since(start); best < 0 || elapsed < best {
					best = elapsed
				}
			}
			count := l4Run(t, exec, "MATCH (n:PDGraphNode) RETURN count(n), sum(CASE WHEN n.carried THEN 1 ELSE 0 END)", nil)
			if fmt.Sprint(count[0][0]) != fmt.Sprint(size) {
				l4Report(t, "H-417", "merge-republication-rows", false, map[string]any{"size": size, "count": count})
			}
			perRow[size] = float64(best.Microseconds()) / float64(size)
		}
		ratio := perRow[8000] / l4Max(perRow[1000], 0.001)
		l4Report(t, "H-417", "merge-republication-per-row", ratio < 3, map[string]any{"usPerRow1000": perRow[1000], "usPerRow8000": perRow[8000], "ratio": ratio})
	})
}
