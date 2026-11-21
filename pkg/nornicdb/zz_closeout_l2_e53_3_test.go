//go:build closeout_l2_e53_3

package nornicdb_test

// L2 close-out, report entry 53.3: with asynchronous writes, a JSON backup
// restore that relabels a node miscounts relationships by label. A
// relationship still in the write-behind buffer ends on a node the restore
// relabels (Held -> After); afterwards the stored count by end label
// (MATCH ()-[r:OTHER]->(:L) RETURN count(r)) disagrees with a traversal.
//
// Port: the entry obtained the JSON backup by wrapping the engine for the
// duration of db.Backup so the native backup was not offered. This test writes
// the same file the JSON fallback of db.Backup writes (db_admin.go:1442-1468 at
// the pin: CollectStoredNodes/CollectStoredEdges of the database's storage,
// PropertyJSON.MarshalIndent of {version 2.0, created_at, nodes, edges}).
// db.Restore detects the JSON file and takes its legacy JSON path itself.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const l2E53_3Runs = 20

type l2E53_3Outcome struct {
	restoreErr error
	// stored and traversal counts per label, in the order Held, After.
	stored, traversal [2]string
	// count store readings by end label (Held, After) before the restore,
	// after it and after the final flush.
	before, afterRestore, afterFlush [2]int64
	labels                           string
}

func (o l2E53_3Outcome) agrees() bool { return o.restoreErr == nil && o.stored == o.traversal }

func (o l2E53_3Outcome) String() string {
	if o.restoreErr != nil {
		return fmt.Sprintf("restoreErr=%q", o.restoreErr.Error())
	}
	return fmt.Sprintf("stored Held/After=%s/%s traversal=%s/%s", o.stored[0], o.stored[1], o.traversal[0], o.traversal[1])
}

func l2E53_3Open(t *testing.T, async bool) *nornicdb.DB {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Memory.EmbeddingEnabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Database.AsyncWritesEnabled = async
	config.Database.StrictDurability = false
	// Buffered writes stay buffered until flushed explicitly.
	config.Database.AsyncFlushInterval = time.Hour
	db, err := nornicdb.Open(t.TempDir(), config)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

// l2E53_3Flush calls FlushWriteBehind on the first engine of the database's
// storage chain that offers it (none when writes are synchronous).
func l2E53_3Flush(t *testing.T, db *nornicdb.DB) {
	t.Helper()
	engine := db.GetStorage()
	for engine != nil {
		if drainer, ok := engine.(interface{ FlushWriteBehind(context.Context) error }); ok {
			if err := drainer.FlushWriteBehind(context.Background()); err != nil {
				t.Fatalf("FlushWriteBehind: %v", err)
			}
			return
		}
		unwrapper, ok := engine.(storage.EngineUnwrapper)
		if !ok {
			return
		}
		inner := unwrapper.GetInnerEngine()
		if inner == engine {
			return
		}
		engine = inner
	}
}

func l2E53_3Exec(t *testing.T, db *nornicdb.DB, query string) *nornicdb.CypherResult {
	t.Helper()
	result, err := db.ExecuteCypher(context.Background(), query, nil)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return result
}

func l2E53_3Scalar(t *testing.T, db *nornicdb.DB, query string) string {
	t.Helper()
	result := l2E53_3Exec(t, db, query)
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
		t.Fatalf("%s: rows %v", query, result.Rows)
	}
	return fmt.Sprint(result.Rows[0][0])
}

// l2E53_3Backup writes the JSON backup db.Backup writes for an engine without
// native backup.
func l2E53_3Backup(t *testing.T, db *nornicdb.DB, path string) {
	t.Helper()
	ctx := context.Background()
	nodes, err := storage.CollectStoredNodes(ctx, db.GetStorage())
	if err != nil {
		t.Fatalf("collect nodes: %v", err)
	}
	edges, err := storage.CollectStoredEdges(ctx, db.GetStorage())
	if err != nil {
		t.Fatalf("collect edges: %v", err)
	}
	data, err := valueconvert.PropertyJSON.MarshalIndent(map[string]interface{}{
		"version":    "2.0",
		"created_at": time.Now().Format(time.RFC3339),
		"nodes":      nodes,
		"edges":      edges,
	}, "", "  ")
	if err != nil {
		t.Fatalf("marshal backup: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write backup: %v", err)
	}
}

func l2E53_3CountStore(t *testing.T, db *nornicdb.DB) [2]int64 {
	t.Helper()
	engine, ok := storage.UnwrapEngine(db.GetBaseStorageForManager()).(*storage.BadgerEngine)
	if !ok {
		return [2]int64{-1, -1}
	}
	var counts [2]int64
	for i, label := range []string{"Held", "After"} {
		count, err := engine.EdgeCountByEndLabel(context.Background(), label, "OTHER")
		if err != nil {
			t.Fatalf("EdgeCountByEndLabel(%s): %v", label, err)
		}
		counts[i] = count
	}
	return counts
}

// l2E53_3Run is one run of the entry's seven steps on a fresh database.
func l2E53_3Run(t *testing.T, async bool) l2E53_3Outcome {
	t.Helper()
	db := l2E53_3Open(t, async)
	defer func() { _ = db.Close() }()
	var outcome l2E53_3Outcome
	l2E53_3Exec(t, db, "CREATE (:Probe {id:'x'}), (:After {id:'y'})")
	l2E53_3Flush(t, db)
	path := filepath.Join(t.TempDir(), "backup.json")
	l2E53_3Backup(t, db, path)
	l2E53_3Exec(t, db, "MATCH (y {id:'y'}) REMOVE y:After SET y:Held")
	l2E53_3Flush(t, db)
	// Acknowledged, and (with asynchronous writes) still in the buffer.
	l2E53_3Exec(t, db, "MATCH (x {id:'x'}), (y {id:'y'}) CREATE (x)-[:OTHER]->(y)")
	outcome.before = l2E53_3CountStore(t, db)
	if err := db.Restore(context.Background(), path); err != nil {
		outcome.restoreErr = err
		return outcome
	}
	outcome.afterRestore = l2E53_3CountStore(t, db)
	l2E53_3Flush(t, db)
	outcome.afterFlush = l2E53_3CountStore(t, db)
	for i, label := range []string{"Held", "After"} {
		outcome.stored[i] = l2E53_3Scalar(t, db, "MATCH ()-[r:OTHER]->(:"+label+") RETURN count(r)")
		outcome.traversal[i] = l2E53_3Scalar(t, db, "MATCH (a)-[r:OTHER]->(b) WHERE b:"+label+" RETURN size(collect(r))")
	}
	outcome.labels = l2E53_3Scalar(t, db, "MATCH (y {id:'y'}) RETURN labels(y)")
	return outcome
}

func l2E53_3Series(t *testing.T, async bool, runs int) (agree, miscount, restoreErrs int, lines []string) {
	t.Helper()
	for run := 1; run <= runs; run++ {
		outcome := l2E53_3Run(t, async)
		switch {
		case outcome.restoreErr != nil:
			restoreErrs++
		case outcome.agrees():
			agree++
		default:
			miscount++
		}
		lines = append(lines, fmt.Sprintf("run%d{%s}", run, outcome))
	}
	return agree, miscount, restoreErrs, lines
}

func TestL2_e53_3_Repro(t *testing.T) {
	agree, miscount, restoreErrs, lines := l2E53_3Series(t, true, l2E53_3Runs)
	figures := fmt.Sprintf("runs=%d agree=%d miscount=%d restoreErrors=%d %s", l2E53_3Runs, agree, miscount, restoreErrs, strings.Join(lines, " "))
	// Defect: in any run the stored count by end label disagrees with the
	// traversal after the restore. A failed restore is a separate symptom of
	// the entry (3 of 20 runs on 2dbc67e96): it is reported in the figures,
	// and if no run reaches the comparison the result is inconclusive.
	if miscount > 0 {
		t.Logf("L2RESULT E53.3 DEFECT %s", figures)
		t.Errorf("stored relationship count by end label disagrees with the traversal after a JSON restore with a buffered relationship: %s", figures)
		return
	}
	if agree == 0 {
		t.Logf("L2RESULT E53.3 INCONCLUSIVE %s", figures)
		t.Fatalf("no run reached the count comparison: %s", figures)
	}
	t.Logf("L2RESULT E53.3 OK %s", figures)
}

// The control: the same steps with synchronous writes (no write-behind
// buffer, the relationship is committed before the restore). Every run must
// agree.
func TestL2_e53_3_Control(t *testing.T) {
	const runs = 5
	agree, miscount, restoreErrs, lines := l2E53_3Series(t, false, runs)
	figures := fmt.Sprintf("runs=%d agree=%d miscount=%d restoreErrors=%d %s", runs, agree, miscount, restoreErrs, strings.Join(lines, " "))
	if agree != runs {
		t.Fatalf("synchronous control did not agree in every run: %s", figures)
	}
	t.Logf("L2RESULT E53.3 CONTROL OK %s", figures)
}

// The cause: the entry names no line. This diagnostic follows the derived
// count store by end label (BadgerEngine.EdgeCountByEndLabel,
// badger_edge_type_count.go:823 at the pin) through the run: before the
// restore (the relationship buffered), after the restore's relabel, and after
// the flush that applies the buffered relationship, beside the node's final
// labels. A miscount whose count store still credits Held after the flush
// shows the buffered relationship's end-label delta was computed from the
// end node's labels before the restore relabelled it, and never moved.
func TestL2_e53_3_Cause(t *testing.T) {
	for run := 1; run <= l2E53_3Runs; run++ {
		outcome := l2E53_3Run(t, true)
		t.Logf("L2CAUSE E53.3 run%d %s countStore(Held,After) before=%v afterRestore=%v afterFlush=%v labels(y)=%s agrees=%v",
			run, outcome, outcome.before, outcome.afterRestore, outcome.afterFlush, outcome.labels, outcome.agrees())
	}
}
