//go:build closeout_l2_e38_10

package cypher

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// E38.10: CALL db.labels() and CALL db.propertyKeys() allocate about the
// whole 32 MiB of chunk vectors the store holds in auto-commit, and 284-287
// MiB inside an explicit transaction, although neither returns anything of
// a vector.
//
// The store is the committed helper newNOR413Graph(t, 16, 256)
// (nor413_call_query_node_reads_test.go, same package, no build tag): 49
// nodes, 16 relationships, 4,113 chunk vectors of 2,048 values (32.13 MiB),
// as on 2dbc67e96. The measure is the committed nor413Allocated: least
// TotalAlloc of 3 runs after a warm-up, BEGIN/ROLLBACK outside it.
//
// Threshold: 8 MiB per run, in either mode. The entry's defect is 32.30 MiB
// (auto-commit) and 284-287 MiB (explicit); a listing that reads no vectors
// allocates what MATCH (n) RETURN count(n) does on this store (0.12-0.20
// MiB). 8 MiB is a quarter of the smallest defect figure and 40 times the
// fixed one. The parser is the process's (NORNICDB_PARSER).

const l2E38_10Bound = 8 << 20

var l2E38_10Procedures = []string{
	"CALL db.labels() YIELD label RETURN label",
	"CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey",
}

var l2E38_10Controls = []string{
	"MATCH (n) RETURN count(n) AS c",
	"MATCH (n) RETURN DISTINCT labels(n) AS l",
}

// l2E38_10Measure returns the bytes one run of statement allocates
// auto-commit and between BEGIN and ROLLBACK.
func l2E38_10Measure(t *testing.T, exec *StorageExecutor, statement string) (auto, explicit uint64) {
	t.Helper()
	ctx := WithTransactionControl(context.Background())
	run := func() {
		res, err := exec.Execute(ctx, statement, nil)
		if err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
		if len(res.Rows) == 0 {
			t.Fatalf("%s returned no row", statement)
		}
	}
	control := func(query string) func() {
		return func() {
			if _, err := exec.Execute(ctx, query, nil); err != nil {
				t.Fatalf("%s: %v", query, err)
			}
		}
	}
	none := func() {}
	auto = nor413Allocated(t, none, run, none)
	explicit = nor413Allocated(t, control("BEGIN"), run, control("ROLLBACK"))
	return auto, explicit
}

func l2E38_10MiB(b uint64) float64 { return float64(b) / (1 << 20) }

func TestL2_e38_10_Repro(t *testing.T) {
	g := newNOR413Graph(t, 16, 256)
	t.Logf("store: %d vector bytes (%.2f MiB)", g.vectorBytes, l2E38_10MiB(g.vectorBytes))
	exec := NewStorageExecutor(g.store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	defect := false
	var decisive []string
	for _, statement := range l2E38_10Procedures {
		auto, explicit := l2E38_10Measure(t, exec, statement)
		t.Logf("%s: auto-commit %.2f MiB, explicit %.2f MiB", statement, l2E38_10MiB(auto), l2E38_10MiB(explicit))
		decisive = append(decisive, fmt.Sprintf("%q=%.2f/%.2fMiB", statement, l2E38_10MiB(auto), l2E38_10MiB(explicit)))
		if auto > l2E38_10Bound {
			defect = true
			t.Errorf("%s allocates %.2f MiB auto-commit (bound %.0f MiB) on a store of %.2f MiB of vectors", statement, l2E38_10MiB(auto), l2E38_10MiB(l2E38_10Bound), l2E38_10MiB(g.vectorBytes))
		}
		if explicit > l2E38_10Bound {
			defect = true
			t.Errorf("%s allocates %.2f MiB in an explicit transaction (bound %.0f MiB)", statement, l2E38_10MiB(explicit), l2E38_10MiB(l2E38_10Bound))
		}
	}
	verdict := "OK"
	if defect {
		verdict = "DEFECT"
	}
	t.Logf("L2RESULT E38.10 %s vectors=%.2fMiB %s", verdict, l2E38_10MiB(g.vectorBytes), strings.Join(decisive, " "))
}

// TestL2_e38_10_Control: the entry's controls on the same store stay far
// under the bound in both modes.
func TestL2_e38_10_Control(t *testing.T) {
	g := newNOR413Graph(t, 16, 256)
	exec := NewStorageExecutor(g.store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	for _, statement := range l2E38_10Controls {
		auto, explicit := l2E38_10Measure(t, exec, statement)
		t.Logf("control %s: auto-commit %.2f MiB, explicit %.2f MiB", statement, l2E38_10MiB(auto), l2E38_10MiB(explicit))
		if auto > l2E38_10Bound || explicit > l2E38_10Bound {
			t.Errorf("control %s: %.2f / %.2f MiB", statement, l2E38_10MiB(auto), l2E38_10MiB(explicit))
		}
	}
}

// TestL2_e38_10_Cause measures the population reads the token listing makes
// (readGraphTokenKeys, pkg/cypher/graph_token_reads.go:35-75 at the pin:
// AllNodes at :42, AllEdges at :60) directly: NamespacedEngine.AllNodes
// (auto-commit) and BadgerTransaction.AllNodes (the explicit transaction's
// read through transactionStorageWrapper.AllNodes), and logs their bytes
// against the store's vector bytes. If AllNodes alone allocates the
// statement's figure, the listing's cost is the full-node population read.
func TestL2_e38_10_Cause(t *testing.T) {
	g := newNOR413Graph(t, 16, 256)
	ctx := context.Background()
	measure := func(name string, read func() (int, error)) {
		if _, err := read(); err != nil {
			t.Logf("L2CAUSE E38.10 %s warm-up error: %v", name, err)
			return
		}
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		n, err := read()
		runtime.ReadMemStats(&after)
		t.Logf("L2CAUSE E38.10 %s: %d entities, err=%v, %.2f MiB allocated (store vectors %.2f MiB)",
			name, n, err, l2E38_10MiB(after.TotalAlloc-before.TotalAlloc), l2E38_10MiB(g.vectorBytes))
	}
	measure("NamespacedEngine.AllNodes", func() (int, error) {
		nodes, err := g.store.AllNodes(ctx)
		chunks := 0
		for _, n := range nodes {
			chunks += len(n.ChunkEmbeddings)
		}
		t.Logf("L2CAUSE E38.10 NamespacedEngine.AllNodes returned %d chunk vectors", chunks)
		return len(nodes), err
	})
	measure("NamespacedEngine.AllEdges", func() (int, error) {
		edges, err := g.store.AllEdges(ctx)
		return len(edges), err
	})
	measure("BadgerTransaction.AllNodes", func() (int, error) {
		tx, err := g.engine.BeginTransaction(ctx, storage.OrdinaryBegin)
		if err != nil {
			return 0, err
		}
		defer func() { _ = tx.Rollback() }()
		if err := tx.SetNamespace("test"); err != nil {
			return 0, err
		}
		nodes, err := tx.AllNodes(ctx)
		chunks := 0
		for _, n := range nodes {
			chunks += len(n.ChunkEmbeddings)
		}
		t.Logf("L2CAUSE E38.10 BadgerTransaction.AllNodes returned %d chunk vectors", chunks)
		return len(nodes), err
	})
}
