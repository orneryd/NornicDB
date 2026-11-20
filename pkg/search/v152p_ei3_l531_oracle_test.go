package search

// Batch-3 probe L531 (research only): the original reproduction,
// TestNOR386RetirementOrderMatchesTheSerialOracle at 55a68d403
// (pkg/search/vector_retirement_order_test.go), reduced to its drop-free
// orders (the L531 defect: no DROP involved). The same random generator and
// seeds; every drop-free order's final indexes (property vectors, named
// embeddings, chunk vectors, fulltext text) are compared with the serial
// oracle in which each refresh applies at its write. Same file on every build;
// adapters in zz_ei3_l531_build_test.go.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

var ei3ROVectors = [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {0.6, 0.8, 0}}

type ei3ROOp struct {
	kind   string
	node   int
	vector int
	pick   int
}

func (op ei3ROOp) String() string { return fmt.Sprintf("%s(n%d,v%d,k%d)", op.kind, op.node, op.vector, op.pick) }

func ei3ROOps(r *rand.Rand, n int) []ei3ROOp {
	kinds := []string{"write", "write", "write", "drain", "drain", "drop", "payload", "rebuild", "persist", "overlap"}
	ops := make([]ei3ROOp, 0, n)
	for i := 0; i < n; i++ {
		ops = append(ops, ei3ROOp{kind: kinds[r.Intn(len(kinds))], node: r.Intn(2), vector: r.Intn(len(ei3ROVectors)), pick: r.Intn(8)})
	}
	return ops
}

func ei3RONode(id string, vector int) *storage.Node {
	return &storage.Node{
		ID:              storage.NodeID(id),
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"text": fmt.Sprintf("term%d", vector), "p": ei3ROVectors[vector]},
		NamedEmbeddings: map[string][]float32{"q": ei3ROVectors[(vector+1)%len(ei3ROVectors)]},
	}
}

func ei3ROState(svc *Service) string {
	svc.indexMu.RLock()
	defer svc.indexMu.RUnlock()
	lookup := svc.getVectorLookup()
	var lines []string
	vector := func(key string) string {
		v, ok := lookup(key)
		if !ok {
			return "missing"
		}
		return fmt.Sprintf("%.3f", v)
	}
	for id, props := range svc.nodePropVector {
		for name, key := range props {
			lines = append(lines, fmt.Sprintf("prop %s %s %s", id, name, vector(key)))
		}
	}
	for id, named := range svc.nodeNamedVector {
		for name, key := range named {
			lines = append(lines, fmt.Sprintf("named %s %s %s", id, name, vector(key)))
		}
	}
	for id, chunks := range svc.nodeChunkVectors {
		for _, key := range chunks {
			lines = append(lines, fmt.Sprintf("chunk %s %s", id, vector(key)))
		}
	}
	for _, id := range []string{"nornic:n0", "nornic:n1"} {
		if index := svc.fulltext(); index != nil {
			if text, ok := index.GetDocument(id); ok {
				lines = append(lines, "text "+id+" "+text)
			}
		}
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func ei3RORun(t *testing.T, ops []ei3ROOp, lazy, serial bool) string {
	t.Helper()
	ctx := context.Background()
	engine := ei3ROEngine(t)
	svc := buildWriteService(t, engine, t.TempDir(), "v2", nil)
	svc.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { _ = svc.Close() })
	for i := range 2 {
		require.NoError(t, ei3ROCreate(ctx, engine, ei3RONode(fmt.Sprintf("nornic:n%d", i), i)))
	}
	if lazy {
		require.NoError(t, ei3ROLazyWarm(ctx, svc))
	} else {
		require.NoError(t, svc.BuildIndexes(ctx))
	}
	var pending []NodeMutation
	drain := func(pick int) {
		m := pending[pick%len(pending)]
		pending = append(pending[:pick%len(pending)], pending[pick%len(pending)+1:]...)
		require.NoError(t, ei3ROApply(ctx, svc, m))
	}
	for _, op := range ops {
		id := storage.NodeID(fmt.Sprintf("nornic:n%d", op.node))
		switch op.kind {
		case "write":
			require.NoError(t, ei3ROUpdate(ctx, engine, ei3RONode(string(id), op.vector)))
			if serial {
				require.NoError(t, ei3RORefresh(ctx, svc, id))
			} else {
				pending = append(pending, svc.ReserveNodeMutation(id))
			}
		case "drain":
			if !serial && len(pending) > 0 {
				drain(op.pick)
			}
		case "payload":
			stored, err := ei3ROGet(ctx, engine, id)
			require.NoError(t, err)
			require.NoError(t, svc.IndexNode(stored))
		case "rebuild":
			require.NoError(t, svc.BuildIndexes(ctx))
		case "persist":
			svc.PersistIndexesToDisk()
		}
	}
	for len(pending) > 0 {
		drain(0)
	}
	return ei3ROState(svc)
}

func TestEI3L531SerialOracleDropFree(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	t.Setenv("NORNICDB_SEARCH_RESULT_CACHE_ENABLED", "false")
	differ, total := 0, 0
	check := func(t *testing.T, name string, ops []ei3ROOp, lazy bool) {
		serial := ei3RORun(t, ops, lazy, true)
		interleaved := ei3RORun(t, ops, lazy, false)
		total++
		if serial != interleaved {
			differ++
			t.Logf("EI3 L531 %s lazy=%t DIFFERS from the serial oracle; calls: %v\nserial:\n%s\ninterleaved:\n%s", name, lazy, ops, serial, interleaved)
		} else {
			t.Logf("EI3 L531 %s lazy=%t equal", name, lazy)
		}
	}
	// The narrowed schedule from the 8 Nov record (two writes pending,
	// rebuild, newer applied, rebuild, older applied), both build modes.
	narrowed := []ei3ROOp{{kind: "write", node: 0, vector: 2}, {kind: "write", node: 0, vector: 3}, {kind: "rebuild"}, {kind: "drain", pick: 1}, {kind: "rebuild"}, {kind: "drain", pick: 0}}
	check(t, "narrowed", narrowed, false)
	check(t, "narrowed", narrowed, true)
	for seed := int64(1); seed <= 400; seed++ {
		ops := ei3ROOps(rand.New(rand.NewSource(seed)), 12)
		dropFree := true
		for _, op := range ops {
			if op.kind == "drop" || op.kind == "overlap" {
				dropFree = false
			}
		}
		if !dropFree {
			continue
		}
		check(t, fmt.Sprintf("seed=%d", seed), ops, seed%3 == 0)
	}
	t.Logf("EI3 L531 drop-free orders=%d differing=%d", total, differ)
	if differ > 0 {
		t.Errorf("EI3 L531 EXISTS: %d of %d drop-free orders end with indexes that differ from the serial oracle", differ, total)
	}
}
