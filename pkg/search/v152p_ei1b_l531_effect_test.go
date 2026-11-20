package search

// Untracked probe (batch 1b, packet G items 3 and 4 / entry 44): the same
// orders as TestEI3L531SerialOracleDropFree (copied oracle file and this build's
// adapters), keeping the service and the store so that after the last
// operation the test can (item 3) search by vector and (both items) read the
// stored node nornic:n0.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func zzEI1bRun(t *testing.T, ops []ei3ROOp, lazy, serial bool) (*Service, storage.Engine) {
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
	return svc, engine
}

func zzEI1bReport(t *testing.T, label string, svc *Service, engine storage.Engine, search bool) {
	ctx := context.Background()
	stored, err := ei3ROGet(ctx, engine, "nornic:n0")
	if err != nil {
		t.Logf("%s: stored nornic:n0: error %v", label, err)
	} else {
		t.Logf("%s: stored nornic:n0: text=%v p=%v q=%v", label, stored.Properties["text"], stored.Properties["p"], stored.NamedEmbeddings["q"])
	}
	t.Logf("%s: index state:\n%s", label, ei3ROState(svc))
	if !search {
		return
	}
	for _, v := range [][]float32{{0.6, 0.8, 0}, {1, 0, 0}, {0, 1, 0}} {
		resp, err := svc.Search(ctx, "", v, &SearchOptions{Limit: 10})
		if err != nil {
			t.Logf("%s: Search(vector %v): error %v", label, v, err)
		} else {
			var hits []string
			for _, r := range resp.Results {
				hits = append(hits, fmt.Sprintf("%s score=%.4f sim=%.4f", r.ID, r.Score, r.Similarity))
			}
			t.Logf("%s: Search(vector %v): %d results %v", label, v, len(resp.Results), hits)
		}
		for _, prop := range []string{"p", "q"} {
			hits, err := svc.VectorQueryNodes(ctx, v, VectorQuerySpec{Label: "Doc", Property: prop, Similarity: "cosine", Limit: 10})
			t.Logf("%s: VectorQueryNodes(Doc.%s, %v): %v err=%v", label, prop, v, hits, err)
		}
	}
}

func TestZZEI1bL531Effect(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	t.Setenv("NORNICDB_SEARCH_RESULT_CACHE_ENABLED", "false")
	narrowed := []ei3ROOp{{kind: "write", node: 0, vector: 2}, {kind: "write", node: 0, vector: 3}, {kind: "rebuild"}, {kind: "drain", pick: 1}, {kind: "rebuild"}, {kind: "drain", pick: 0}}
	for _, lazy := range []bool{false, true} {
		for _, serial := range []bool{true, false} {
			svc, engine := zzEI1bRun(t, narrowed, lazy, serial)
			zzEI1bReport(t, fmt.Sprintf("item3 narrowed lazy=%t serial=%t", lazy, serial), svc, engine, true)
		}
	}
	ops := ei3ROOps(rand.New(rand.NewSource(27)), 12)
	t.Logf("item4 seed=27 ops=%v", ops)
	for _, serial := range []bool{true, false} {
		svc, engine := zzEI1bRun(t, ops, true, serial)
		zzEI1bReport(t, fmt.Sprintf("item4 seed=27 lazy=true serial=%t", serial), svc, engine, false)
	}
}
