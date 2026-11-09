// Author: #1683 implementation lane (Codex), 2026-11-01.
package cypher

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strings"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testutil/memorypressure"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fulltextMemoryLogs struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *fulltextMemoryLogs) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, record.Clone())
	h.mu.Unlock()
	return nil
}

func (h *fulltextMemoryLogs) matching(text string) []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	var result []slog.Record
	for _, record := range h.records {
		if strings.Contains(record.Message, text) {
			result = append(result, record)
		}
	}
	return result
}

// The public fixture reaches procedures, the retained Service and the real
// logger. Query-result caches are disabled; storage acquisition is observed.
func fulltextMemoryPublicFixture(t *testing.T) (*StorageExecutor, *fulltextCountingStore, *fulltextMemoryLogs, *search.Service) {
	t.Helper()
	ctx := t.Context()
	_, store := fulltextMaintainedFixture(t, 160)
	options := search.DefaultServiceOptions("fulltext_fixture")
	service := search.NewService(store, search.DefaultVectorDimensions, &options)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	logs := &fulltextMemoryLogs{}
	service.SetLogger(slog.New(fixtureowner.RawLogHandler(logs.Handle)))
	executor := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	executor.SetSearchServiceResolver("fulltext_fixture", func(string) *search.Service { return service })
	_, err := executor.Execute(ctx, "CREATE FULLTEXT INDEX link_text FOR ()-[r:LINK]-() ON EACH [r.text]", nil)
	require.NoError(t, err)
	for i := range 4 {
		require.NoError(t, store.CreateEdge(ctx, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("link-%d", i)),
			StartNode: "n000000", EndNode: "n000040", Type: "LINK", Properties: map[string]any{"id": i, "text": "hotel services"}}))
	}
	return executor, store, logs, service
}

func fulltextMemoryQueries() []string {
	return []string{
		"CALL db.index.fulltext.queryNodes('source_text','hotel') YIELD node,score RETURN node.id,score ORDER BY score DESC,node.id",
		"CALL db.index.fulltext.queryRelationships('link_text','hotel',{skip:1,limit:2}) YIELD relationship,score RETURN relationship.id,score",
	}
}

func fulltextRuntimeUsed() int64 {
	samples := []metrics.Sample{{Name: "/memory/classes/total:bytes"}, {Name: "/memory/classes/heap/released:bytes"}}
	metrics.Read(samples)
	return int64(samples[0].Value.Uint64() - samples[1].Value.Uint64())
}

func fulltextAssertTransition(t *testing.T, logs *fulltextMemoryLogs, transition string, level slog.Level) {
	t.Helper()
	records := logs.matching("Full-text retained copies " + transition + ":")
	require.Len(t, records, 2, "one transition for each node/relationship index")
	for _, record := range records {
		require.Equal(t, level, record.Level)
		require.Contains(t, record.Message, `database="fulltext_fixture"`)
		require.Contains(t, record.Message, `index="`)
		require.Regexp(t, `memory_used_bytes=[1-9][0-9]*`, record.Message)
		require.Regexp(t, `memory_limit_bytes=[1-9][0-9]*`, record.Message)
		if transition == "released" {
			require.Contains(t, record.Message, "searches on this index now read from storage")
		}
	}
}

// The legacy deployed setting and GOMEMLIMIT share this exact applied runtime
// limit. Keep live ballast so a GC cannot make the pressure premise disappear.
// This serial test deliberately changes the process limit, then restores it.
func TestFulltextRetainedMemoryRuntimePublic(t *testing.T) {
	// Establish the retained state independently of the suite's ambient limit;
	// the pressure phase below installs and exercises its own real runtime limit.
	memorypressure.WithoutRuntimePressure(t)
	executor, store, logs, service := fulltextMemoryPublicFixture(t)
	ctx := t.Context()
	queries := fulltextMemoryQueries()
	var expected [][][]any
	for _, query := range queries {
		result, err := executor.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.NotEmpty(t, result.Rows)
		expected = append(expected, result.Rows)
	}
	warm := store.scans.Load()
	for _, query := range queries {
		_, err := executor.Execute(ctx, query, nil)
		require.NoError(t, err)
	}
	require.Equal(t, warm, store.scans.Load(), "fixture must reach a maintained corpus before applying pressure")
	assert.Positive(t, service.IndexSizeBytes("bm25"), "existing index-size metrics must include the retained copies")

	previousLimit := debug.SetMemoryLimit(-1)
	previousGC := debug.SetGCPercent(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(previousLimit); debug.SetGCPercent(previousGC) })
	ballast := make([]byte, 64<<20)
	for i := 0; i < len(ballast); i += 4096 {
		ballast[i] = 1
	}
	debug.FreeOSMemory()
	limit := fulltextRuntimeUsed() * 9 / 10
	settings := config.MemoryConfig{RuntimeLimit: limit, GCPercent: -1}
	settings.ApplyRuntimeMemory()
	require.Equal(t, limit, debug.SetMemoryLimit(-1), "use the existing configuration owner")
	assert.Eventually(t, func() bool { return len(logs.matching("Full-text retained copies released:")) == 2 }, 5*time.Second, 10*time.Millisecond,
		"optional copies ignore the configured memory limit or release is silent")

	before := store.scans.Load()
	for call := range 3 {
		for i, query := range queries {
			result, err := executor.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, expected[i], result.Rows, "pressure call %d", call)
		}
	}
	assert.GreaterOrEqual(t, store.scans.Load()-before, int64(6), "released copies must read from storage on every call")
	fulltextAssertTransition(t, logs, "released", slog.LevelWarn)
	require.Zero(t, service.IndexSizeBytes("bm25"), "released copies must leave existing index-size metrics")

	runtime.KeepAlive(ballast)

	debug.SetMemoryLimit(previousLimit)
	ballast = nil
	debug.FreeOSMemory()
	for i, query := range queries {
		result, err := executor.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Equal(t, expected[i], result.Rows)
	}
	refilled := store.scans.Load()
	for _, query := range queries {
		_, err := executor.Execute(ctx, query, nil)
		require.NoError(t, err)
	}
	require.Equal(t, refilled, store.scans.Load(), "successful refill must be maintained again")
	require.Positive(t, service.IndexSizeBytes("bm25"))
	fulltextAssertTransition(t, logs, "refilled", slog.LevelInfo)
	fulltextAssertTransition(t, logs, "released", slog.LevelWarn)
}
