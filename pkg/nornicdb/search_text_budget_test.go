package nornicdb

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

// A text the BM25 memory budget refuses is refused on its own: the node's
// vectors stay served by every vector index on the property, and an index
// created again serves the node (NOR-386 and NOR-404, round 9 of #1753).

const textBudgetCreateDocs = "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}"

// openTextBudgetDB opens a database whose search service has a 128 KiB BM25
// memory budget.
func openTextBudgetDB(t *testing.T) (*DB, *search.Service, *dbSearchService) {
	t.Helper()
	ctx := context.Background()
	config := DefaultConfig()
	config.Memory.EmbeddingEnabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.EmbeddingDimensions = 3
	config.Memory.SearchBM25Enabled = true
	config.Memory.SearchVectorEnabled = true
	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	db.SetDbSearchOptionsResolver(func(name string) search.ServiceOptions {
		options := search.DefaultServiceOptions(name)
		options.BM25MemoryMaxBytes = 128 * 1024
		return options
	})
	db.ResetSearchService(db.defaultDatabaseName())
	svc, err := db.EnsureSearchIndexes(ctx, db.defaultDatabaseName(), nil, SearchIndexBuildWait)
	require.NoError(t, err)
	db.searchServicesMu.RLock()
	entry := db.searchServices[db.defaultDatabaseName()]
	db.searchServicesMu.RUnlock()
	require.NotNil(t, entry)
	return db, svc, entry
}

func textBudgetRun(t *testing.T, db *DB, query string, params map[string]interface{}) [][]interface{} {
	t.Helper()
	result, err := db.ExecuteCypher(t.Context(), query, params)
	require.NoError(t, err, query)
	return result.Rows
}

// textBudgetServed is what docs serves for [1, 0, 0], by node id.
func textBudgetServed(t *testing.T, db *DB) map[interface{}]float64 {
	t.Helper()
	served := map[interface{}]float64{}
	for _, row := range textBudgetRun(t, db, "CALL db.index.vector.queryNodes('docs', 10, [1.0, 0.0, 0.0]) YIELD node, score RETURN node.id, score", nil) {
		score, ok := row[1].(float64)
		require.True(t, ok, "score %v", row[1])
		served[row[0]] = math.Round(score*1000) / 1000
	}
	return served
}

// With the BM25 budget reached, an ordinary text write the budget refuses
// leaves the node's vector served by the existing index, with no DROP
// involved, as on the release head.
func TestNOR386TextBeyondBudgetKeepsVectorServed(t *testing.T) {
	db, _, entry := openTextBudgetDB(t)
	waitDBWorkIdle(t, db)
	// Each storage event re-arms the flush with this delay: refreshes stay
	// queued until the test applies them, as the flush would later.
	entry.pendingFlushDelay = time.Hour
	textBudgetRun(t, db, textBudgetCreateDocs, nil)
	textBudgetRun(t, db, "CREATE (:Doc {id: 'a', emb: [1.0, 0.0, 0.0], text: 'short'}), (:Doc {id: 'b', emb: [0.0, 1.0, 0.0], text: 'short'})", nil)
	require.Eventually(t, entry.hasPending, 5*time.Second, time.Millisecond)
	applyQueued(t, entry)
	require.Equal(t, map[interface{}]float64{"a": 1, "b": 0.5}, textBudgetServed(t, db))

	textBudgetRun(t, db, "MATCH (n:Doc {id: 'a'}) SET n.text = $text", map[string]interface{}{"text": strings.Repeat("payload ", 32768)})
	require.Eventually(t, entry.hasPending, 5*time.Second, time.Millisecond, "the write's refresh was not queued")
	applyQueued(t, entry)
	require.Equal(t, map[interface{}]float64{"a": 1, "b": 0.5}, textBudgetServed(t, db), "a text beyond the BM25 budget must not take the node's vector out of the index")
}

// Review round 9 of #1753 (reviewer probe): ordinary writes and DDL, with the
// production debounce, no storage faults, held builds or manual reservations.
// After a text write the BM25 budget refuses, a DROP and CREATE of the vector
// index serves both stored matching nodes. The reviewer's probe waited for
// the refresh by counting the node's vector out of the search service (the
// round-7 withdrawal this round removes); it now waits for the refusal itself
// (the node's replaced text withdrawn), on every head alike, and requires the
// recreation to succeed.
func TestReview1753R9PublicRecreateAfterCapacityRefusal(t *testing.T) {
	ctx := context.Background()
	db, svc, entry := openTextBudgetDB(t)
	textBudgetRun(t, db, textBudgetCreateDocs, nil)
	textBudgetRun(t, db, "CREATE (:Doc {id:'a',emb:[1.0,0.0,0.0],text:'short'}),(:Doc {id:'b',emb:[0.0,1.0,0.0],text:'short'})", nil)
	require.Eventually(t, func() bool { return svc.CountPropertyVectorEntries("emb") == 2 }, 5*time.Second, 10*time.Millisecond)
	textHits := func(term string) int {
		resp, err := svc.Search(ctx, term, nil, &search.SearchOptions{Limit: 10})
		require.NoError(t, err)
		return len(resp.Results)
	}
	require.Eventually(t, func() bool { return textHits("short") == 2 }, 5*time.Second, 10*time.Millisecond)
	textBudgetRun(t, db, "MATCH (n:Doc {id:'a'}) SET n.text=$text", map[string]interface{}{"text": strings.Repeat("payload ", 32768)})
	require.Eventually(t, func() bool {
		entry.pendingFlushMu.Lock()
		running := entry.pendingFlushRunning
		entry.pendingFlushMu.Unlock()
		return !entry.hasPending() && !running && textHits("short") == 1
	}, 10*time.Second, 10*time.Millisecond, "let the ordinary debounced refresh finish its real capacity refusal")
	textBudgetRun(t, db, "DROP INDEX docs", nil)
	textBudgetRun(t, db, textBudgetCreateDocs, nil)
	shown := textBudgetRun(t, db, "SHOW VECTOR INDEXES YIELD name, state RETURN name, state", nil)
	served := textBudgetServed(t, db)
	t.Logf("public recreation: state=%v rows=%v", shown, served)
	require.Equal(t, [][]interface{}{{"docs", "ONLINE"}}, shown)
	require.Equal(t, map[interface{}]float64{"a": 1, "b": 0.5}, served, fmt.Sprintf("a recreated index must serve both stored matching vectors; served %v", served))
}
