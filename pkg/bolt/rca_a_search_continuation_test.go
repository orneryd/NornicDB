package bolt

// Root-cause diagnostic (campaign v1.5.2, lane A, group G-search-continuation, member E53.2).
// Test-side only. Same store, Voyage stand-in and Bolt route as
// TestV152PortRankedContinuation/windowRemoved, but the statement is reduced
// to the db.rretrieve continuation itself and RETURNs the page's own
// completion fields, so the decision that ends the stream
// (continuationExecution.observe, search_execution.go:67-96) is visible.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/voyage"
	"github.com/stretchr/testify/require"
)

func TestRCARankedContinuationCompletion(t *testing.T) {
	const n = 150
	t.Setenv("NORNICDB_SEARCH_RERANK_MAX_DOCUMENT_CHARS", "1800")
	var mu sync.Mutex
	var calls []int
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query     string   `json:"query"`
			Documents []string `json:"documents"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		total := 0
		for _, s := range req.Documents {
			total += utf8.RuneCountInString(s) + utf8.RuneCountInString(req.Query)
		}
		mu.Lock()
		calls = append(calls, len(req.Documents))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if total > 600000 {
			w.WriteHeader(400)
			fmt.Fprint(w, `{"detail":"synthetic request token budget >600000"}`)
			return
		}
		data := []map[string]any{}
		for i, doc := range req.Documents {
			score := 0.5
			var k int
			if at := strings.Index(doc, "rank="); at >= 0 {
				fmt.Sscanf(doc[at:], "rank=%d", &k)
				score = float64(k) / float64(n+1)
			}
			data = append(data, map[string]any{"index": i, "relevance_score": score})
		}
		sort.Slice(data, func(i, j int) bool { return data[i]["relevance_score"].(float64) > data[j]["relevance_score"].(float64) })
		json.NewEncoder(w).Encode(map[string]any{"data": data, "usage": map[string]any{"total_tokens": total}})
	}))
	defer peer.Close()
	rerank, err := voyage.NewReranker(&voyage.RerankerConfig{Enabled: true, APIURL: peer.URL, APIKey: "synthetic", Model: "rerank-3", TopK: 1000, Timeout: 10 * time.Second, DisableTruncation: true})
	require.NoError(t, err)
	base := storage.NewMemoryEngine()
	defer base.Close()
	store := storage.NewNamespacedEngine(base, "nornic")
	for i := 1; i <= n; i++ {
		text := fmt.Sprintf("bandicoot rank=%d ", i) + strings.Repeat("wombat ", i) + strings.Repeat("z ", 4000)
		pdv2Store(t, store, "CreateNode", &storage.Node{ID: storage.NodeID(fmt.Sprintf("synthetic-note-%d", i)), Labels: []string{"PDGraphNode", "PDSearchable"}, Properties: map[string]any{"id": fmt.Sprintf("synthetic-note-%d", i), "kind": "note", "origin": "user", "revision": int64(1), "current": true, "search_text": text}, ChunkEmbeddings: [][]float32{{1, float32(n-i) / float32(n)}}})
	}
	fn := reflect.ValueOf(search.NewService)
	args := []reflect.Value{reflect.ValueOf(store)}
	if fn.Type().NumIn() == 3 {
		args = append(args, reflect.ValueOf(2), reflect.Zero(fn.Type().In(2)))
	}
	svc := fn.Call(args)[0].Interface().(*search.Service)
	svc.SetFulltextProperties([]string{"text", "search_text"})
	require.NoError(t, svc.BuildIndexes(context.Background()))
	svc.SetReranker(rerank)
	port := serveBoltIntegrationServer(t, New(&Config{Port: 0, MaxConnections: 10, ReadBufferSize: 8192, WriteBufferSize: 8192}, &pdv2Exec{store: store, svc: svc}))
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	defer driver.Close(context.Background())

	where := "(node:PDSearchSource OR $history OR coalesce(node.current, true)) AND (NOT node:PDSummary OR coalesce(node.current, false)) AND NOT coalesce(node.search_hidden, false) AND (NOT node:PDSummary OR node.search_hidden = false)"
	ret := " YIELD page RETURN page.qid AS qid, page.has_more AS more, page.returned AS returned, page.discovered AS discovered, page.total AS total, page.completion AS completion, page.mode AS mode, page.ranked_count AS ranked, page.ranked_pool_exhausted AS rpe, page.collection_exhausted AS ce, page.eligible_count AS eligible, page.fallback_reason AS fb, [r IN page.results | r.node.id] AS ids"
	first := "CALL db.rretrieve({query:$query, types:['PDSearchSource','PDSearchable'], where:$where, n:20, failClosed:true, minRRFScore:0})" + ret
	next := "CALL db.rretrieve({query:$query, types:['PDSearchSource','PDSearchable'], n:20, qid:$qid, failClosed:true, minRRFScore:0})" + ret
	seen := map[string]bool{}
	var qid any
	for page := 0; page < 30; page++ {
		q, params := first, map[string]any{"query": "bandicoot", "where": where, "history": false}
		if qid != nil {
			q, params = next, map[string]any{"query": "bandicoot", "qid": qid}
		}
		rows, err := pdv2Run(driver, q, params, false)
		if err != nil || len(rows) == 0 {
			t.Logf("RCA_CONT page=%d err=%v rows=%d", page, err, len(rows))
			break
		}
		m := rows[0].AsMap()
		ids, _ := m["ids"].([]any)
		for _, id := range ids {
			seen[fmt.Sprint(id)] = true
		}
		mu.Lock()
		c := append([]int(nil), calls...)
		mu.Unlock()
		t.Logf("RCA_CONT page=%d ids=%d distinct=%d more=%v returned=%v discovered=%v total=%v completion=%v mode=%v ranked=%v rankedPoolExhausted=%v collectionExhausted=%v eligible=%v fallback=%v rerankDocsPerCall=%v",
			page, len(ids), len(seen), m["more"], m["returned"], m["discovered"], m["total"], m["completion"], m["mode"], m["ranked"], m["rpe"], m["ce"], m["eligible"], m["fb"], c)
		qid = m["qid"]
		if more, _ := m["more"].(bool); !more {
			break
		}
	}
	t.Logf("RCA_CONT_END distinct=%d of %d", len(seen), n)
}
