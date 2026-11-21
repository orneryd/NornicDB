package bolt

// Lane L6b (closeout): Q01 E53.2, the exact loss step of the ranked
// continuation (150 qualifying notes, the stream ends after 125).
// Same store, Voyage stand-in and Bolt route as rcaRankedContinuation
// (rca_a_search_continuation_test.go). Added: a reranker wrapper that records
// the candidate IDs each provider call receives and the IDs it returns, and
// page sizes 20, 25 and 50. For every note never delivered it logs which
// provider call received it and whether the provider returned it. If every
// lost note was returned by the provider in an earlier window and the loss
// count follows the page size, the rows are dropped when a retained window is
// rebuilt for a later step (rerank_continuation.go:153-167, rows outside this
// step's coverage, which is capped at opts.Limit at :113-118, are skipped
// while their decision stays recorded), not by retrieval or the provider.

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

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/voyage"
	"github.com/stretchr/testify/require"
)

type zzL6bCall struct {
	in  []string
	out []string
}

type zzL6bRecordingReranker struct {
	inner search.Reranker
	mu    *sync.Mutex
	calls *[]zzL6bCall
}

func (r zzL6bRecordingReranker) Name() string                        { return r.inner.Name() }
func (r zzL6bRecordingReranker) Enabled() bool                       { return r.inner.Enabled() }
func (r zzL6bRecordingReranker) IsAvailable(ctx context.Context) bool { return r.inner.IsAvailable(ctx) }
func (r zzL6bRecordingReranker) Rerank(ctx context.Context, query string, candidates []search.RerankCandidate) ([]search.RerankResult, error) {
	out, err := r.inner.Rerank(ctx, query, candidates)
	call := zzL6bCall{}
	for _, c := range candidates {
		call.in = append(call.in, c.ID)
	}
	for _, o := range out {
		call.out = append(call.out, o.ID)
	}
	r.mu.Lock()
	*r.calls = append(*r.calls, call)
	r.mu.Unlock()
	return out, err
}

func TestL6bContinuationLossStep(t *testing.T) {
	for _, pageSize := range []int{20, 25, 50} {
		t.Run(fmt.Sprintf("n=%d", pageSize), func(t *testing.T) { zzL6bContinuation(t, pageSize) })
	}
}

func zzL6bContinuation(t *testing.T, pageSize int) {
	const n = 150
	t.Setenv("NORNICDB_SEARCH_RERANK_MAX_DOCUMENT_CHARS", "1800")
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query     string   `json:"query"`
			Documents []string `json:"documents"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
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
		json.NewEncoder(w).Encode(map[string]any{"data": data, "usage": map[string]any{"total_tokens": 1}})
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
	var mu sync.Mutex
	var calls []zzL6bCall
	svc.SetReranker(zzL6bRecordingReranker{inner: rerank, mu: &mu, calls: &calls})
	port := serveBoltIntegrationServer(t, New(&Config{Port: 0, MaxConnections: 10, ReadBufferSize: 8192, WriteBufferSize: 8192}, &pdv2Exec{store: store, svc: svc}))
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	defer driver.Close(context.Background())

	ret := " YIELD page RETURN page.qid AS qid, page.has_more AS more, page.completion AS completion, page.ranked_count AS ranked, page.ranked_pool_exhausted AS rpe, [r IN page.results | r.node.id] AS ids"
	first := fmt.Sprintf("CALL db.rretrieve({query:$query, types:['PDSearchSource','PDSearchable'], n:%d, failClosed:true, minRRFScore:0})", pageSize) + ret
	next := fmt.Sprintf("CALL db.rretrieve({query:$query, types:['PDSearchSource','PDSearchable'], n:%d, qid:$qid, failClosed:true, minRRFScore:0})", pageSize) + ret
	seen := map[string]int{}
	var qid any
	for page := 0; page < 40; page++ {
		q, params := first, map[string]any{"query": "bandicoot"}
		if qid != nil {
			q, params = next, map[string]any{"query": "bandicoot", "qid": qid}
		}
		rows, err := pdv2Run(driver, q, params, false)
		if err != nil || len(rows) == 0 {
			t.Logf("L6B_Q01 n=%d page=%d err=%v rows=%d", pageSize, page, err, len(rows))
			break
		}
		m := rows[0].AsMap()
		ids, _ := m["ids"].([]any)
		for _, id := range ids {
			if _, ok := seen[fmt.Sprint(id)]; !ok {
				seen[fmt.Sprint(id)] = page
			}
		}
		mu.Lock()
		callCount := len(calls)
		mu.Unlock()
		t.Logf("L6B_Q01 n=%d page=%d ids=%d distinct=%d more=%v completion=%v ranked=%v rpe=%v provider_calls=%d", pageSize, page, len(ids), len(seen), m["more"], m["completion"], m["ranked"], m["rpe"], callCount)
		qid = m["qid"]
		if more, _ := m["more"].(bool); !more {
			break
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for i, c := range calls {
		t.Logf("L6B_Q01 n=%d provider_call=%d in=%d out=%d", pageSize, i, len(c.in), len(c.out))
	}
	lost := []string{}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("synthetic-note-%d", i)
		if _, ok := seen[id]; !ok {
			lost = append(lost, id)
		}
	}
	inCall, returnedBy := map[string][]int{}, map[string][]int{}
	for ci, c := range calls {
		for _, id := range c.in {
			inCall[id] = append(inCall[id], ci)
		}
		for _, id := range c.out {
			returnedBy[id] = append(returnedBy[id], ci)
		}
	}
	allReturned := len(lost) > 0
	for _, id := range lost {
		if len(returnedBy[id]) == 0 {
			allReturned = false
		}
	}
	sample := lost
	if len(sample) > 8 {
		sample = sample[:8]
	}
	for _, id := range sample {
		t.Logf("L6B_Q01 n=%d lost=%s provider_input_calls=%v provider_returned_calls=%v", pageSize, id, inCall[id], returnedBy[id])
	}
	t.Logf("L6B_Q01_SUMMARY n=%d delivered=%d lost=%d all_lost_returned_by_provider=%v", pageSize, len(seen), len(lost), allReturned)
	if len(lost) > 0 && allReturned {
		t.Errorf("CAUSE Q01 (n=%d): %d notes the provider ranked and returned are never delivered; they are dropped after the provider answered, in the continuation rerank stage", pageSize, len(lost))
	}
}
