package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const n15290PageQuery = `CALL db.rretrieve($request) YIELD page
RETURN page.qid AS qid, page.has_more AS has_more,
       [r IN page.results | r.node.id] AS ids`

const (
	n15290Stored  = "NOT coalesce(node.search_hidden, false)"
	n15290Direct  = "NOT (EXISTS { MATCH (:PDDocument {id: node.document_id})-[:MEMBER_OF]->(:PDCollection) } AND NOT EXISTS { MATCH (:PDDocument {id: node.document_id})-[:MEMBER_OF]->(k:PDCollection) WHERE NOT coalesce(k.archived, false) })"
	n15290Inverse = "NOT (EXISTS { MATCH (:PDCollection)<-[:MEMBER_OF]-(:PDDocument {id: node.document_id}) } AND NOT EXISTS { MATCH (k:PDCollection)<-[:MEMBER_OF]-(:PDDocument {id: node.document_id}) WHERE NOT coalesce(k.archived, false) })"
)

type n15290Page struct {
	qid  string
	more bool
	ids  []string
}

type n15290Stats struct {
	median float64
	p95    float64
	max    float64
}

// TestN15290RetrievalFilterCostProbe reproduces the PD eligibility shapes over
// Bolt. It prints a cold initial page separately, then 30 warm first-page and
// continuation-page samples per fixture, predicate and page size. The complete
// stream is also drained once per case to verify its returned IDs.
func TestN15290RetrievalFilterCostProbe(t *testing.T) {
	ctx := context.Background()
	queryVector := make([]float64, 2048)
	queryVector[0] = 1
	srv, _ := setupTestServer(t)
	database := srv.dbManager.DefaultDatabaseName()
	store, err := srv.dbManager.GetStorage(database)
	require.NoError(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	boltServer := bolt.NewWithDatabaseManager(bolt.DefaultConfig(), searchNodeWireExecutor{server: srv}, srv.GetDatabaseManager())
	serveDone := make(chan error, 1)
	go func() { serveDone <- boltServer.Serve(listener) }()
	t.Cleanup(func() { _ = boltServer.Close(); <-serveDone })
	driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
	t.Cleanup(func() { require.NoError(t, session.Close(ctx)) })

	for _, fixture := range []struct {
		documents int
		degree    int
	}{{400, 300}, {2000, 200}} {
		name := fmt.Sprintf("d%d-degree%d", fixture.documents, fixture.degree)
		t.Run(name, func(t *testing.T) {
			want := n15290Library(t, ctx, store, fixture.documents, fixture.degree)
			searchService, err := srv.db.GetOrCreateSearchService(database, store)
			require.NoError(t, err)
			require.NoError(t, searchService.BuildIndexes(ctx))
			require.Eventually(t, func() bool {
				result, runErr := session.Run(ctx, "CALL db.retrieve({query:'quokka',embedding:$vector,types:['PDSearchSource'],minSimilarity:-1,minRRFScore:0,limit:100000}) YIELD node,vector_rank,bm25_rank RETURN count(CASE WHEN vector_rank > 0 AND bm25_rank > 0 THEN 1 END) AS both", map[string]any{"vector": queryVector})
				if runErr != nil {
					return false
				}
				record, recordErr := result.Single(ctx)
				if recordErr != nil {
					return false
				}
				both, _ := record.Get("both")
				return both == int64(fixture.documents)
			}, 15*time.Minute, 500*time.Millisecond, "source indexing must complete before timing")
			// The public retrieval call above qualifies both source indexes.
			for _, rule := range []struct{ name, where string }{
				{"stored", n15290Stored}, {"direct", n15290Direct}, {"direct_inverse", n15290Inverse},
			} {
				for _, pageSize := range []int{20, 200} {
					t.Run(fmt.Sprintf("%s/n%d", rule.name, pageSize), func(t *testing.T) {
						request := func(qid string) map[string]any {
							if qid != "" {
								return map[string]any{"qid": qid, "n": pageSize}
							}
							return map[string]any{"query": "quokka", "embedding": queryVector, "types": []string{"PDSearchSource"}, "where": rule.where, "failClosed": true, "minRRFScore": 0, "n": pageSize}
						}
						call := func(req map[string]any) (n15290Page, time.Duration) {
							started := time.Now()
							result, runErr := session.Run(ctx, n15290PageQuery, map[string]any{"request": req})
							require.NoError(t, runErr)
							record, recordErr := result.Single(ctx)
							require.NoError(t, recordErr)
							elapsed := time.Since(started)
							idsValue, ok := record.Get("ids")
							require.True(t, ok)
							idsAny, ok := idsValue.([]any)
							require.True(t, ok, "%T", idsValue)
							page := n15290Page{}
							qidValue, _ := record.Get("qid")
							page.qid, _ = qidValue.(string)
							hasMoreValue, _ := record.Get("has_more")
							page.more, _ = hasMoreValue.(bool)
							for _, id := range idsAny {
								page.ids = append(page.ids, id.(string))
							}
							return page, elapsed
						}

						// One un-warmed search start and pull, explicitly reported
						// apart from the 30 warm samples.
						coldPage, coldFirst := call(request(""))
						for coldPage.more {
							coldPage, _ = call(request(coldPage.qid))
						}
						firstSamples := make([]time.Duration, 0, 30)
						continuationSamples := make(map[int][]time.Duration)
						for i := 0; i < 30; i++ {
							first, firstElapsed := call(request(""))
							firstSamples = append(firstSamples, firstElapsed)
							require.True(t, first.more, "warm start must have a continuation page")
							pageNumber := 2
							for first.more {
								continuation, continuationElapsed := call(request(first.qid))
								continuationSamples[pageNumber] = append(continuationSamples[pageNumber], continuationElapsed)
								require.NotEmpty(t, continuation.ids)
								first = continuation
								pageNumber++
							}
						}
						// Drain one independent stream and fingerprint all returned IDs.
						page, _ := call(request(""))
						ids := append([]string(nil), page.ids...)
						for page.more {
							require.Less(t, len(ids), len(want)+pageSize, "stream did not advance")
							page, _ = call(request(page.qid))
							ids = append(ids, page.ids...)
						}
						got := make(map[string]struct{}, len(ids))
						for _, id := range ids {
							got[id] = struct{}{}
						}
						require.Len(t, got, len(ids), "stream repeated IDs")
						require.Equal(t, len(want), len(got), "row count differs from source eligibility set")
						for id := range got {
							_, exists := want[id]
							require.True(t, exists, "unexpected id %s", id)
						}
						var continuationJSON strings.Builder
						for pageNumber := 2; pageNumber <= len(continuationSamples)+1; pageNumber++ {
							if pageNumber > 2 {
								continuationJSON.WriteByte(',')
							}
							fmt.Fprintf(&continuationJSON, "\"page_%d\":%s", pageNumber, n15290Summary(continuationSamples[pageNumber]))
						}
						fmt.Printf("N152_90_PROBE {\"fixture\":%q,\"rule\":%q,\"n\":%d,\"cold_first_ms\":%.3f,\"warm_first\":%s,\"warm_continuation\":{%s},\"rows\":%d,\"ids_sha256\":%q}\n",
							name, rule.name, pageSize, float64(coldFirst.Microseconds())/1000, n15290Summary(firstSamples), continuationJSON.String(), len(ids), n15290IDHash(ids))
					})
				}
			}
		})
	}
}

func n15290Library(t *testing.T, ctx context.Context, store storage.Engine, documents, degree int) map[string]struct{} {
	t.Helper()
	_, err := store.CreateNode(ctx, &storage.Node{ID: "c-open", Labels: []string{"PDCollection"}, Properties: map[string]any{"id": "open", "archived": false}})
	require.NoError(t, err)
	_, err = store.CreateNode(ctx, &storage.Node{ID: "c-closed", Labels: []string{"PDCollection"}, Properties: map[string]any{"id": "closed", "archived": true}})
	require.NoError(t, err)
	vector := make([]float32, 2048)
	vector[0] = 1
	want := make(map[string]struct{}, documents*2/3)
	for start := 1; start <= documents; start += 100 {
		end := min(start+99, documents)
		nodes := make([]*storage.Node, 0, (end-start+1)*3+(end-start+1)*degree)
		edges := make([]*storage.Edge, 0, (end-start+1)*(degree+3))
		for i := start; i <= end; i++ {
			d, v, s := fmt.Sprintf("d%d", i), fmt.Sprintf("v%d", i), fmt.Sprintf("s%d", i)
			hidden := i%3 == 1 && i%9 != 4
			nodes = append(nodes,
				&storage.Node{ID: storage.NodeID(d), Labels: []string{"PDGraphNode", "PDDocument"}, Properties: map[string]any{"id": d, "current_version_id": v}},
				&storage.Node{ID: storage.NodeID(v), Labels: []string{"PDVersion"}, Properties: map[string]any{"id": v, "document_id": d, "current_source_id": s}},
				&storage.Node{ID: storage.NodeID(s), Labels: []string{"PDSearchSource"}, Properties: map[string]any{"id": s, "document_id": d, "version_id": v, "text": "quokka report " + fmt.Sprint(i), "view_text": "quokka report " + fmt.Sprint(i), "search_hidden": hidden}, ChunkEmbeddings: [][]float32{vector}},
			)
			edges = append(edges, &storage.Edge{ID: storage.EdgeID("has-version-" + v), StartNode: storage.NodeID(d), EndNode: storage.NodeID(v), Type: "HAS_VERSION", Properties: map[string]any{}})
			if i%3 == 0 || i%9 == 4 {
				edges = append(edges, &storage.Edge{ID: storage.EdgeID("member-open-" + d), StartNode: storage.NodeID(d), EndNode: "c-open", Type: "MEMBER_OF", Properties: map[string]any{}})
			}
			if i%3 == 1 {
				edges = append(edges, &storage.Edge{ID: storage.EdgeID("member-closed-" + d), StartNode: storage.NodeID(d), EndNode: "c-closed", Type: "MEMBER_OF", Properties: map[string]any{}})
			}
			if !hidden {
				want[s] = struct{}{}
			}
			for j := 1; j <= degree; j++ {
				x := fmt.Sprintf("x%d-%d", i, j)
				nodes = append(nodes, &storage.Node{ID: storage.NodeID(x), Labels: []string{"PDGraphNode"}, Properties: map[string]any{"id": x}})
				edges = append(edges, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("relation-%d-%d", i, j)), StartNode: storage.NodeID(d), EndNode: storage.NodeID(x), Type: "PD_RELATION", Properties: map[string]any{"kind": "mentions"}})
			}
		}
		require.NoError(t, store.BulkCreateNodes(ctx, nodes), "fixture nodes %d-%d", start, end)
		require.NoError(t, store.BulkCreateEdges(ctx, edges), "fixture edges %d-%d", start, end)
	}
	return want
}

func n15290Summary(samples []time.Duration) string {
	values := make([]float64, len(samples))
	for i, sample := range samples {
		values[i] = float64(sample.Microseconds()) / 1000
	}
	sort.Float64s(values)
	median := (values[(len(values)-1)/2] + values[len(values)/2]) / 2
	p95 := values[int(math.Ceil(.95*float64(len(values))))-1]
	return fmt.Sprintf("{\"median_ms\":%.3f,\"p95_ms\":%.3f,\"max_ms\":%.3f,\"samples\":%d}", median, p95, values[len(values)-1], len(values))
}

func n15290IDHash(ids []string) string {
	ordered := append([]string(nil), ids...)
	sort.Strings(ordered)
	sum := sha256.Sum256([]byte(strings.Join(ordered, "\n")))
	return hex.EncodeToString(sum[:])
}
