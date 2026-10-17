package cypher

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// restrictedSearchCorpus is a library where hidden items rank ahead of
// visible ones everywhere a restricted search could let them: hidden terms
// sort ahead in the prefix lexicon ("prefixaa…" before "prefixzzz"), hidden
// documents share the visible ones' terms (BM25 statistics), and hidden
// vectors lie nearer the query (vector ranks, and so reciprocal ranks).
// Documents v* are in collection A, h* in collection B; a relationship is
// visible when both its ends are, so rx (v1 to h1) is hidden.
var restrictedSearchCorpus = struct {
	documents     map[string]string
	vectors       map[string][]float32
	relationships map[string][3]string // id -> start, end, reason
	hidden        map[string]bool
}{
	documents: map[string]string{
		"v1": "alpha prefixzzz household budget",
		"v2": "beta alpha household",
		"v3": "gamma river household",
		"h1": "alpha alpha prefixaaa",
		"h2": "alpha beta prefixaab household",
		"h3": "alpha household household",
	},
	vectors: map[string][]float32{
		"v1": {0.8, 0.6, 0}, "v2": {0.6, 0.8, 0}, "v3": {0, 1, 0},
		"h1": {1, 0, 0}, "h2": {0.99, 0.14, 0}, "h3": {0.97, 0, 0.24},
		"rv1": {0.7, 0.7, 0.1}, "rv2": {0.2, 0.9, 0.3}, "rv3": {0, 0.6, 0.8},
		"rh1": {1, 0, 0}, "rh2": {0.98, 0.1, 0.1}, "rx": {0.99, 0.05, 0},
	},
	relationships: map[string][3]string{
		"rv1": {"v1", "v2", "alpha prefixzzz bridge household"},
		"rv2": {"v2", "v3", "beta river household"},
		"rv3": {"v1", "v3", "gamma span"},
		"rh1": {"h1", "h2", "alpha alpha prefixaaa bridge"},
		"rh2": {"h2", "h3", "alpha prefixaab bridge bridge household"},
		"rx":  {"v1", "h1", "alpha alpha household prefixaac"},
	},
	hidden: map[string]bool{"h1": true, "h2": true, "h3": true, "rh1": true, "rh2": true, "rx": true, "B": true},
}

// restrictedSearchLibrary is the corpus as database lib with a search
// service: every item, or with visibleOnly the items a login limited to
// collection A sees (the reference its restricted searches must equal).
func restrictedSearchLibrary(t *testing.T, visibleOnly bool) (*StorageExecutor, storage.Engine) {
	t.Helper()
	ctx := context.Background()
	inner := newTestMemoryEngine(t)
	lib := storage.NewNamespacedEngine(inner, "lib")
	corpus := restrictedSearchCorpus
	vector := func(id string) []interface{} {
		out := make([]interface{}, 0, 3)
		for _, v := range corpus.vectors[id] {
			out = append(out, float64(v))
		}
		return out
	}
	for _, collection := range []string{"A", "B"} {
		if visibleOnly && corpus.hidden[collection] {
			continue
		}
		_, err := lib.CreateNode(&storage.Node{ID: storage.NodeID("c" + collection), Labels: []string{"Collection"},
			Properties: map[string]any{"id": collection}})
		require.NoError(t, err)
	}
	nodes := 0
	for id, text := range corpus.documents {
		if visibleOnly && corpus.hidden[id] {
			continue
		}
		_, err := lib.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Document"},
			Properties:      map[string]any{"id": id, "content": text, "emb": vector(id)},
			ChunkEmbeddings: [][]float32{corpus.vectors[id]}})
		require.NoError(t, err)
		collection := "cA"
		if strings.HasPrefix(id, "h") {
			collection = "cB"
		}
		require.NoError(t, lib.CreateEdge(&storage.Edge{ID: storage.EdgeID("m-" + id), StartNode: storage.NodeID(id),
			EndNode: storage.NodeID(collection), Type: "MEMBER_OF"}))
		nodes++
	}
	relationships := 0
	for id, edge := range corpus.relationships {
		if visibleOnly && corpus.hidden[id] {
			continue
		}
		require.NoError(t, lib.CreateEdge(&storage.Edge{ID: storage.EdgeID(id), StartNode: storage.NodeID(edge[0]),
			EndNode: storage.NodeID(edge[1]), Type: "LINKS",
			Properties: map[string]any{"id": id, "reason": edge[2], "features": vector(id)}}))
		relationships++
	}

	exec := NewStorageExecutor(lib)
	svc := search.NewServiceWithDimensions(lib, 3)
	t.Cleanup(func() { _ = svc.Close() })
	svc.EnableRelationshipSearch(func(_ context.Context, edge *storage.Edge) (search.RelationshipEmbedding, error) {
		id, _ := edge.Properties["id"].(string)
		if corpus.vectors[id] == nil {
			return search.RelationshipEmbedding{}, nil
		}
		return search.RelationshipEmbedding{Vectors: [][]float32{corpus.vectors[id]}}, nil
	})
	svc.StartRelationshipIndex()
	exec.SetSearchService(svc)
	for _, statement := range []string{
		"CREATE VECTOR INDEX doc_emb FOR (n:Document) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}",
		"CREATE VECTOR INDEX link_emb FOR ()-[r:LINKS]-() ON (r.features) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}",
		"CREATE FULLTEXT INDEX doc_text FOR (n:Document) ON EACH [n.content]",
		"CREATE FULLTEXT INDEX link_text FOR ()-[r:LINKS]-() ON EACH [r.reason]",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	require.NoError(t, svc.BuildIndexes(ctx))
	require.Eventually(t, func() bool {
		return svc.IsReady() && svc.CountPropertyVectorEntries("emb") == nodes &&
			svc.HasRelationshipVectorEntries("LINKS", "features") && svc.RelationshipEmbeddingCount() == relationships
	}, 10*time.Second, 20*time.Millisecond, "the indexes hold the corpus")
	return exec, lib
}

// restrictedSearchRows is a result's rows with floats rounded, so a
// restricted search and its reference compare by identity, score, rank and
// order.
func restrictedSearchRows(result *ExecuteResult) []string {
	out := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		cells := make([]string, 0, len(row))
		for _, cell := range row {
			switch v := cell.(type) {
			case float64:
				cells = append(cells, fmt.Sprint(math.Round(v*1e9)/1e9))
			case float32:
				cells = append(cells, fmt.Sprint(math.Round(float64(v)*1e9)/1e9))
			default:
				cells = append(cells, fmt.Sprint(v))
			}
		}
		out = append(out, strings.Join(cells, " "))
	}
	return out
}

// Every search consumer a restricted login reaches through Cypher returns,
// on every BM25 engine (V1, V2 exact, V2 bounded prefix) and in every mode it
// serves (keyword, prefix keyword, vector, hybrid), exactly what the same
// statement returns on a library holding only what the login sees: the same
// items with the same scores and ranks in the same order, at every top-K.
// The hidden items rank first in every population a search builds, so one
// built before visibility applies shows up as a difference.
func TestRestrictedCypherSearchEqualsVisibleOnlyLibrary(t *testing.T) {
	engines := map[string][2]string{ // name -> BM25 engine, prefix expansions
		"v1":                {"v1", "0"},
		"v2 exact":          {"v2", "0"},
		"v2 bounded prefix": {"v2", "1"},
	}
	embedding := []interface{}{1.0, 0.0, 0.0}
	modes := map[string]map[string]interface{}{
		"keyword":        {"query": "alpha prefix household"},
		"prefix keyword": {"query": "prefix"},
		// The query is required; no document holds this word.
		"vector": {"query": "qqqnomatch", "embedding": embedding},
		"hybrid": {"query": "alpha prefix household", "embedding": embedding},
	}
	retrieve := map[string]string{
		"db.retrieve":              "CALL db.retrieve($req) YIELD node, score, rrf_score, vector_rank, bm25_rank RETURN node.id, score, rrf_score, vector_rank, bm25_rank",
		"db.rretrieve":             "CALL db.rretrieve($req) YIELD node, score, rrf_score, vector_rank, bm25_rank RETURN node.id, score, rrf_score, vector_rank, bm25_rank",
		"db.retrieveRelationships": "CALL db.retrieveRelationships($req) YIELD relationship, score, rrf_score, vector_rank, bm25_rank RETURN relationship.id, score, rrf_score, vector_rank, bm25_rank",
		"db.retrieveGraph":         "CALL db.retrieveGraph($req) YIELD kind, node, relationship, score, rrf_score, vector_rank, bm25_rank RETURN kind, coalesce(node.id, relationship.id), score, rrf_score, vector_rank, bm25_rank",
		"db.rretrieveGraph":        "CALL db.rretrieveGraph($req) YIELD kind, node, relationship, score, rrf_score, vector_rank, bm25_rank RETURN kind, coalesce(node.id, relationship.id), score, rrf_score, vector_rank, bm25_rank",
	}
	// The index procedures and the MATCH fast paths rank one population each.
	vectorOnly := map[string]string{
		"db.index.vector.queryNodes":           "CALL db.index.vector.queryNodes('doc_emb', %d, $embedding) YIELD node, score RETURN node.id, score",
		"db.index.vector.queryRelationships":   "CALL db.index.vector.queryRelationships('link_emb', %d, $embedding) YIELD relationship, score RETURN relationship.id, score",
		"MATCH node vector.similarity":         "MATCH (n:Document) RETURN n.id, vector.similarity.cosine(n.emb, $embedding) AS score ORDER BY score DESC LIMIT %d",
		"MATCH relationship vector.similarity": "MATCH ()-[r:LINKS]->() RETURN r.id, vector.similarity.cosine(r.features, $embedding) AS score ORDER BY score DESC LIMIT %d",
	}
	keywordOnly := map[string]string{
		"db.index.fulltext.queryNodes":         "CALL db.index.fulltext.queryNodes('doc_text', $query, {limit: %d}) YIELD node, score RETURN node.id, score",
		"db.index.fulltext.queryRelationships": "CALL db.index.fulltext.queryRelationships('link_text', $query, {limit: %d}) YIELD relationship, score RETURN relationship.id, score",
	}
	for engineName, engine := range engines {
		t.Run(engineName, func(t *testing.T) {
			t.Setenv(search.EnvSearchBM25Engine, engine[0])
			t.Setenv("NORNICDB_BM25_PREFIX_MAX_EXPANSIONS", engine[1])
			t.Setenv("NORNICDB_BM25_PREFIX_MIN_LEN", "3")
			full, _ := restrictedSearchLibrary(t, false)
			reference, _ := restrictedSearchLibrary(t, true)
			system := storage.NewNamespacedEngine(newTestMemoryEngine(t), "system")
			ctx := context.Background()
			require.NoError(t, auth.NewRoleStore(system).CreateRole(ctx, "agent_a"))
			for _, statement := range []string{
				`CREATE ACCESS SCOPE collections ON GRAPH lib
				  MEMBERSHIP (d:Document)-[:MEMBER_OF]->(c:Collection) KEY d.id, c.id
				  DERIVE (x:Collection) FROM COLLECTION x.id`,
				"GRANT MATCH {*} ON GRAPH lib FOR (n:Document) WHERE (n)-[:MEMBER_OF]->(:Collection {id: 'A'}) TO agent_a",
			} {
				_, err := NewStorageExecutor(system).Execute(ctx, statement, nil)
				require.NoError(t, err, statement)
			}
			agent := WithPrincipalAccess(ctx, "lib", PrincipalAccess{Visibility: func(database string) (*GraphVisibility, error) {
				return ResolveGraphVisibility(context.Background(), system, []string{"agent_a"}, database)
			}})
			check := func(name, statement string, params map[string]interface{}) {
				t.Helper()
				want, err := reference.Execute(ctx, statement, params)
				require.NoError(t, err, name)
				got, err := full.Execute(agent, statement, params)
				require.NoError(t, err, name)
				require.Equal(t, restrictedSearchRows(want), restrictedSearchRows(got), name)
				for _, row := range got.Rows {
					for _, cell := range row {
						if id, ok := cell.(string); ok {
							require.False(t, restrictedSearchCorpus.hidden[id], "%s returned hidden %s", name, id)
						}
					}
				}
			}
			for _, limit := range []int{1, 3, 10} {
				for consumer, statement := range retrieve {
					for modeName, mode := range modes {
						req := map[string]interface{}{"limit": int64(limit), "matches": false}
						for key, value := range mode {
							req[key] = value
						}
						check(fmt.Sprintf("%s/%s/limit %d", consumer, modeName, limit), statement, map[string]interface{}{"req": req})
					}
				}
				for consumer, statement := range vectorOnly {
					check(fmt.Sprintf("%s/limit %d", consumer, limit), fmt.Sprintf(statement, limit), map[string]interface{}{"embedding": embedding})
				}
				for consumer, statement := range keywordOnly {
					for _, query := range []string{"alpha household", "prefix*"} {
						check(fmt.Sprintf("%s/%s/limit %d", consumer, query, limit), fmt.Sprintf(statement, limit), map[string]interface{}{"query": query})
					}
				}
			}
		})
	}
}
