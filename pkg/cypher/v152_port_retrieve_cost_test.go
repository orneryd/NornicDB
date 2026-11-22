package cypher

import (
 "context"
 "crypto/sha256"
 "encoding/binary"
 "encoding/json"
 "fmt"
 "github.com/orneryd/nornicdb/pkg/search"
 "github.com/orneryd/nornicdb/pkg/storage"
 "math/rand"
 "os"
 "reflect"
 "regexp"
 "strconv"
 "strings"
 "sync/atomic"
 "testing"
)

type nor407ABEmbedder struct{ dims int }

func (e nor407ABEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	r := rand.New(rand.NewSource(int64(len(text))))
	out := make([]float32, e.dims)
	for i := range out {
		out[i] = r.Float32()
	}
	return out, nil
}

func (e nor407ABEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	return []string{text}, nil
}

type nor407ABReranker struct{}

func (nor407ABReranker) Name() string                     { return "stub" }
func (nor407ABReranker) Enabled() bool                    { return true }
func (nor407ABReranker) IsAvailable(context.Context) bool { return true }
func (nor407ABReranker) Rerank(_ context.Context, _ string, candidates []search.RerankCandidate) ([]search.RerankResult, error) {
	out := make([]search.RerankResult, 0, len(candidates))
	for i, c := range candidates {
		out = append(out, search.RerankResult{ID: c.ID, Content: c.Content, OriginalRank: i + 1, NewRank: i + 1, BiScore: c.Score, CrossScore: c.Score, FinalScore: c.Score})
	}
	return out, nil
}

func nor407ABEnvInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

// nor407ABCreate calls CreateNode with or without a context, as the build has it.
func nor407ABCreate(store storage.Engine, node *storage.Node) error {
	method := reflect.ValueOf(store).MethodByName("CreateNode")
	args := []reflect.Value{reflect.ValueOf(node)}
	if method.Type().NumIn() == 2 {
		args = []reflect.Value{reflect.ValueOf(context.Background()), reflect.ValueOf(node)}
	}
	out := method.Call(args)
	if err, ok := out[len(out)-1].Interface().(error); ok && err != nil {
		return err
	}
	return nil
}

// nor407ABService calls search.NewService(engine) or (engine, dimensions, options).
func nor407ABService(store storage.Engine, dims int) *search.Service {
	ctor := reflect.ValueOf(search.NewService)
	args := []reflect.Value{reflect.ValueOf(store)}
	if ctor.Type().NumIn() == 3 {
		args = append(args, reflect.ValueOf(dims), reflect.Zero(ctor.Type().In(2)))
	} else if dims != 1024 {
		panic("the one-argument search.NewService has 1024 dimensions")
	}
	return ctor.Call(args)[0].Interface().(*search.Service)
}

var nor407ABToken = regexp.MustCompile(`"[A-Za-z0-9_-]{60,}"`)

var nor407ABExpiry = regexp.MustCompile(`"expires_at":"[^"]*"`)

// nor407ABRows is a number that stands for the rows of a result: equal sums on
// both arms mean equal rows. A continuation token and its expiry are masked.
func nor407ABRows(result *ExecuteResult) uint64 {
	if result == nil {
		return 1
	}
	data, err := json.Marshal(result.Rows)
	if err != nil {
		data = []byte(fmt.Sprintf("%v", result.Rows))
	}
	data = nor407ABToken.ReplaceAll(data, []byte(`"<token>"`))
	data = nor407ABExpiry.ReplaceAll(data, []byte(`"expires_at":"<time>"`))
	sum := sha256.Sum256(data)
	return uint64(binary.BigEndian.Uint32(sum[:4]) % 1000003)
}

var nor407ABCalls atomic.Int64

func nor407ABParams() map[string]interface{} {
	queries := []string{"invoice payment", "tenant lease contract", "insurance claim", "company registration", "bank statement balance", "letter account", "policy office record"}
	i := nor407ABCalls.Add(1) - 1
	q := fmt.Sprintf("%s %d", queries[int(i)%len(queries)], i)
	return map[string]interface{}{"q": q, "query": q, "qid": nil, "page_size": int64(3), "min_similarity": 0.0, "rerank_min_score": 0.0, "history": false, "anchors": []interface{}{}}
}

func nor407ABExecutor(b *testing.B) *StorageExecutor {
	b.Helper()
	dims := 1024
	sources := nor407ABEnvInt("RR_SOURCES", 356)
	total := nor407ABEnvInt("RR_NODES", 12500)
	ctx := context.Background()
	engine, err := storage.NewBadgerEngine(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = engine.Close() })
	store := storage.NewNamespacedEngine(engine, "verify")
	executor := NewStorageExecutor(store)
	r := rand.New(rand.NewSource(7))
	vector := func() []float32 {
		v := make([]float32, dims)
		for i := range v {
			v[i] = r.Float32()
		}
		return v
	}
	words := strings.Fields("company registration office record invoice payment contract tenant lease insurance claim policy letter statement balance account")
	text := func(n int) string {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = words[r.Intn(len(words))]
		}
		return strings.Join(parts, " ")
	}
	body := strings.Repeat("payload stored on an unrelated node ", 170)
	for i := 0; i < total-2*sources; i++ {
		if err := nor407ABCreate(store, &storage.Node{ID: storage.NodeID(fmt.Sprintf("o%d", i)), Labels: []string{"Other"}, Properties: map[string]interface{}{"id": int64(i), "body": body}, NamedEmbeddings: map[string][]float32{"default": vector()}}); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < sources; i++ {
		id := fmt.Sprintf("s%d", i)
		doc := fmt.Sprintf("d%d", i)
		if err := nor407ABCreate(store, &storage.Node{ID: storage.NodeID(doc), Labels: []string{"PDDocument"}, Properties: map[string]interface{}{"id": doc, "current_version_id": "v" + doc}}); err != nil {
			b.Fatal(err)
		}
		if err := nor407ABCreate(store, &storage.Node{ID: storage.NodeID(id), Labels: []string{"PDSearchSource", "PDSearchable"}, Properties: map[string]interface{}{"id": id, "document_id": doc, "text": text(1000), "current": true}, NamedEmbeddings: map[string][]float32{"default": vector()}}); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := executor.Execute(ctx, "CREATE (:PDCollection {id: 'c', archived: false}), (:PDSearchSettings {id: 'search', rerank_top_k: 20})", nil); err != nil {
		b.Fatal(err)
	}
	if _, err := executor.Execute(ctx, "MATCH (d:PDDocument), (c:PDCollection) CREATE (d)-[:MEMBER_OF]->(c)", nil); err != nil {
		b.Fatal(err)
	}
	if os.Getenv("NOR407_INDEX") != "" {
		if _, err := executor.Execute(ctx, "CREATE INDEX nor407_document_id IF NOT EXISTS FOR (d:PDDocument) ON (d.id)", nil); err != nil {
			b.Fatal(err)
		}
	}
	service := nor407ABService(store, dims)
	b.Cleanup(func() { _ = service.Close() })
	if err := service.BuildIndexes(ctx); err != nil {
		b.Fatal(err)
	}
	service.SetReranker(nor407ABReranker{})
	executor.SetSearchService(service)
	executor.SetEmbedder(nor407ABEmbedder{dims: dims})
	return executor
}

func nor407ABRun(b *testing.B, executor *StorageExecutor, query string) {
	ctx := context.Background()
	var rows, sum uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := executor.Execute(ctx, query, nor407ABParams())
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		rows += uint64(len(result.Rows))
		sum += nor407ABRows(result)
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(rows)/float64(b.N), "rows/op")
	b.ReportMetric(float64(sum)/float64(b.N), "rowsum/op")
}

func BenchmarkV152PortNOR407Shapes(b *testing.B) {
	executor := nor407ABExecutor(b)
	where := "coalesce(node.current, true) AND EXISTS { MATCH (:PDDocument {id: node.document_id})-[:MEMBER_OF]->(:PDCollection) }"
	for _, shape := range []struct{ name, query string }{
		{"retrieve3where", "CALL db.retrieve({query: $q, n: 3, types: ['PDSearchSource', 'PDSearchable'], where: '" + where + "'})"},
		{"retrieve300where", "CALL db.retrieve({query: $q, n: 300, types: ['PDSearchSource', 'PDSearchable'], where: '" + where + "'})"},
	} {
		b.Run(shape.name, func(b *testing.B) { nor407ABRun(b, executor, shape.query) })
	}
}
