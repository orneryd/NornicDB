package ir

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type collectingDocumentSink struct {
	documents []Document
	err       error
}

type fixedRetriever struct {
	results map[string][]RunResult
}

func (r fixedRetriever) Retrieve(_ context.Context, query string, _ int) ([]RunResult, error) {
	return r.results[query], nil
}

func (s *collectingDocumentSink) StoreDocument(_ context.Context, document Document) error {
	if s.err != nil {
		return s.err
	}
	s.documents = append(s.documents, document)
	return nil
}

func TestReadBEIRArtifactsAndManifest(t *testing.T) {
	var documents []Document
	err := ReadDocuments(strings.NewReader("{\"_id\":\"d1\",\"title\":\"Title\",\"text\":\"Body\"}\n"), func(document Document) error {
		documents = append(documents, document)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, "d1", documents[0].ID)

	queries, err := ReadQueries(strings.NewReader("{\"_id\":\"q2\",\"text\":\"second\"}\n{\"_id\":\"q1\",\"text\":\"first\"}\n"))
	require.NoError(t, err)
	manifest, err := NewQueryManifest("fiqa", "test", queries, 1000, 42)
	require.NoError(t, err)
	assert.Equal(t, []string{"q1", "q2"}, manifest.QueryIDs)
	assert.Len(t, manifest.SHA256, 64)
}

func TestFilterQueriesWithQrelsExcludesUnjudgedQueries(t *testing.T) {
	queries := []Query{{ID: "judged", Text: "evaluated"}, {ID: "unjudged", Text: "not evaluated"}}
	filtered := FilterQueriesWithQrels(queries, Qrels{"judged": {"doc": 1}})
	assert.Equal(t, []Query{{ID: "judged", Text: "evaluated"}}, filtered)
}

func TestReadBEIRArtifactsRejectInvalidIDs(t *testing.T) {
	err := ReadDocuments(strings.NewReader("{\"_id\":\"d1\",\"text\":\"one\"}\n{\"_id\":\"d1\",\"text\":\"two\"}\n"), func(Document) error { return nil })
	require.Error(t, err)
	_, err = ReadQueries(strings.NewReader("{\"_id\":\"q1\",\"text\":\"query\"}\n{\"_id\":\"q1\",\"text\":\"query\"}\n"))
	require.Error(t, err)
}

func TestIngestDocuments(t *testing.T) {
	sink := &collectingDocumentSink{}
	stats, err := IngestDocuments(context.Background(), strings.NewReader("{\"_id\":\"d1\",\"text\":\"one\"}\n{\"_id\":\"d2\",\"text\":\"two\"}\n"), sink)
	require.NoError(t, err)
	assert.EqualValues(t, 2, stats.Documents)
	assert.Equal(t, []string{"d1", "d2"}, []string{sink.documents[0].ID, sink.documents[1].ID})

	_, err = IngestDocuments(context.Background(), strings.NewReader("{\"_id\":\"d1\",\"text\":\"one\"}\n"), &collectingDocumentSink{err: errors.New("write failed")})
	require.Error(t, err)
}

func TestNornicDocumentSinkPreservesBEIRID(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "bench")
	index := search.NewServiceWithDimensions(engine, 2)
	sink := &NornicDocumentSink{Engine: engine, Indexer: index}
	require.NoError(t, sink.StoreDocument(context.Background(), Document{ID: "doc-1", Title: "Title", Text: "retrieval text"}))
	node, err := engine.GetNode("doc-1")
	require.NoError(t, err)
	assert.Equal(t, "doc-1", node.Properties["beir_id"])
	assert.Equal(t, "retrieval text", node.Properties["text"])
}

func TestRunResultsFromSearchResults(t *testing.T) {
	results, err := RunResultsFromSearchResults([]search.SearchResult{{ID: "bench:physical", Score: 0.8, Properties: map[string]any{"beir_id": "d1"}}})
	require.NoError(t, err)
	assert.Equal(t, []RunResult{{DocumentID: "d1", Score: 0.8}}, results)
	_, err = RunResultsFromSearchResults([]search.SearchResult{{ID: "bench:physical"}})
	require.Error(t, err)
}

func TestNornicRetrieverRequiresService(t *testing.T) {
	_, err := (&NornicRetriever{}).Retrieve(context.Background(), "query", 10)
	require.Error(t, err)
}

func TestRunManifest(t *testing.T) {
	manifest := QueryManifest{QueryIDs: []string{"q1", "q2"}}
	queries := []Query{{ID: "q1", Text: "first"}, {ID: "q2", Text: "second"}}
	var output strings.Builder
	stats, err := RunManifest(context.Background(), manifest, queries, fixedRetriever{results: map[string][]RunResult{
		"first":  {{DocumentID: "d1", Score: 0.9}, {DocumentID: "d2", Score: 0.8}},
		"second": {{DocumentID: "d3", Score: 0.7}, {DocumentID: "d4", Score: 0.6}},
	}}, 1, "rrf", &output)
	require.NoError(t, err)
	assert.EqualValues(t, 2, stats.Queries)
	run, err := ReadRun(strings.NewReader(output.String()))
	require.NoError(t, err)
	assert.Equal(t, []string{"d1"}, run["q1"])
	assert.Equal(t, []string{"d3"}, run["q2"])
}

func TestRunManifestRejectsMissingQuery(t *testing.T) {
	_, err := RunManifest(context.Background(), QueryManifest{QueryIDs: []string{"missing"}}, nil, fixedRetriever{}, 100, "rrf", &strings.Builder{})
	require.Error(t, err)
}
