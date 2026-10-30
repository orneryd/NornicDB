// Author: Codex NOR-283 verification lane, 2026-10-26.
// API port author: Codex NOR-148 implementation lane,29 October2026.
// Nornic2 ruled: v2 only after TJ removed v1; all remaining assertions retained.
// Admission regression expansion: Codex NOR-283 implementation lane, 29 October 2026.
// Adapted from #1344's rejected-checkpoint matrix for the release build owner.
package cypher

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// The adapter exposes exactly one of the two node traversal contracts, without
// forwarding an optional snapshot capability that could bypass that traversal.
type nor283ScanEngine struct {
	storage.Engine
	reads atomic.Int32
}

func (e *nor283ScanEngine) AllNodes() ([]*storage.Node, error) {
	e.reads.Add(1)
	return e.Engine.AllNodes()
}

type nor283IteratorEngine struct{ *nor283ScanEngine }

func (e *nor283IteratorEngine) IterateNodes(fn func(*storage.Node) bool) error {
	e.reads.Add(1)
	return e.Engine.(search.NodeIterator).IterateNodes(fn)
}

func nor283Source(t *testing.T, engine storage.Engine, traversal string) (storage.Engine, *atomic.Int32) {
	t.Helper()
	scan := &nor283ScanEngine{Engine: engine}
	var source storage.Engine = scan
	if traversal == "iterator" {
		_, ok := engine.(search.NodeIterator)
		require.True(t, ok, "the fixture must delegate to the real storage iterator")
		source = &nor283IteratorEngine{scan}
	}
	_, iterator := source.(search.NodeIterator)
	require.Equal(t, traversal == "iterator", iterator)
	return source, &scan.reads
}

func nor283OpenStore(t *testing.T, dir string) (*storage.BadgerEngine, func()) {
	t.Helper()
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true}))
	require.NoError(t, err)
	return fixtureowner.OwnWithClose(t, engine)
}

func nor283Service(t *testing.T, source storage.Engine, dir string) (*search.Service, func()) {
	t.Helper()
	options := search.DefaultServiceOptions("")
	options.BM25MemoryMaxBytes = 1 << 20
	service := search.NewService(source, 3, &options)
	service.FixVectorDimensions()
	if dir != "" {
		service.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
		service.SetVectorIndexPath(filepath.Join(dir, "vectors"))
		service.SetHNSWIndexPath(filepath.Join(dir, "hnsw"))
		service.SetPersistenceEnabled(true)
	}
	return fixtureowner.OwnWithClose(t, service)
}

func nor283DamageCheckpoint(t *testing.T, path, kind string) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Greater(t, len(data), 2, "start with a real persisted population")
	switch kind {
	case "corrupt":
		data = []byte{0xc1} // Reserved MessagePack byte: the v2 format cannot decode it.
	case "truncated":
		data = data[:len(data)/2]
	case "wrong_version", "missing_version", "empty_version", "malformed_version", "older_version",
		"missing_terms", "missing_document", "document_count", "document_mapping", "document_length",
		"total_length", "average_length", "missing_posting", "posting_document", "posting_frequency",
		"duplicate_posting", "wrong_term", "wrong_lexicon":
		var snapshot map[string]interface{}
		payload := bytes.NewReader(data)
		require.NoError(t, msgpack.NewDecoder(payload).Decode(&snapshot))
		// Keep any writer-provided integrity trailer unchanged. Re-encoding
		// damaged content must not silently bless it with a fresh digest.
		trailer := data[len(data)-payload.Len():]
		require.Contains(t, snapshot, "v")
		nor283MutateDecodedCheckpoint(t, snapshot, kind)
		data, err = msgpack.Marshal(snapshot)
		require.NoError(t, err)
		data = append(data, trailer...)
	case "valid":
	default:
		t.Fatalf("unknown checkpoint fixture %q", kind)
	}
	require.NoError(t, os.WriteFile(path, data, 0600))
	probe := search.NewFulltextIndexV2WithAnalyzer(nil)
	probe.Index("probe", "sentinel", []string{"sentinel"}, map[string]int{"sentinel": 1}, true)
	require.NoError(t, probe.Load(path), "release Load reports recoverable rejection as nil")
	if kind == "valid" {
		require.Equal(t, 2, probe.Count(), "the control restores the actual saved population")
	} else {
		assert.Zero(t, probe.Count(), "this fixture must really be rejected by the selected backend")
	}
}

func nor283MutateDecodedCheckpoint(t *testing.T, snapshot map[string]interface{}, kind string) {
	t.Helper()
	// Work from a complete real checkpoint; each mutation retains all other
	// fields so a forced settings rebuild cannot hide bad admission.
	switch kind {
	case "wrong_version":
		snapshot["v"] = "99.0.0"
	case "missing_version":
		delete(snapshot, "v")
	case "empty_version":
		snapshot["v"] = ""
	case "malformed_version":
		snapshot["v"] = "not-semver"
	case "older_version":
		snapshot["v"] = "2.0.0"
	case "missing_terms":
		delete(snapshot, "terms")
	case "missing_document":
		delete(snapshot["docs"].(map[string]interface{}), "nornic:doc-a")
	case "document_count":
		snapshot["doc_count"] = 3
	case "document_mapping":
		snapshot["doc_id_to_num"].(map[string]interface{})["nornic:doc-a"] = uint32(1000)
	case "document_length":
		snapshot["doc_lengths"].([]interface{})[0] = uint32(1000)
	case "total_length":
		snapshot["total_doc_len"] = int64(1000)
	case "average_length":
		snapshot["avg_doc_len"] = float64(1000)
	case "wrong_lexicon":
		// Keep the array sorted and its size unchanged, so the old loader's
		// cache-order/size check cannot repair this missing prefix-search term.
		snapshot["lexicon"].([]interface{})[0] = "aaaa-not-an-indexed-term"
	default:
		terms := snapshot["terms"].(map[string]interface{})
		term := terms["orchard"].(map[string]interface{})
		postings := term["p"].([]interface{})
		require.NotEmpty(t, postings)
		switch kind {
		case "missing_posting":
			term["p"] = postings[:len(postings)-1]
		case "posting_document":
			postings[0].(map[string]interface{})["d"] = uint32(1000)
		case "posting_frequency":
			postings[0].(map[string]interface{})["t"] = uint16(500)
		case "duplicate_posting":
			term["p"] = append(postings, postings[0])
		case "wrong_term":
			delete(terms, "orchard")
			terms["not-orchard"] = term
			lexicon := snapshot["lexicon"].([]interface{})
			for i, name := range lexicon {
				if name == "orchard" {
					lexicon[i] = "not-orchard"
				}
			}
		default:
			t.Fatalf("unknown decoded corruption %q", kind)
		}
	}
}

func nor283Node(id, text string, vector []float32) *storage.Node {
	return &storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"},
		Properties: map[string]interface{}{"text": text}, NamedEmbeddings: map[string][]float32{"v": vector}}
}

func TestNOR283RejectedCheckpointReleaseMatrix(t *testing.T) {
	nor283ReleaseMatrix(t, []string{
		"corrupt", "truncated", "wrong_version", "missing_version", "empty_version", "malformed_version", "older_version",
		"missing_terms", "missing_document", "document_count", "document_mapping", "document_length",
		"total_length", "average_length", "missing_posting", "posting_document", "posting_frequency",
		"duplicate_posting", "wrong_term", "wrong_lexicon",
	})
}

func TestNOR283AcceptedCheckpointReleaseControls(t *testing.T) {
	nor283ReleaseMatrix(t, []string{"valid"})
}

func nor283ReleaseMatrix(t *testing.T, kinds []string) {
	t.Helper()
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
	for _, traversal := range []string{"iterator", "all_nodes"} {
		for _, lifecycle := range []string{"restart", "restored_copy"} {
			for _, kind := range kinds {
				for _, entry := range []string{"explicit_build", "live_before_first_read"} {
					t.Run(fmt.Sprintf("v2/%s/%s/%s/%s", traversal, lifecycle, kind, entry), func(t *testing.T) {
						nor283ReleaseCase(t, traversal, lifecycle, kind, entry)
					})
				}
			}
		}
	}
}

func nor283ReleaseCase(t *testing.T, traversal, lifecycle, kind, entry string) {
	t.Helper()
	if kind == "wrong_lexicon" {
		t.Setenv("NORNICDB_BM25_PREFIX_MAX_EXPANSIONS", "100")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir := t.TempDir()
	dataDir, indexDir := filepath.Join(dir, "data"), filepath.Join(dir, "indexes")
	engine, closeStore := nor283OpenStore(t, dataDir)
	for _, node := range []*storage.Node{
		nor283Node("nornic:doc-a", "orchard orchard orchard citrus", []float32{1, 0, 0}),
		nor283Node("nornic:doc-c", "orchard meadow", []float32{0, 0, 1}),
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}
	source, _ := nor283Source(t, engine, traversal)
	seed, closeSeed := nor283Service(t, source, indexDir)
	require.NoError(t, seed.BuildIndexes(ctx))
	seed.PersistIndexesToDisk()
	closeSeed()
	closeStore()
	for _, name := range []string{"bm25", "vectors.vec", "vectors.meta"} {
		info, err := os.Stat(filepath.Join(indexDir, name))
		require.NoError(t, err)
		require.Positive(t, info.Size(), "restore both text and vector artifacts")
	}
	if lifecycle == "restored_copy" {
		restored := filepath.Join(dir, "restored-indexes")
		require.NoError(t, os.CopyFS(restored, os.DirFS(indexDir)))
		indexDir = restored
	}
	nor283DamageCheckpoint(t, filepath.Join(indexDir, "bm25"), kind)

	// Reopen the actual on-disk database as well as the search service. No
	// source reads or result state survive in the old engine/service objects.
	engine, _ = nor283OpenStore(t, dataDir)
	source, reads := nor283Source(t, engine, traversal)
	service, _ := nor283Service(t, source, indexDir)
	var lazyErr error
	if entry == "explicit_build" {
		require.NoError(t, service.BuildIndexes(ctx))
		require.True(t, service.IsReady())
	} else {
		// Release's database owner wires the lazy build at first search, rather
		// than develop's first-mutation population owner. Use that public hook.
		service.SetLazyWarming(true, func() { lazyErr = service.BuildIndexes(ctx) })
	}
	if kind != "valid" {
		// Preserve #1344's live-before-build dimension: a live write is present
		// before first read, and rejected disk text must not strand older nodes.
		node := nor283Node("nornic:doc-b", "harbor ferry", []float32{0, 1, 0})
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, service.IndexNode(node))
	} else if entry == "live_before_first_read" {
		// The accepted control does not introduce a newer population into its
		// checkpoint; an unchanged live node still exercises this entry route.
		node, err := engine.GetNode("nornic:doc-a")
		require.NoError(t, err)
		require.NoError(t, service.IndexNode(node))
	}

	// This independent service has no checkpoint paths and must rebuild from
	// storage. Keep its traversal counter separate from the candidate's.
	freshSource, _ := nor283Source(t, engine, traversal)
	fresh, _ := nor283Service(t, freshSource, "")
	require.NoError(t, fresh.BuildIndexes(ctx))
	actualExecutor, freshExecutor := NewStorageExecutor(source), NewStorageExecutor(freshSource)
	actualExecutor.SetSearchService(service)
	freshExecutor.SetSearchService(fresh)
	queries := []string{"orchard", "citrus", "harbor"}
	if kind == "wrong_lexicon" {
		queries = append(queries, "cit") // Exercises the cached lexical vocabulary.
	}
	for _, query := range queries {
		want, err := fresh.Search(ctx, query, nil, &search.SearchOptions{Limit: 10})
		require.NoError(t, err)
		wantCount := 1
		if query == "orchard" {
			wantCount = 2
		} else if query == "harbor" && kind == "valid" {
			wantCount = 0
		}
		require.Len(t, want.Results, wantCount, "non-vacuous fresh rebuild for %s", query)
		got, err := service.Search(ctx, query, nil, &search.SearchOptions{Limit: 10})
		require.NoError(t, err)
		require.NoError(t, lazyErr)
		if assert.Len(t, got.Results, len(want.Results), "public search rows for %s", query) {
			for i := range want.Results {
				assert.Equal(t, want.Results[i].ID, got.Results[i].ID, "rank %d for %s", i, query)
				assert.InDelta(t, want.Results[i].Score, got.Results[i].Score, 1e-12, "score at rank %d for %s", i, query)
			}
		}
		statement := "CALL db.retrieve({query: $query, limit: 10}) YIELD node, score RETURN node.text AS text, score"
		params := map[string]interface{}{"query": query}
		wantRows, err := freshExecutor.Execute(ctx, statement, params)
		require.NoError(t, err)
		require.Len(t, wantRows.Rows, wantCount, "Cypher must reach the populated fresh service")
		gotRows, err := actualExecutor.Execute(ctx, statement, params)
		require.NoError(t, err)
		assert.Equal(t, wantRows.Columns, gotRows.Columns)
		assert.Equal(t, wantRows.Rows, gotRows.Rows, "Cypher rows, rank and score for %s", query)
	}
	if kind != "valid" {
		assert.Positive(t, reads.Load(), "rejected checkpoint must traverse nodes before answering")
	} else {
		assert.Zero(t, reads.Load(), "valid checkpoint must restore without candidate node traversal")
	}
	require.True(t, service.IsReady())
	t.Logf("checkpoint=%s backend=v2 traversal=%s lifecycle=%s entry=%s: public search and Cypher rows/ranks/scores equal fresh rebuild; candidate node traversals=%d", kind, traversal, lifecycle, entry, reads.Load())
}
