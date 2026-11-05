// Author: Codex NOR-283 implementation lane, 29 October 2026.
package search

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// These controls use only the existing public writer/loader APIs, so they also
// compile on the release red baseline. They distinguish accepted empty states
// from clearing a rejected file and retain the writer's legitimate tombstones,
// frequency cap and equivalent lookup orderings.
func TestBM25CheckpointLoadPreservesWriterStates(t *testing.T) {
	for _, saveMode := range []string{"copy", "no_copy"} {
		for _, population := range []string{"empty", "populated", "deleted", "updated", "all_deleted", "capped_frequency", "reordered"} {
			t.Run(saveMode+"/"+population, func(t *testing.T) {
				index := NewFulltextIndexV2WithAnalyzer(nil)
				if population != "empty" {
					index.Index("a", "orchard orchard citrus", nil, nil, false)
					index.Index("b", "orchard meadow", nil, nil, false)
				}
				switch population {
				case "deleted":
					index.Remove("b")
				case "updated":
					index.Index("b", "orchard harbor", nil, nil, false)
				case "all_deleted":
					index.Remove("a")
					index.Remove("b")
				case "capped_frequency":
					index.Index("large", strings.Repeat("orchard ", 70_000), nil, nil, false)
				}
				path := filepath.Join(t.TempDir(), "bm25")
				if saveMode == "copy" {
					require.NoError(t, index.Save(path))
				} else {
					require.NoError(t, index.SaveNoCopy(path))
				}
				if population == "reordered" {
					data, err := os.ReadFile(path)
					require.NoError(t, err)
					var snapshot bm25V2Snapshot
					require.NoError(t, msgpack.Unmarshal(data, &snapshot))
					for _, state := range snapshot.TermIndex {
						for i, j := 0, len(state.Postings)-1; i < j; i, j = i+1, j-1 {
							state.Postings[i], state.Postings[j] = state.Postings[j], state.Postings[i]
						}
					}
					for i, j := 0, len(snapshot.Lexicon)-1; i < j; i, j = i+1, j-1 {
						snapshot.Lexicon[i], snapshot.Lexicon[j] = snapshot.Lexicon[j], snapshot.Lexicon[i]
					}
					data, err = msgpack.Marshal(&snapshot)
					require.NoError(t, err)
					require.NoError(t, os.WriteFile(path, data, 0600))
				}
				loaded := NewFulltextIndexV2WithAnalyzer(nil)
				loaded.Index("sentinel", "sentinel", nil, nil, false)
				require.NoError(t, loaded.Load(path))
				require.False(t, loaded.IsDirty(), "a legitimate checkpoint must be installed, not rejected/cleared")
				require.Equal(t, index.Count(), loaded.Count())
				for _, query := range []string{"orchard", "citrus", "meadow", "harbor", "sentinel"} {
					require.Equal(t, index.Search(query, 10), loaded.Search(query, 10), query)
				}
			})
		}
	}
}

func BenchmarkBM25CheckpointAdmission(b *testing.B) {
	rebuildStarted := time.Now()
	index := NewFulltextIndexV2WithAnalyzer(nil)
	for document := 0; document < 1000; document++ {
		index.Index(strconv.Itoa(document), strings.Repeat("orchard meadow citrus harbor ", 10), nil, nil, false)
	}
	b.Logf("reference: rebuilding the same 1000-document fulltext index (no storage traversal) took %s", time.Since(rebuildStarted))
	path := filepath.Join(b.TempDir(), "bm25")
	require.NoError(b, index.Save(path))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		loaded := NewFulltextIndexV2WithAnalyzer(nil)
		if err := loaded.Load(path); err != nil || loaded.Count() != 1000 {
			b.Fatalf("checkpoint did not restore: count=%d error=%v", loaded.Count(), err)
		}
	}
}

// These fixtures are encoded afresh by the current writer: on the digest
// format they have valid integrity, so rejection proves the structural layer.
// The same source compiles on the release baseline without a digest API.
func TestBM25CheckpointIntegrityAdmission(t *testing.T) {
	for _, kind := range []string{"missing_digest", "bad_digest", "truncated_digest", "previous_format", "missing_terms", "nil_term",
		"document_count", "document_mapping", "document_length", "total_length", "average_length",
		"missing_posting", "posting_document", "posting_frequency", "duplicate_posting", "wrong_lexicon"} {
		t.Run(kind, func(t *testing.T) {
			index := NewFulltextIndexV2WithAnalyzer(nil)
			index.Index("a", "orchard orchard citrus", nil, nil, false)
			index.Index("b", "orchard meadow", nil, nil, false)
			path := filepath.Join(t.TempDir(), "bm25")
			require.NoError(t, index.Save(path))
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			var snapshot bm25V2Snapshot
			require.NoError(t, msgpack.Unmarshal(data, &snapshot))
			switch kind {
			case "previous_format":
				snapshot.Version = "2.1.0"
			case "missing_terms":
				snapshot.TermIndex = nil
			case "nil_term":
				snapshot.TermIndex["orchard"] = nil
			case "document_count":
				snapshot.DocCount++
			case "document_mapping":
				snapshot.DocIDToNum["a"] = 1000
			case "document_length":
				snapshot.DocLengths[0]++
			case "total_length":
				snapshot.TotalDocLength++
			case "average_length":
				snapshot.AvgDocLength++
			case "missing_posting":
				snapshot.TermIndex["orchard"].Postings = snapshot.TermIndex["orchard"].Postings[:1]
			case "posting_document":
				snapshot.TermIndex["orchard"].Postings[0].DocNum = 1000
			case "posting_frequency":
				snapshot.TermIndex["orchard"].Postings[0].TF = 500
			case "duplicate_posting":
				state := snapshot.TermIndex["orchard"]
				state.Postings = append(state.Postings, state.Postings[0])
			case "wrong_lexicon":
				snapshot.Lexicon[0] = "not-an-indexed-term"
			}
			if kind == "missing_digest" || kind == "previous_format" {
				type payload bm25V2Snapshot
				data, err = msgpack.Marshal((*payload)(&snapshot))
			} else {
				data, err = msgpack.Marshal(&snapshot)
			}
			require.NoError(t, err)
			if kind == "bad_digest" {
				data[len(data)-1] ^= 1
			} else if kind == "truncated_digest" {
				data = data[:len(data)-1]
			}
			require.NoError(t, os.WriteFile(path, data, 0600))
			loaded := NewFulltextIndexV2WithAnalyzer(nil)
			loaded.Index("sentinel", "sentinel", nil, nil, false)
			require.NoError(t, loaded.Load(path))
			require.Zero(t, loaded.Count(), "recoverable content rejection must leave the candidate eligible for rebuilding")
		})
	}
}
