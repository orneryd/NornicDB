package search

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search/stemmer"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestFulltextIndexV2PreparedCustomAnalyzerDoesNotGuessFieldOwnership(t *testing.T) {
	analyzer := NewStemmedTextAnalyzer(stemmer.Registration{
		ID: "test.reorder", StemTokens: func(tokens []string) []string {
			out := append([]string(nil), tokens...)
			for left, right := 0, len(out)-1; left < right; left, right = left+1, right-1 {
				out[left], out[right] = out[right], out[left]
			}
			return out
		},
	})
	index := NewFulltextIndexV2WithAnalyzer(analyzer)
	text := "public secret"
	terms := analyzer.Analyze(text)
	index.IndexFieldsPrepared("doc", text, []FulltextField{
		{Name: "public", Text: "public"}, {Name: "secret", Text: "secret"},
	}, terms, termFrequencies(terms))
	_, found := index.FieldProvenance("doc")
	require.False(t, found, "term order across fields cannot be inferred from prepared custom tokens")
	hits := index.Search("public", 1)
	require.Len(t, hits, 1)
	require.Equal(t, "doc", hits[0].ID)
}

// IndexFields must not change what the index already guarantees. The same
// corpus in the same insertion order must produce the same postings, so every
// unrestricted read returns what a corpus indexed through Index returns, while
// the fielded index also holds provenance for each document.
func TestFulltextIndexV2IndexFieldsKeepsUnrestrictedSearch(t *testing.T) {
	t.Setenv("NORNICDB_BM25_PREFIX_MAX_EXPANSIONS", "")
	corpus := []struct {
		id    string
		title string
		body  string
	}{
		{id: "doc-1", title: "Rare Omega", body: "omega beta beta"},
		{id: "doc-2", title: "Common Omega", body: "beta gamma"},
		{id: "doc-3", title: "Kinase report", body: "omega omega rare"},
	}
	plain := NewFulltextIndexV2WithAnalyzer(nil)
	fielded := NewFulltextIndexV2WithAnalyzer(nil)
	for _, document := range corpus {
		composed := document.title + "\n" + document.body
		plain.Index(document.id, composed, nil, nil, false)
		fielded.IndexFields(document.id, composed, []FulltextField{
			{Name: "title", Text: document.title},
			{Name: "body", Text: document.body},
		})
	}

	require.Equal(t, plain.Count(), fielded.Count())
	for _, query := range []string{"omega", "rare", "beta", "omega beta", "kinase", "absent"} {
		require.Equal(t, plain.Search(query, 10), fielded.Search(query, 10), query)
		require.Equal(t, plain.Search(query, 2), fielded.Search(query, 2), query)
	}
	allowed := func(id string) bool { return id != "doc-2" }
	require.Equal(t, plain.SearchAllowed("omega", 10, allowed), fielded.SearchAllowed("omega", 10, allowed))
	require.Equal(t, plain.PhraseSearch("omega beta", 10), fielded.PhraseSearch("omega beta", 10))
	require.Equal(t, plain.TermIDF("omega"), fielded.TermIDF("omega"))
	require.Equal(t, plain.LexicalSeedHints(4, 2), fielded.LexicalSeedHints(4, 2))
	require.Equal(t, plain.LexicalSeedDocIDs(4, 2), fielded.LexicalSeedDocIDs(4, 2))
	for _, document := range corpus {
		plainText, plainFound := plain.GetDocument(document.id)
		fieldedText, fieldedFound := fielded.GetDocument(document.id)
		require.Equal(t, plainText, fieldedText)
		require.Equal(t, plainFound, fieldedFound)
	}

	for _, document := range corpus {
		_, found := fielded.FieldProvenance(document.id)
		require.True(t, found, document.id)
		_, found = plain.FieldProvenance(document.id)
		require.False(t, found, "Index stores no field provenance")
	}
}

// Provenance is the analyzed content of each field: its analyzed length and the
// frequency of each distinct term, which is what authorization-aware scoring
// needs and what must partition the document the postings describe.
func TestFulltextIndexV2IndexFieldsStoresFieldProvenance(t *testing.T) {
	index := NewFulltextIndexV2WithAnalyzer(nil)
	index.IndexFields("doc", "Rare Omega\nomega beta beta", []FulltextField{
		{Name: "title", Text: "Rare Omega"},
		{Name: "body", Text: "omega beta beta"},
	})

	stored, found := index.FieldProvenance("doc")
	require.True(t, found)
	require.Equal(t, []DocumentFieldProvenance{
		{Name: "title", Length: 2, Terms: []FieldTermFrequency{{Term: "omega", Frequency: 1}, {Term: "rare", Frequency: 1}}},
		{Name: "body", Length: 3, Terms: []FieldTermFrequency{{Term: "beta", Frequency: 2}, {Term: "omega", Frequency: 1}}},
	}, stored)
	require.Equal(t, len(index.analyze("Rare Omega\nomega beta beta")), stored[0].Length+stored[1].Length,
		"the fields partition the document's analyzed text")

	// Callers read a copy: changing it cannot change the index.
	stored[0].Name = "changed"
	stored[1].Terms[0].Frequency = 99
	reloaded, found := index.FieldProvenance("doc")
	require.True(t, found)
	require.Equal(t, "title", reloaded[0].Name)
	require.Equal(t, 2, reloaded[1].Terms[0].Frequency)
}

// A field view the analyzer reads as different terms than the composed text
// cannot be composed with the document's postings. The document is still
// indexed exactly as Index indexes it and no provenance is stored, so a later
// restricted scoring path must project the readable corpus.
func TestFulltextIndexV2IndexFieldsFieldBoundaryFallback(t *testing.T) {
	cases := []struct {
		name       string
		composed   string
		fields     []FulltextField
		searchable bool
	}{
		{
			name:       "field texts joined without an analyzer term boundary",
			composed:   "alphabeta",
			fields:     []FulltextField{{Name: "left", Text: "alpha"}, {Name: "right", Text: "beta"}},
			searchable: true,
		},
		{
			name:       "composed text does not hold a field",
			composed:   "alpha beta",
			fields:     []FulltextField{{Name: "left", Text: "alpha"}, {Name: "right", Text: "gamma"}},
			searchable: true,
		},
		{
			name:       "composed text holds fewer terms than the fields",
			composed:   "alpha",
			fields:     []FulltextField{{Name: "left", Text: "alpha"}, {Name: "right", Text: "beta"}},
			searchable: true,
		},
		{
			name:       "duplicate field names",
			composed:   "alpha beta",
			fields:     []FulltextField{{Name: "body", Text: "alpha"}, {Name: "body", Text: "beta"}},
			searchable: true,
		},
		{
			name:       "unnamed field",
			composed:   "alpha beta",
			fields:     []FulltextField{{Name: "", Text: "alpha"}, {Name: "body", Text: "beta"}},
			searchable: true,
		},
		{
			name:       "no fields",
			composed:   "alpha beta",
			fields:     nil,
			searchable: true,
		},
		{
			name:       "text without terms",
			composed:   "",
			fields:     nil,
			searchable: false,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			index := NewFulltextIndexV2WithAnalyzer(nil)
			index.IndexFields("doc", testCase.composed, testCase.fields)
			_, found := index.FieldProvenance("doc")
			require.False(t, found)

			plain := NewFulltextIndexV2WithAnalyzer(nil)
			plain.Index("doc", testCase.composed, nil, nil, false)
			require.Equal(t, plain.Count(), index.Count())
			for _, query := range []string{"alpha", "beta", "alphabeta", "gamma"} {
				require.Equal(t, plain.Search(query, 10), index.Search(query, 10), query)
			}
			if !testCase.searchable {
				require.Equal(t, 0, index.Count(), "a document without terms is not indexed")
			}
		})
	}
}

// Provenance never outlives the text it describes: every document mutation the
// index owns must drop the entry it no longer describes, and only for the
// document it touches.
func TestFulltextIndexV2IndexFieldsLifecycle(t *testing.T) {
	index := NewFulltextIndexV2WithAnalyzer(nil)
	index.IndexFields("doc", "alpha beta", []FulltextField{
		{Name: "title", Text: "alpha"},
		{Name: "body", Text: "beta"},
	})
	index.IndexFields("other", "kappa lambda", []FulltextField{
		{Name: "title", Text: "kappa"},
		{Name: "body", Text: "lambda"},
	})
	requireProvenanceFieldNames(t, index, "doc", "title", "body")

	t.Run("update replaces the document's provenance", func(t *testing.T) {
		index.IndexFields("doc", "gamma delta", []FulltextField{{Name: "body", Text: "gamma delta"}})
		stored := requireProvenanceFieldNames(t, index, "doc", "body")
		require.Equal(t, []DocumentFieldProvenance{
			{Name: "body", Length: 2, Terms: []FieldTermFrequency{{Term: "delta", Frequency: 1}, {Term: "gamma", Frequency: 1}}},
		}, stored)
		require.Contains(t, resultIDs(index.Search("gamma", 10)), "doc")
		require.NotContains(t, resultIDs(index.Search("alpha", 10)), "doc")
		requireProvenanceFieldNames(t, index, "other", "title", "body")
	})

	t.Run("re-index without fields drops the document's provenance", func(t *testing.T) {
		index.Index("doc", "epsilon zeta", nil, nil, false)
		_, found := index.FieldProvenance("doc")
		require.False(t, found)
		require.Contains(t, resultIDs(index.Search("epsilon", 10)), "doc")
		requireProvenanceFieldNames(t, index, "other", "title", "body")
	})

	t.Run("batch re-index drops the document's provenance", func(t *testing.T) {
		index.IndexFields("doc", "alpha beta", []FulltextField{{Name: "body", Text: "alpha beta"}})
		requireProvenanceFieldNames(t, index, "doc", "body")
		index.IndexBatch([]FulltextBatchEntry{{ID: "doc", Text: "alpha beta"}})
		_, found := index.FieldProvenance("doc")
		require.False(t, found)
		require.Contains(t, resultIDs(index.Search("alpha", 10)), "doc")
		requireProvenanceFieldNames(t, index, "other", "title", "body")
	})

	t.Run("remove drops the document's provenance", func(t *testing.T) {
		index.IndexFields("doc", "alpha beta", []FulltextField{{Name: "body", Text: "alpha beta"}})
		requireProvenanceFieldNames(t, index, "doc", "body")
		index.Remove("doc")
		_, found := index.FieldProvenance("doc")
		require.False(t, found)
		require.NotContains(t, resultIDs(index.Search("alpha", 10)), "doc")
		requireProvenanceFieldNames(t, index, "other", "title", "body")
	})

	t.Run("an empty id stores nothing", func(t *testing.T) {
		index.IndexFields("", "alpha beta", []FulltextField{{Name: "body", Text: "alpha beta"}})
		_, found := index.FieldProvenance("")
		require.False(t, found)
	})

	t.Run("clear drops every document's provenance", func(t *testing.T) {
		requireProvenanceFieldNames(t, index, "other", "title", "body")
		index.Clear()
		_, found := index.FieldProvenance("other")
		require.False(t, found)
		require.Equal(t, 0, index.Count())
	})
}

// Saved provenance survives both snapshot writers, and a corpus indexed without
// IndexFields persists no provenance at all.
func TestFulltextIndexV2IndexFieldsSaveLoadRoundTrip(t *testing.T) {
	index := NewFulltextIndexV2WithAnalyzer(nil)
	index.Index("plain", "kappa lambda", nil, nil, false)
	index.IndexFields("fielded", "Rare Omega\nomega beta beta", []FulltextField{
		{Name: "title", Text: "Rare Omega"},
		{Name: "body", Text: "omega beta beta"},
	})
	expected, found := index.FieldProvenance("fielded")
	require.True(t, found)
	require.True(t, index.IsDirty())

	path := filepath.Join(t.TempDir(), "bm25-fields.v2")
	require.NoError(t, index.Save(path))
	require.False(t, index.IsDirty())

	written := readBM25Checkpoint(t, path)
	require.Equal(t, bm25V2FormatVersion, written.Version, "field provenance has its own checkpoint format version")
	require.Contains(t, written.FieldDocuments, "fielded")
	require.NotContains(t, written.FieldDocuments, "plain")

	loaded := NewFulltextIndexV2WithAnalyzer(nil)
	require.NoError(t, loaded.Load(path))
	require.False(t, loaded.IsDirty())
	require.Equal(t, index.Count(), loaded.Count())
	require.Equal(t, index.Search("omega", 10), loaded.Search("omega", 10))
	reloaded, found := loaded.FieldProvenance("fielded")
	require.True(t, found)
	require.Equal(t, expected, reloaded)
	_, found = loaded.FieldProvenance("plain")
	require.False(t, found)
	require.Equal(t, index.Search("kappa", 10), loaded.Search("kappa", 10))

	noCopyPath := filepath.Join(t.TempDir(), "bm25-fields-nocopy.v2")
	require.NoError(t, index.SaveNoCopy(noCopyPath))
	noCopyLoaded := NewFulltextIndexV2WithAnalyzer(nil)
	require.NoError(t, noCopyLoaded.Load(noCopyPath))
	noCopyProvenance, found := noCopyLoaded.FieldProvenance("fielded")
	require.True(t, found)
	require.Equal(t, expected, noCopyProvenance)
	require.Equal(t, index.Search("omega", 10), noCopyLoaded.Search("omega", 10))

	// A corpus indexed only through Index persists no provenance section at
	// all, and still loads.
	fieldless := NewFulltextIndexV2WithAnalyzer(nil)
	fieldless.Index("plain", "kappa lambda", nil, nil, false)
	fieldlessPath := filepath.Join(t.TempDir(), "bm25-fieldless.v2")
	require.NoError(t, fieldless.Save(fieldlessPath))
	require.Empty(t, readBM25Checkpoint(t, fieldlessPath).FieldDocuments)
	fieldlessLoaded := NewFulltextIndexV2WithAnalyzer(nil)
	require.NoError(t, fieldlessLoaded.Load(fieldlessPath))
	require.Equal(t, fieldless.Search("kappa", 10), fieldlessLoaded.Search("kappa", 10))
}

// Load admits provenance only when it accounts for the document the snapshot
// describes, and it rebuilds a checkpoint written before provenance existed.
func TestFulltextIndexV2IndexFieldsCheckpointAdmission(t *testing.T) {
	t.Run("previous checkpoint format is rebuilt", func(t *testing.T) {
		snapshot := indexFieldsCheckpoint()
		snapshot.Version = "2.2.0"
		snapshot.FieldDocuments = nil
		index := NewFulltextIndexV2WithAnalyzer(nil)
		index.Index("stale", "stale text", nil, nil, false)
		require.NoError(t, index.Load(writeIndexFieldsCheckpoint(t, snapshot)))
		require.Equal(t, 0, index.Count(), "a provenance-free checkpoint is rebuilt, not silently accepted")
	})

	t.Run("current checkpoint format loads provenance", func(t *testing.T) {
		index := NewFulltextIndexV2WithAnalyzer(nil)
		require.NoError(t, index.Load(writeIndexFieldsCheckpoint(t, indexFieldsCheckpoint())))
		require.Equal(t, 1, index.Count())
		stored, found := index.FieldProvenance("doc")
		require.True(t, found)
		require.Equal(t, []DocumentFieldProvenance{
			{Name: "title", Length: 1, Terms: []FieldTermFrequency{{Term: "alpha", Frequency: 1}}},
			{Name: "body", Length: 1, Terms: []FieldTermFrequency{{Term: "beta", Frequency: 1}}},
		}, stored)
	})

	t.Run("provenance that does not account for the document is rebuilt", func(t *testing.T) {
		snapshot := indexFieldsCheckpoint()
		document := snapshot.FieldDocuments["doc"]
		document.Fields[1] = bm25Field{Name: "body"}
		snapshot.FieldDocuments["doc"] = document
		index := NewFulltextIndexV2WithAnalyzer(nil)
		require.NoError(t, index.Load(writeIndexFieldsCheckpoint(t, snapshot)))
		require.Equal(t, 0, index.Count())
	})

	t.Run("provenance for a document the corpus lacks is rebuilt", func(t *testing.T) {
		snapshot := indexFieldsCheckpoint()
		snapshot.FieldDocuments["absent"] = snapshot.FieldDocuments["doc"]
		index := NewFulltextIndexV2WithAnalyzer(nil)
		require.NoError(t, index.Load(writeIndexFieldsCheckpoint(t, snapshot)))
		require.Equal(t, 0, index.Count())
	})

	t.Run("unsorted field terms are rebuilt", func(t *testing.T) {
		snapshot := indexFieldsCheckpoint()
		document := snapshot.FieldDocuments["doc"]
		document.Fields[0] = bm25Field{
			Name:   "title",
			Length: 2,
			Terms:  []bm25FieldFrequency{{Term: "omega", Frequency: 1}, {Term: "alpha", Frequency: 1}},
		}
		document.Fields[1] = bm25Field{Name: "body"}
		snapshot.FieldDocuments["doc"] = document
		index := NewFulltextIndexV2WithAnalyzer(nil)
		require.NoError(t, index.Load(writeIndexFieldsCheckpoint(t, snapshot)))
		require.Equal(t, 0, index.Count(), "field terms must be sorted and distinct")
	})
}

// IndexFields is safe alongside the reads that observe the same document.
func TestFulltextIndexV2IndexFieldsConcurrentMutation(t *testing.T) {
	index := NewFulltextIndexV2WithAnalyzer(nil)
	var wait sync.WaitGroup
	for writer := 0; writer < 2; writer++ {
		wait.Add(1)
		go func(writer int) {
			defer wait.Done()
			for document := 0; document < 25; document++ {
				id := fmt.Sprintf("doc-%d-%d", writer, document)
				title := fmt.Sprintf("title%d", document)
				index.IndexFields(id, title+"\nshared omega", []FulltextField{
					{Name: "title", Text: title},
					{Name: "body", Text: "shared omega"},
				})
			}
		}(writer)
	}
	for reader := 0; reader < 2; reader++ {
		wait.Add(1)
		go func(reader int) {
			defer wait.Done()
			for iteration := 0; iteration < 25; iteration++ {
				index.Search("shared", 5)
				index.FieldProvenance(fmt.Sprintf("doc-%d-%d", reader, iteration))
			}
		}(reader)
	}
	wait.Wait()

	require.Equal(t, 50, index.Count())
	require.Len(t, index.Search("shared", 50), 50)
	stored, found := index.FieldProvenance("doc-1-7")
	require.True(t, found)
	require.Equal(t, []DocumentFieldProvenance{
		{Name: "title", Length: 1, Terms: []FieldTermFrequency{{Term: "title7", Frequency: 1}}},
		{Name: "body", Length: 2, Terms: []FieldTermFrequency{{Term: "omega", Frequency: 1}, {Term: "shared", Frequency: 1}}},
	}, stored)
}

// requireProvenanceFieldNames returns the stored provenance of id and requires
// its fields to be exactly the named ones, in field order.
func requireProvenanceFieldNames(t *testing.T, index *FulltextIndexV2, id string, expected ...string) []DocumentFieldProvenance {
	t.Helper()
	stored, found := index.FieldProvenance(id)
	require.True(t, found, id)
	names := make([]string, len(stored))
	for position, field := range stored {
		names[position] = field.Name
	}
	require.Equal(t, expected, names, id)
	return stored
}

// indexFieldsCheckpoint is one admitted two-field document whose provenance
// accounts for the two terms its single posting list describes.
func indexFieldsCheckpoint() bm25V2Snapshot {
	return bm25V2Snapshot{
		Version:    bm25V2FormatVersion,
		Documents:  map[string]string{"doc": "alpha beta"},
		DocIDToNum: map[string]uint32{"doc": 0},
		DocNumToID: []string{"doc"},
		DocLengths: []uint32{2},
		TermIndex: map[string]*bm25TermState{
			"alpha": {Postings: []bm25Posting{{DocNum: 0, TF: 1}}},
			"beta":  {Postings: []bm25Posting{{DocNum: 0, TF: 1}}},
		},
		Lexicon:        []string{"alpha", "beta"},
		AvgDocLength:   2,
		DocCount:       1,
		TotalDocLength: 2,
		FieldDocuments: map[string]bm25FieldDocument{
			"doc": {Fields: []bm25Field{
				{Name: "title", Length: 1, Terms: []bm25FieldFrequency{{Term: "alpha", Frequency: 1}}},
				{Name: "body", Length: 1, Terms: []bm25FieldFrequency{{Term: "beta", Frequency: 1}}},
			}},
		},
	}
}

// writeIndexFieldsCheckpoint writes snapshot through the index's own encoder, so
// the file carries the integrity trailer Load requires and admission is decided
// by the snapshot's content.
func writeIndexFieldsCheckpoint(t *testing.T, snapshot bm25V2Snapshot) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bm25-checkpoint.v2")
	file, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, msgpack.NewEncoder(file).Encode(&snapshot))
	require.NoError(t, file.Close())
	return path
}

// readBM25Checkpoint decodes the snapshot at path, ignoring the integrity
// trailer Load verifies separately.
func readBM25Checkpoint(t *testing.T, path string) bm25V2Snapshot {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var snapshot bm25V2Snapshot
	require.NoError(t, msgpack.NewDecoder(bytes.NewReader(data)).Decode(&snapshot))
	return snapshot
}

// validV2Snapshot is the admission owner for stored provenance. Every rule it
// enforces rejects a snapshot the caller rebuilds instead of trusting field
// data that does not describe the document the snapshot claims.
func TestFulltextIndexV2IndexFieldsAdmissionRules(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*bm25V2Snapshot)
		admit  bool
	}{
		{
			name:   "provenance that partitions the document",
			mutate: func(*bm25V2Snapshot) {},
			admit:  true,
		},
		{
			name: "empty field holding the document's remaining length",
			mutate: func(snapshot *bm25V2Snapshot) {
				document := snapshot.FieldDocuments["doc"]
				document.Fields[0] = bm25Field{Name: "title", Length: 2, Terms: []bm25FieldFrequency{{Term: "alpha", Frequency: 2}}}
				document.Fields[1] = bm25Field{Name: "body"}
				snapshot.FieldDocuments["doc"] = document
			},
			admit: true,
		},
		{
			name: "no fields",
			mutate: func(snapshot *bm25V2Snapshot) {
				snapshot.FieldDocuments["doc"] = bm25FieldDocument{}
			},
			admit: false,
		},
		{
			name: "unnamed field",
			mutate: func(snapshot *bm25V2Snapshot) {
				document := snapshot.FieldDocuments["doc"]
				document.Fields[0].Name = ""
				snapshot.FieldDocuments["doc"] = document
			},
			admit: false,
		},
		{
			name: "duplicate field names",
			mutate: func(snapshot *bm25V2Snapshot) {
				document := snapshot.FieldDocuments["doc"]
				document.Fields[1].Name = document.Fields[0].Name
				snapshot.FieldDocuments["doc"] = document
			},
			admit: false,
		},
		{
			name: "empty term",
			mutate: func(snapshot *bm25V2Snapshot) {
				document := snapshot.FieldDocuments["doc"]
				document.Fields[0].Terms[0].Term = ""
				snapshot.FieldDocuments["doc"] = document
			},
			admit: false,
		},
		{
			name: "zero frequency",
			mutate: func(snapshot *bm25V2Snapshot) {
				document := snapshot.FieldDocuments["doc"]
				document.Fields[0].Terms[0].Frequency = 0
				snapshot.FieldDocuments["doc"] = document
			},
			admit: false,
		},
		{
			name: "unsorted terms",
			mutate: func(snapshot *bm25V2Snapshot) {
				document := snapshot.FieldDocuments["doc"]
				document.Fields[0] = bm25Field{Name: "title", Length: 2, Terms: []bm25FieldFrequency{
					{Term: "omega", Frequency: 1},
					{Term: "alpha", Frequency: 1},
				}}
				snapshot.FieldDocuments["doc"] = document
			},
			admit: false,
		},
		{
			name: "frequency above the field length",
			mutate: func(snapshot *bm25V2Snapshot) {
				document := snapshot.FieldDocuments["doc"]
				document.Fields[0].Terms[0].Frequency = 2
				snapshot.FieldDocuments["doc"] = document
			},
			admit: false,
		},
		{
			name: "field lengths above the document length",
			mutate: func(snapshot *bm25V2Snapshot) {
				document := snapshot.FieldDocuments["doc"]
				document.Fields[1].Length = 2
				document.Fields[1].Terms[0].Frequency = 2
				snapshot.FieldDocuments["doc"] = document
			},
			admit: false,
		},
		{
			name: "provenance for a document the corpus lacks",
			mutate: func(snapshot *bm25V2Snapshot) {
				snapshot.FieldDocuments["absent"] = snapshot.FieldDocuments["doc"]
			},
			admit: false,
		},
		{
			name: "provenance for a removed document",
			mutate: func(snapshot *bm25V2Snapshot) {
				snapshot.DocLengths[0] = 0
			},
			admit: false,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			snapshot := indexFieldsCheckpoint()
			testCase.mutate(&snapshot)
			require.Equal(t, testCase.admit, NewFulltextIndexV2WithAnalyzer(nil).validV2Snapshot(snapshot))
		})
	}

	// A field frequency saturates at the uint16 ceiling the postings use, so a
	// longer field stays admissible while its sum can no longer be exact.
	capped := bm25Field{Name: "body", Length: 70_000, Terms: []bm25FieldFrequency{{Term: "alpha", Frequency: ^uint16(0)}}}
	require.True(t, validBM25Field(capped))
	capped.Length = 1
	require.False(t, validBM25Field(capped))
}
