package search

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// locate reads the same terms as Analyze, in order, for the default
// analyzer and for stemmed ones, over texts NFKC and case folding change:
// it is the general path term matches use, Analyze the indexing fast path.
func TestLocateReadsAnalyzeTerms(t *testing.T) {
	analyzers := map[string]textAnalyzer{
		"default": DefaultTextAnalyzer().(textAnalyzer),
		"stem": {stem: func(term string) string {
			if term == "the" {
				return ""
			}
			return strings.TrimSuffix(term, "s")
		}},
		"batch": {stemTokens: func(terms []string) []string {
			out := make([]string, len(terms))
			for index, term := range terms {
				out[index] = term + "~"
			}
			return out
		}},
	}
	corpus := []string{
		"", "   ", "Hello, World!", "The cats sat on the mats.",
		"Straße STRASSE straße", "İstanbul ıi", "ΣΊΣΥΦΟΣ σίσυφος", "ﬁne ﬂow", "½ ① ２０２６",
		"café café CAFÉ", "東京都 2026年", "e-mail: a.b@c.de", "ǅemal ǈ", "x y\tz\nw", "🙂smile🙂",
		"Ａ-Ｂ　ｃ", "à́b", "123abc456",
		"A" + strings.Repeat("⃐", 25) + " b", // a segment longer than the fold buffer
	}
	random := rand.New(rand.NewSource(1060))
	alphabet := []rune("aZ09 ßẞİıσςΣﬁ½①ＡＢ１２é́ -.,東京🙂 \tǅ̀")
	for range 3000 {
		runes := make([]rune, random.Intn(40))
		for index := range runes {
			runes[index] = alphabet[random.Intn(len(alphabet))]
		}
		corpus = append(corpus, string(runes))
	}
	for name, analyzer := range analyzers {
		for _, text := range corpus {
			located := analyzer.locate(text)
			terms := make([]string, 0, len(located))
			previous := 0
			for _, term := range located {
				require.True(t, previous <= term.start && term.start < term.end && term.end <= len(text), "%s %q %+v", name, text, term)
				previous = term.start
				terms = append(terms, term.term)
			}
			want := analyzer.Analyze(text)
			require.Equal(t, len(want), len(terms), "%s %q", name, text)
			if len(want) > 0 {
				require.Equal(t, want, terms, "%s %q", name, text)
			}
			// locateIn is locate filtered to the query terms (every other term).
			query := map[string]*queryTerm{}
			var filtered, got []locatedTerm
			for index, term := range located {
				if _, seen := query[term.term]; !seen && index%2 == 0 {
					query[term.term] = &queryTerm{term: term.term, index: len(query)}
				}
			}
			for _, term := range located {
				if _, ok := query[term.term]; ok {
					filtered = append(filtered, term)
				}
			}
			analyzer.locateIn(text, query, nil, func(term *queryTerm, start, end int) {
				got = append(got, locatedTerm{term: term.term, start: start, end: end})
			})
			require.Equal(t, filtered, got, "%s %q", name, text)
		}
	}
	located := DefaultTextAnalyzer().(textAnalyzer).locate("Notices go to MÜLLER, Straße 1")
	require.Equal(t, locatedTerm{term: "müller", start: 14, end: 21}, located[3])
	require.Equal(t, locatedTerm{term: "strasse", start: 23, end: 30}, located[4])
}

func newMatchTestService(t *testing.T) (*Service, storage.Engine) {
	engine := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "nornic")
	service := NewService(engine, 2, nil)
	fixtureowner.Own(t, service)
	return service, engine
}

func indexMatchTestNode(t *testing.T, service *Service, engine storage.Engine, node *storage.Node) {
	_, err := engine.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, service.IndexNode(node))
}

// A hit's matches say what matched and where (#1060): a vector hit's best
// chunks (the best always, the next ones from the minimum similarity), each
// placed in the properties as the embedding recorded it; a keyword hit's
// query term occurrences, in the order the index reads the properties.
func TestSearchMatches(t *testing.T) {
	service, engine := newMatchTestService(t)
	properties := map[string]any{"search_text": "Invoice from Müller", "text": "Rent is due monthly. Notices go to MÜLLER in Berlin."}
	source := embeddingutil.BuildEmbedText(properties, nil, &embeddingutil.EmbedTextOptions{})
	chunks := []string{"search_text: Invoice from Müller\n", "text: Rent is due monthly. ", "Notices go to MÜLLER in Berlin."}
	node := &storage.Node{ID: "doc", Labels: []string{"Doc"}, Properties: properties}
	embeddingutil.ApplyManagedEmbedding(node, [][]float32{{0.6, 0.8}, {0, 1}, {1, 0}}, "test", 2, time.Now())
	embeddingutil.RecordChunkSpans(node, source, chunks)
	indexMatchTestNode(t, service, engine, node)
	ctx := context.Background()

	chunkMatches := []SearchMatch{
		{Kind: SearchMatchChunk, Chunk: 2, Score: 1, Property: "text", Start: 21, End: 52},
		{Kind: SearchMatchChunk, Chunk: 0, Score: 0.6, Property: "search_text", Start: 0, End: 19},
	}
	termMatches := []SearchMatch{
		{Kind: SearchMatchTerm, Chunk: -1, Term: "müller", Property: "text", Start: 35, End: 41},
		{Kind: SearchMatchTerm, Chunk: -1, Term: "müller", Property: "search_text", Start: 13, End: 19},
	}
	requireMatches := func(response *SearchResponse, want []SearchMatch) {
		t.Helper()
		require.NotEmpty(t, response.Results)
		got := response.Results[0].Matches
		require.Len(t, got, len(want))
		for index := range want {
			require.InDelta(t, want[index].Score, got[index].Score, 1e-5)
			got[index].Score = want[index].Score
		}
		require.Equal(t, want, got)
	}

	response, err := service.Search(ctx, "", []float32{1, 0}, &SearchOptions{Limit: 5})
	require.NoError(t, err)
	requireMatches(response, chunkMatches)

	response, err = service.Search(ctx, "müller", nil, &SearchOptions{Limit: 5})
	require.NoError(t, err)
	requireMatches(response, termMatches)

	response, err = service.Search(ctx, "MÜLLER", []float32{1, 0}, &SearchOptions{Limit: 5})
	require.NoError(t, err)
	requireMatches(response, append(append([]SearchMatch(nil), chunkMatches...), termMatches...))

	// With fulltext properties, term matches come from those alone (a new
	// query text: the setting is made before indexing, so the result cache
	// doesn't key on it).
	service.SetFulltextProperties([]string{"search_text"})
	response, err = service.Search(ctx, "Müller", nil, &SearchOptions{Limit: 5})
	require.NoError(t, err)
	require.Equal(t, []SearchMatch{termMatches[1]}, response.Results[0].Matches)

	require.Equal(t, []any{
		map[string]any{"kind": "chunk", "chunk": int64(2), "score": float64(1), "property": "text", "start": int64(21), "end": int64(52)},
		map[string]any{"kind": "term", "term": "müller", "property": "text", "start": int64(35), "end": int64(41)},
		map[string]any{"kind": "chunk", "chunk": int64(1), "score": float64(0), "property": nil, "start": nil, "end": nil},
	}, SearchMatchValues([]SearchMatch{chunkMatches[0], termMatches[0], {Kind: SearchMatchChunk, Chunk: 1}}))
}

// An embedding made before chunk places were recorded is placed from the
// chunk texts it kept (Voyage's contextualized chunks), in the text built
// with the worker's options; without kept chunk texts the chunk matches,
// with no place.
func TestSearchMatchesOfEarlierEmbeddings(t *testing.T) {
	service, engine := newMatchTestService(t)
	service.SetEmbedTextOptions(&embeddingutil.EmbedTextOptions{Include: []string{"text"}})
	kept := &storage.Node{ID: "kept", Properties: map[string]any{"text": "alpha beta. gamma delta.", "title": "T"}}
	embeddingutil.ApplyManagedEmbedding(kept, [][]float32{{0, 1}, {1, 0}}, "voyage-context-4", 2, time.Now())
	kept.EmbedMeta["chunk_texts"] = []any{"text: alpha beta. ", "gamma delta."}
	indexMatchTestNode(t, service, engine, kept)
	bare := &storage.Node{ID: "bare", Properties: map[string]any{"text": "unrelated"}}
	embeddingutil.ApplyManagedEmbedding(bare, [][]float32{{0.9, 0.1}}, "local", 2, time.Now())
	indexMatchTestNode(t, service, engine, bare)

	response, err := service.Search(context.Background(), "", []float32{1, 0}, &SearchOptions{Limit: 5})
	require.NoError(t, err)
	require.Len(t, response.Results, 2)
	byID := map[string][]SearchMatch{}
	for _, result := range response.Results {
		byID[result.ID] = result.Matches
	}
	require.Len(t, byID["kept"], 1)
	require.Equal(t, SearchMatch{Kind: SearchMatchChunk, Chunk: 1, Score: byID["kept"][0].Score, Property: "text", Start: 12, End: 24}, byID["kept"][0])
	require.Len(t, byID["bare"], 1)
	require.Equal(t, "", byID["bare"][0].Property)
	require.Equal(t, 0, byID["bare"][0].Chunk)
}

// On Badger the worker writes the embedding metadata to the sidecar, which
// the embedding-free hit read leaves out: the hits' chunk places, and the
// chunk texts the reranker reads, come from the metadata-only read.
func TestSearchMatchesReadBadgerSidecarMeta(t *testing.T) {
	badgerEngine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = badgerEngine.Close() })
	engine := storage.NewNamespacedEngine(badgerEngine, "nornic")
	service := NewService(engine, 2, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	properties := map[string]any{"text": "alpha beta. gamma delta."}
	_, err = engine.CreateNode(&storage.Node{ID: "doc", Properties: properties})
	require.NoError(t, err)
	node, err := badgerEngine.GetNode("nornic:doc")
	require.NoError(t, err)
	chunks := []string{"text: alpha beta. ", "gamma delta."}
	embeddingutil.ApplyManagedEmbedding(node, [][]float32{{0, 1}, {1, 0}}, "voyage-context-4", 2, time.Now())
	node.EmbedMeta["chunk_texts"] = []any{chunks[0], chunks[1]}
	embeddingutil.RecordChunkSpans(node, embeddingutil.BuildEmbedText(properties, nil, &embeddingutil.EmbedTextOptions{}), chunks)
	require.NoError(t, badgerEngine.UpdateNodeEmbeddingSidecar(node))
	// Indexed from a copy without the metadata, as a write that didn't load
	// the embedding indexes it (and a full read would cache the node with its
	// metadata, which the hit read would then copy).
	indexed := storage.CopyNode(node)
	indexed.ID = "doc"
	indexed.EmbedMeta = nil
	require.NoError(t, service.IndexNode(indexed))

	response, err := service.Search(context.Background(), "", []float32{1, 0}, &SearchOptions{Limit: 5})
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	matches := response.Results[0].Matches
	require.Len(t, matches, 1)
	require.Equal(t, SearchMatch{Kind: SearchMatchChunk, Chunk: 1, Score: matches[0].Score, Property: "text", Start: 12, End: 24}, matches[0])

	// Re-embedded with other chunks and indexed again: the remembered places
	// are dropped, and the new ones read.
	node.EmbedMeta["chunk_texts"] = []any{"text: alpha beta. gamma ", "delta."}
	embeddingutil.RecordChunkSpans(node, embeddingutil.BuildEmbedText(properties, nil, &embeddingutil.EmbedTextOptions{}), []string{"text: alpha beta. gamma ", "delta."})
	require.NoError(t, badgerEngine.UpdateNodeEmbeddingSidecar(node))
	require.NoError(t, service.IndexNode(indexed))
	response, err = service.Search(context.Background(), "", []float32{1, 0}, &SearchOptions{Limit: 6})
	require.NoError(t, err)
	require.Equal(t, 18, response.Results[0].Matches[0].Start)
	require.Equal(t, 24, response.Results[0].Matches[0].End)
	node.EmbedMeta["chunk_texts"] = []any{chunks[0], chunks[1]}
	embeddingutil.RecordChunkSpans(node, embeddingutil.BuildEmbedText(properties, nil, &embeddingutil.EmbedTextOptions{}), chunks)
	require.NoError(t, badgerEngine.UpdateNodeEmbeddingSidecar(node))

	light, err := service.getNodeWithoutEmbeddings("doc")
	require.NoError(t, err)
	require.Empty(t, light.EmbedMeta)
	// The matched chunk's window; without its chunk texts the reranker would
	// read the text's start ("alpha beta.").
	require.Equal(t, "gamma delta.", service.rerankCandidateContent(light, rrfResult{ID: "doc", MatchID: nodeChunkVectorKey("doc", 1)}, "zzz", 12))
}

// A keyword hit's term matches list its rarest query terms' occurrences
// first, by the index's IDF: a common word doesn't crowd them out.
func TestTermMatchesRarestTermsFirst(t *testing.T) {
	service, engine := newMatchTestService(t)
	for index, text := range []string{"the lease of the flat", "the deposit and the rent", "the notices go to the landlord Müller"} {
		indexMatchTestNode(t, service, engine, &storage.Node{ID: storage.NodeID(fmt.Sprintf("doc-%d", index)), Properties: map[string]any{"text": text}})
	}
	response, err := service.Search(context.Background(), "the Müller", nil, &SearchOptions{Limit: 5})
	require.NoError(t, err)
	require.Equal(t, "doc-2", response.Results[0].ID)
	terms := make([]string, 0, 3)
	for _, match := range response.Results[0].Matches {
		terms = append(terms, match.Term)
	}
	require.Equal(t, []string{"müller", "the", "the"}, terms)
	require.Equal(t, SearchMatch{Kind: SearchMatchTerm, Chunk: -1, Term: "the", Property: "text", Start: 0, End: 3}, response.Results[0].Matches[1])
}
