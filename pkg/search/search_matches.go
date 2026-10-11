package search

import (
	"context"
	"sort"
	"unicode/utf8"
	"unsafe"

	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// SearchMatch is what made a hit match, and where it lies in the hit's
// string properties (#1060). A SearchResult lists its matched chunks first,
// best first, then its term occurrences.
//
//   - Kind "chunk": one of the hit's embedding chunks matched the query
//     vector. Chunk is its index and Score its similarity to the query. A
//     chunk that runs across two properties is one match per property.
//   - Kind "term": one occurrence of a query term matched the keyword
//     query. Term is the term as the BM25 analyzer reads it.
//
// [Start, End) counts Unicode code points of the Property value, as
// Cypher's substring() does. Property is "" for a chunk whose place in the
// properties is unknown (an embedding made before chunk places were
// recorded, whose chunk texts weren't kept).
type SearchMatch struct {
	Kind     string  `json:"kind"`
	Chunk    int     `json:"chunk"`
	Score    float64 `json:"score,omitempty"`
	Term     string  `json:"term,omitempty"`
	Property string  `json:"property,omitempty"`
	Start    int     `json:"start"`
	End      int     `json:"end"`
}

// Value is the match as clients read it, in Cypher and over HTTP: kind;
// chunk and score, or term; property, start and end, which are null for a
// chunk whose place in the properties is unknown.
func (m SearchMatch) Value() map[string]any {
	value := map[string]any{"kind": m.Kind, "property": nil, "start": nil, "end": nil}
	if m.Kind == SearchMatchChunk {
		value["chunk"] = int64(m.Chunk)
		value["score"] = m.Score
	} else {
		value["term"] = m.Term
	}
	if m.Property != "" {
		value["property"] = m.Property
		value["start"] = int64(m.Start)
		value["end"] = int64(m.End)
	}
	return value
}

// searchMatchesBytes is the memory matches hold, for the result cache and
// continuation budgets.
func searchMatchesBytes(matches []SearchMatch) int64 {
	size := int64(0)
	for _, match := range matches {
		size += int64(unsafe.Sizeof(match)) + int64(len(match.Kind)+len(match.Term)+len(match.Property))
	}
	return size
}

// SearchMatchValues is matches as Value lists them.
func SearchMatchValues(matches []SearchMatch) []any {
	values := make([]any, len(matches))
	for index, match := range matches {
		values[index] = match.Value()
	}
	return values
}

const (
	// SearchMatchChunk is a SearchMatch of an embedding chunk.
	SearchMatchChunk = "chunk"
	// SearchMatchTerm is a SearchMatch of a query term occurrence.
	SearchMatchTerm = "term"

	// maxChunkMatchesPerHit bounds a hit's chunk matches: its best chunk,
	// then the next best that reach the search's minimum similarity.
	maxChunkMatchesPerHit = 3
	// maxTermMatchesPerHit bounds a hit's term matches: the occurrences of
	// its rarest query terms first.
	maxTermMatchesPerHit = 32
)

// hitMatchQuery is what a search's hits matched: the keyword query for
// keyword hits and the query vector for vector hits. In a hybrid search a
// hit was found by a retriever only when it has that retriever's rank.
type hitMatchQuery struct {
	text      string
	embedding []float32
	hybrid    bool
}

// attachSearchMatches sets each result's Matches from its node (nodes[i]
// is results[i]'s): the chunks of vector hits that matched the query
// vector best, and the query term occurrences of keyword hits.
func (s *Service) attachSearchMatches(ctx context.Context, results []SearchResult, nodes []*storage.Node, query hitMatchQuery, opts *SearchOptions) {
	if len(results) == 0 || (len(query.embedding) == 0 && query.text == "") {
		return
	}
	var vectorHits []int
	var terms map[string]float64
	analyzer := normalizeAnalyzer(s.bm25Analyzer)
	var fulltextProperties []string
	if query.text != "" {
		fulltextProperties = s.FulltextProperties()
		idf, _ := s.fulltext().(termIDFReader)
		terms = make(map[string]float64)
		for _, term := range analyzer.Analyze(query.text) {
			if idf != nil {
				terms[term] = idf.TermIDF(term)
			} else {
				terms[term] = 0
			}
		}
	}
	for index := range results {
		if nodes[index] == nil {
			continue
		}
		if len(query.embedding) > 0 && (!query.hybrid || results[index].VectorRank > 0) {
			vectorHits = append(vectorHits, index)
		}
		if len(terms) > 0 && (!query.hybrid || results[index].BM25Rank > 0) {
			results[index].Matches = append(results[index].Matches, termMatches(nodes[index], fulltextProperties, analyzer, terms)...)
		}
	}
	if len(vectorHits) > 0 {
		s.attachChunkMatches(ctx, results, nodes, vectorHits, query.embedding, opts.GetMinSimilarity(0.5))
	}
}

// attachChunkMatches scores every chunk of the vector hits against the
// query vector, in one exact-scorer call, and puts each hit's best chunks
// first in its Matches.
func (s *Service) attachChunkMatches(ctx context.Context, results []SearchResult, nodes []*storage.Node, hits []int, embedding []float32, minSimilarity float64) {
	hitByNode := make(map[string]int, len(hits))
	var candidates []Candidate
	// Lock order indexMu -> mu, as index writes take them.
	s.indexMu.RLock()
	s.mu.RLock()
	for _, index := range hits {
		nodeID := string(nodes[index].ID)
		hitByNode[nodeID] = index
		for _, vectorID := range s.nodeChunkVectors[nodeID] {
			candidates = append(candidates, Candidate{ID: vectorID})
		}
	}
	vectors, fileStore := s.vectorIndex, s.vectorFileStore
	s.mu.RUnlock()
	s.indexMu.RUnlock()
	if len(candidates) == 0 || (vectors == nil && fileStore == nil) {
		return
	}
	scored, err := s.resolveVectorExactScorer(exactScorerPolicyCPU, vectors, fileStore).ScoreCandidates(ctx, embedding, candidates)
	if err != nil {
		return
	}
	chunkScores := make(map[int][]ScoredCandidate, len(hits))
	for _, candidate := range scored { // best first
		index, ok := hitByNode[normalizeVectorResultIDToNodeID(candidate.ID)]
		if !ok {
			continue
		}
		chunk, ok := matchingChunkIndex(string(nodes[index].ID), candidate.ID)
		if !ok {
			continue
		}
		kept := chunkScores[index]
		if len(kept) >= maxChunkMatchesPerHit || (len(kept) > 0 && candidate.Score < minSimilarity) || keptChunk(kept, nodes[index], chunk) {
			continue
		}
		chunkScores[index] = append(kept, candidate)
	}
	for index, kept := range chunkScores {
		spans := s.chunkSpans(nodes[index])
		matches := make([]SearchMatch, 0, len(kept)+len(results[index].Matches))
		for _, candidate := range kept {
			chunk, _ := matchingChunkIndex(string(nodes[index].ID), candidate.ID)
			placed := false
			for _, span := range spans {
				if span.Chunk == chunk {
					matches = append(matches, SearchMatch{Kind: SearchMatchChunk, Chunk: chunk, Score: candidate.Score, Property: span.Property, Start: span.Start, End: span.End})
					placed = true
				}
			}
			if !placed {
				matches = append(matches, SearchMatch{Kind: SearchMatchChunk, Chunk: chunk, Score: candidate.Score})
			}
		}
		results[index].Matches = append(matches, results[index].Matches...)
	}
}

// keptChunk reports whether chunk is already among kept: chunk 0 is
// indexed under the node's own ID.
func keptChunk(kept []ScoredCandidate, node *storage.Node, chunk int) bool {
	for _, candidate := range kept {
		if index, _ := matchingChunkIndex(string(node.ID), candidate.ID); index == chunk {
			return true
		}
	}
	return false
}

// chunkSpans is where node's chunks lie in its string properties: as the
// embedding recorded them, or, for an embedding made before they were
// recorded, found from the chunk texts it kept (a provider chunker's, such
// as Voyage's contextualized chunks) in the node's embedding text.
func (s *Service) chunkSpans(node *storage.Node) []embeddingutil.ChunkSpan {
	meta := s.hitEmbedMeta(node)
	if spans, ok := embeddingutil.StoredChunkSpans(meta); ok {
		return spans
	}
	chunks := managedChunkTexts(meta)
	if len(chunks) == 0 || len(chunks) != embeddingutil.ChunkCount(meta) {
		return nil
	}
	s.mu.RLock()
	opts := s.embedTextOptions
	s.mu.RUnlock()
	return embeddingutil.BuildEmbedText(node.Properties, node.Labels, opts).ChunkSpans(chunks)
}

// hitEmbedMeta is a hit node's embedding metadata: its kept chunk texts and
// chunk places. The node was read without its embedding, which may leave the
// metadata out (Badger keeps it in the embedding sidecar), so it is read on
// its own, without the vectors, from an engine that can.
func (s *Service) hitEmbedMeta(node *storage.Node) map[string]any {
	reader, canRead := s.engine.(storage.NodeEmbeddingMetaReader)
	if canRead {
		if meta, err := reader.GetNodeEmbeddingMeta(node.ID); err == nil && len(meta) > 0 {
			return meta
		}
	}
	if len(node.EmbedMeta) > 0 || canRead {
		return node.EmbedMeta
	}
	if full, err := s.engine.GetNode(node.ID); err == nil && full != nil {
		return full.EmbedMeta
	}
	return nil
}

// SetEmbedTextOptions tells the service how the embedding worker builds a
// node's embedding text (labels and the included properties), so the
// chunks of an embedding made before chunk places were recorded can be
// placed in the node's properties.
func (s *Service) SetEmbedTextOptions(opts *embeddingutil.EmbedTextOptions) {
	s.mu.Lock()
	s.embedTextOptions = opts
	s.mu.Unlock()
}

// termIDFReader is a BM25 index that can weigh a query term.
type termIDFReader interface {
	TermIDF(term string) float64
}

// termMatches returns the occurrences of the query terms in node's string
// properties that the BM25 index reads, up to maxTermMatchesPerHit: the
// rarest terms' first (terms holds each term's IDF), so a common word
// doesn't crowd out the words that made the hit, each term's in the index's
// property order.
func termMatches(node *storage.Node, fulltextProperties []string, analyzer Analyzer, terms map[string]float64) []SearchMatch {
	located, ok := analyzer.(locatingAnalyzer)
	if !ok {
		return nil
	}
	var matches []SearchMatch
	visitSearchableFields(node, fulltextProperties, func(key string, value any, _ bool) bool {
		text, isString := value.(string)
		if !isString {
			return true
		}
		for _, token := range located.locate(text) {
			if _, matched := terms[token.term]; !matched {
				continue
			}
			start := utf8.RuneCountInString(text[:token.start])
			matches = append(matches, SearchMatch{
				Kind: SearchMatchTerm, Chunk: -1, Term: token.term, Property: key,
				Start: start, End: start + utf8.RuneCountInString(text[token.start:token.end]),
			})
		}
		return true
	})
	sort.SliceStable(matches, func(i, j int) bool {
		return terms[matches[i].Term] > terms[matches[j].Term]
	})
	if len(matches) > maxTermMatchesPerHit {
		matches = matches[:maxTermMatchesPerHit]
	}
	return matches
}
