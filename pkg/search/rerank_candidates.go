package search

import (
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	// EnvSearchRerankMaxDocumentBytes bounds each candidate sent to a reranker.
	EnvSearchRerankMaxDocumentBytes = "NORNICDB_SEARCH_RERANK_MAX_DOCUMENT_BYTES"
	defaultRerankMaxDocumentBytes   = 2048
	// EnvSearchRerankContextProperties lists the short identifying properties
	// (comma-separated) placed in front of every rerank candidate so the
	// reranker knows which document a passage belongs to.
	EnvSearchRerankContextProperties = "NORNICDB_SEARCH_RERANK_CONTEXT_PROPERTIES"
	defaultRerankContextProperties   = "title,name"
	// rerankContextPropertyMaxBytes bounds each identifying property value.
	rerankContextPropertyMaxBytes = 256
)

func effectiveRerankMaxBytes(opts *SearchOptions) int {
	if opts != nil && opts.RerankMaxBytes > 0 {
		return opts.RerankMaxBytes
	}
	value := envutil.GetInt(EnvSearchRerankMaxDocumentBytes, defaultRerankMaxDocumentBytes)
	if value <= 0 {
		return defaultRerankMaxDocumentBytes
	}
	return value
}

// rerankCandidateContent builds the text a reranker sees for one candidate:
// the node's identifying properties (title/name by default) followed by the
// passage. For a managed-embedding hit the passage is the matched chunk
// extended with its neighbouring chunks until maxBytes is reached, so the
// reranker judges the document around the match instead of one small chunk;
// for a lexical-only hit it is the query-centred window as before. The header
// is taken out of the same byte budget.
func (s *Service) rerankCandidateContent(node *storage.Node, result rrfResult, query string, maxBytes int) string {
	header := rerankContextHeader(node, rerankContextProperties())
	remaining := maxBytes
	if header != "" {
		remaining -= len(header) + 1
	}
	var body string
	if chunkIndex, ok := matchingChunkIndex(string(node.ID), result.MatchID); ok {
		if chunks := managedChunkTexts(node); chunkIndex < len(chunks) && chunks[chunkIndex] != "" {
			body = expandedChunkWindow(chunks, chunkIndex, query, remaining)
		}
	}
	if body == "" {
		body = s.boundedNodeSearchableText(node, query, remaining)
	}
	if header == "" {
		return body
	}
	if body == "" {
		return header
	}
	return header + "\n" + body
}

func rerankContextProperties() []string {
	raw := envutil.Get(EnvSearchRerankContextProperties, defaultRerankContextProperties)
	parts := strings.Split(raw, ",")
	properties := parts[:0]
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			properties = append(properties, part)
		}
	}
	return properties
}

// rerankContextHeader joins the node's identifying property values, each
// bounded to rerankContextPropertyMaxBytes, in the configured order.
func rerankContextHeader(node *storage.Node, properties []string) string {
	if node == nil || len(properties) == 0 {
		return ""
	}
	var header strings.Builder
	for _, property := range properties {
		text := strings.TrimSpace(propertyToString(node.Properties[property]))
		if text == "" {
			continue
		}
		if header.Len() > 0 {
			header.WriteString(" | ")
		}
		header.WriteString(strings.TrimSpace(boundedUTF8Prefix(text, rerankContextPropertyMaxBytes)))
	}
	return header.String()
}

// expandedChunkWindow returns chunks[index] extended alternately with the
// following and preceding chunks while the result stays within maxBytes. A
// neighbour that does not fit whole is added as a bounded prefix (after) or
// suffix (before) to use the remaining budget, then expansion stops. Text the
// neighbouring chunks share through chunk overlap is added only once.
func expandedChunkWindow(chunks []string, index int, query string, maxBytes int) string {
	chunk := chunks[index]
	if len(chunk) >= maxBytes {
		return boundedQueryWindow(chunk, query, maxBytes)
	}
	window := chunk
	after, before := index+1, index-1
	for len(window) < maxBytes && (after < len(chunks) || before >= 0) {
		if after < len(chunks) {
			next := trimChunkOverlapPrefix(window, chunks[after])
			if room := maxBytes - len(window) - 1; room > 0 && next != "" {
				if len(next) > room {
					if next = strings.TrimSpace(boundedUTF8Prefix(next, room)); next != "" {
						window += " " + next
					}
					break
				}
				window += " " + next
			}
			after++
		}
		if before >= 0 && len(window) < maxBytes {
			prev := trimChunkOverlapSuffix(chunks[before], window)
			if room := maxBytes - len(window) - 1; room > 0 && prev != "" {
				if len(prev) > room {
					if prev = strings.TrimSpace(boundedUTF8Suffix(prev, room)); prev != "" {
						window = prev + " " + window
					}
					break
				}
				window = prev + " " + window
			}
			before--
		}
	}
	return strings.TrimSpace(window)
}

// trimChunkOverlapPrefix removes from next the longest prefix that is also a
// suffix of window (the chunker's overlap), so shared text is not repeated.
func trimChunkOverlapPrefix(window, next string) string {
	for size := min(len(window), len(next)); size > 0; size-- {
		if size < len(next) && !utf8.RuneStart(next[size]) {
			continue
		}
		if strings.HasSuffix(window, next[:size]) {
			return strings.TrimSpace(next[size:])
		}
	}
	return strings.TrimSpace(next)
}

// trimChunkOverlapSuffix removes from prev the longest suffix that is also a
// prefix of window.
func trimChunkOverlapSuffix(prev, window string) string {
	for size := min(len(prev), len(window)); size > 0; size-- {
		start := len(prev) - size
		if !utf8.RuneStart(prev[start]) {
			continue
		}
		if strings.HasPrefix(window, prev[start:]) {
			return strings.TrimSpace(prev[:start])
		}
	}
	return strings.TrimSpace(prev)
}

func matchingChunkIndex(nodeID, matchID string) (int, bool) {
	if matchID == "" {
		return 0, false
	}
	prefix := nodeID + "-chunk-"
	if strings.HasPrefix(matchID, prefix) {
		index, err := strconv.Atoi(strings.TrimPrefix(matchID, prefix))
		return index, err == nil && index >= 0
	}
	if matchID == nodeID {
		return 0, true
	}
	return 0, false
}

// managedChunkTexts returns the chunk texts stored on the node by the embedding
// worker, in chunk order, or nil when the node has none.
func managedChunkTexts(node *storage.Node) []string {
	if node == nil || node.EmbedMeta == nil {
		return nil
	}
	switch chunks := node.EmbedMeta["chunk_texts"].(type) {
	case []string:
		return chunks
	case []any:
		texts := make([]string, len(chunks))
		for i, chunk := range chunks {
			texts[i], _ = chunk.(string)
		}
		return texts
	}
	return nil
}

func (s *Service) boundedNodeSearchableText(node *storage.Node, query string, maxBytes int) string {
	values := s.searchableTextValues(node)
	for _, value := range values {
		if position := firstQueryTermPosition(value, query); position >= 0 {
			return boundedUTF8Window(value, position, maxBytes)
		}
	}

	var result strings.Builder
	result.Grow(maxBytes)
	for _, value := range values {
		if value == "" {
			continue
		}
		remaining := maxBytes - result.Len()
		if result.Len() > 0 {
			if remaining <= 1 {
				break
			}
			result.WriteByte(' ')
			remaining--
		}
		result.WriteString(boundedUTF8Prefix(value, remaining))
		if result.Len() >= maxBytes {
			break
		}
	}
	return strings.TrimSpace(result.String())
}

func (s *Service) searchableTextValues(node *storage.Node) []string {
	if node == nil {
		return nil
	}
	properties := s.FulltextProperties()
	if len(properties) > 0 {
		values := make([]string, 0, len(properties))
		for _, property := range properties {
			if text := propertyToString(node.Properties[property]); text != "" {
				values = append(values, text)
			}
		}
		return values
	}

	values := make([]string, 0, len(node.Labels)+len(node.Properties)*2)
	values = append(values, node.Labels...)
	for _, property := range SearchableProperties {
		if text := propertyToString(node.Properties[property]); text != "" {
			values = append(values, text)
		}
	}
	for key, value := range node.Properties {
		if _, priority := searchablePropertiesSet[key]; priority {
			continue
		}
		if text := propertyToString(value); text != "" {
			values = append(values, key, text)
		}
	}
	return values
}

func boundedQueryWindow(text, query string, maxBytes int) string {
	if position := firstQueryTermPosition(text, query); position >= 0 {
		return boundedUTF8Window(text, position, maxBytes)
	}
	return boundedUTF8Prefix(text, maxBytes)
}

func firstQueryTermPosition(text, query string) int {
	terms := strings.Fields(query)
	for _, term := range terms {
		term = strings.Trim(term, " ,.;:!?()[]{}\"'")
		if term == "" {
			continue
		}
		if position := strings.Index(text, term); position >= 0 {
			return position
		}
	}
	for _, term := range terms {
		term = strings.Trim(term, " ,.;:!?()[]{}\"'")
		if position := indexEqualFold(text, term); position >= 0 {
			return position
		}
	}
	return -1
}

func indexEqualFold(text, term string) int {
	if term == "" {
		return -1
	}
	for position := range text {
		end := position + len(term)
		if end <= len(text) && strings.EqualFold(text[position:end], term) {
			return position
		}
	}
	return -1
}

func boundedUTF8Window(text string, position, maxBytes int) string {
	if len(text) <= maxBytes {
		return strings.TrimSpace(text)
	}
	if maxBytes <= 0 {
		return ""
	}
	start := position - maxBytes/3
	if start < 0 {
		start = 0
	}
	if start+maxBytes > len(text) {
		start = len(text) - maxBytes
	}
	for start < len(text) && !utf8.RuneStart(text[start]) {
		start++
	}
	if start > 0 {
		for start < position && start < len(text) && !isRerankBoundary(text[start]) {
			start++
		}
		for start < position && start < len(text) && isRerankBoundary(text[start]) {
			start++
		}
	}
	end := min(start+maxBytes, len(text))
	for end > start && end < len(text) && !utf8.RuneStart(text[end]) {
		end--
	}
	if end < len(text) {
		for end > position && !isRerankBoundary(text[end-1]) {
			end--
		}
	}
	return strings.TrimSpace(text[start:end])
}

func isRerankBoundary(value byte) bool {
	return value == ' ' || value == '\n' || value == '\r' || value == '\t'
}

func boundedUTF8Prefix(text string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(text) <= maxBytes {
		return text
	}
	end := maxBytes
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:end]
}

func boundedUTF8Suffix(text string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(text) <= maxBytes {
		return text
	}
	start := len(text) - maxBytes
	for start < len(text) && !utf8.RuneStart(text[start]) {
		start++
	}
	return text[start:]
}
