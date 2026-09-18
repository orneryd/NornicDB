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
	defaultRerankMaxDocumentBytes   = 4096
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

func (s *Service) rerankCandidateContent(node *storage.Node, result rrfResult, query string, maxBytes int) string {
	if chunkIndex, ok := matchingChunkIndex(string(node.ID), result.MatchID); ok {
		if chunk := managedChunkText(node, chunkIndex); chunk != "" {
			return boundedQueryWindow(chunk, query, maxBytes)
		}
	}
	return s.boundedNodeSearchableText(node, query, maxBytes)
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

func managedChunkText(node *storage.Node, index int) string {
	if node == nil || index < 0 || node.EmbedMeta == nil {
		return ""
	}
	switch chunks := node.EmbedMeta["chunk_texts"].(type) {
	case []string:
		if index < len(chunks) {
			return chunks[index]
		}
	case []any:
		if index < len(chunks) {
			text, _ := chunks[index].(string)
			return text
		}
	}
	return ""
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
