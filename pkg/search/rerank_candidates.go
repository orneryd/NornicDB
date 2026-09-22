package search

import (
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	// EnvSearchRerankMaxDocumentChars bounds each candidate sent to a reranker,
	// in characters (Unicode code points), so the same setting yields the same
	// amount of text for Latin, Cyrillic or CJK content.
	EnvSearchRerankMaxDocumentChars = "NORNICDB_SEARCH_RERANK_MAX_DOCUMENT_CHARS"
	// EnvSearchRerankMaxDocumentBytes is the deprecated predecessor of
	// EnvSearchRerankMaxDocumentChars. It is honoured when the new variable is
	// unset and its value is interpreted as a character count.
	EnvSearchRerankMaxDocumentBytes = "NORNICDB_SEARCH_RERANK_MAX_DOCUMENT_BYTES"
	defaultRerankMaxDocumentChars   = 2048
	// EnvSearchRerankContextProperties lists the short identifying properties
	// (comma-separated) placed in front of every rerank candidate so the
	// reranker knows which document a passage belongs to.
	EnvSearchRerankContextProperties = "NORNICDB_SEARCH_RERANK_CONTEXT_PROPERTIES"
	defaultRerankContextProperties   = "title,name"
	// rerankContextPropertyMaxChars bounds each identifying property value.
	rerankContextPropertyMaxChars = 256
)

func effectiveRerankMaxChars(opts *SearchOptions) int {
	if opts != nil && opts.RerankMaxChars > 0 {
		return opts.RerankMaxChars
	}
	value := envutil.GetInt(EnvSearchRerankMaxDocumentChars, 0)
	if value <= 0 {
		value = envutil.GetInt(EnvSearchRerankMaxDocumentBytes, defaultRerankMaxDocumentChars)
	}
	if value <= 0 {
		return defaultRerankMaxDocumentChars
	}
	return value
}

func charLen(text string) int {
	return utf8.RuneCountInString(text)
}

// rerankCandidateContent builds the text a reranker sees for one candidate:
// the node's identifying properties (title/name by default) followed by the
// passage. For a managed-embedding hit the passage is the matched chunk
// extended with its neighbouring chunks until maxChars is reached, so the
// reranker judges the document around the match instead of one small chunk;
// for a lexical-only hit it is the query-centred window as before. The header
// is taken out of the same character budget.
func (s *Service) rerankCandidateContent(node *storage.Node, result rrfResult, query string, maxChars int) string {
	header := rerankContextHeader(node, rerankContextProperties())
	remaining := maxChars
	if header != "" {
		remaining -= charLen(header) + 1
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
// bounded to rerankContextPropertyMaxChars, in the configured order.
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
		header.WriteString(strings.TrimSpace(boundedPrefix(text, rerankContextPropertyMaxChars)))
	}
	return header.String()
}

// expandedChunkWindow returns chunks[index] extended alternately with the
// following and preceding chunks while the result stays within maxChars. A
// neighbour that does not fit whole is added as a bounded prefix (after) or
// suffix (before) to use the remaining budget, then expansion stops. Text the
// neighbouring chunks share through chunk overlap is added only once.
func expandedChunkWindow(chunks []string, index int, query string, maxChars int) string {
	chunk := chunks[index]
	if charLen(chunk) >= maxChars {
		return boundedQueryWindow(chunk, query, maxChars)
	}
	window := chunk
	used := charLen(window)
	after, before := index+1, index-1
	for used < maxChars && (after < len(chunks) || before >= 0) {
		if after < len(chunks) {
			next := trimChunkOverlapPrefix(window, chunks[after])
			if room := maxChars - used - 1; room > 0 && next != "" {
				if charLen(next) > room {
					if next = strings.TrimSpace(boundedPrefix(next, room)); next != "" {
						window += " " + next
					}
					break
				}
				window += " " + next
				used += 1 + charLen(next)
			}
			after++
		}
		if before >= 0 && used < maxChars {
			prev := trimChunkOverlapSuffix(chunks[before], window)
			if room := maxChars - used - 1; room > 0 && prev != "" {
				if charLen(prev) > room {
					if prev = strings.TrimSpace(boundedSuffix(prev, room)); prev != "" {
						window = prev + " " + window
					}
					break
				}
				window = prev + " " + window
				used += 1 + charLen(prev)
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

func (s *Service) boundedNodeSearchableText(node *storage.Node, query string, maxChars int) string {
	values := s.searchableTextValues(node)
	for _, value := range values {
		if position := firstQueryTermPosition(value, query); position >= 0 {
			return boundedWindow(value, position, maxChars)
		}
	}

	var result strings.Builder
	result.Grow(maxChars)
	used := 0
	for _, value := range values {
		if value == "" {
			continue
		}
		remaining := maxChars - used
		if used > 0 {
			if remaining <= 1 {
				break
			}
			result.WriteByte(' ')
			used++
			remaining--
		}
		part := boundedPrefix(value, remaining)
		result.WriteString(part)
		used += charLen(part)
		if used >= maxChars {
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

func boundedQueryWindow(text, query string, maxChars int) string {
	if position := firstQueryTermPosition(text, query); position >= 0 {
		return boundedWindow(text, position, maxChars)
	}
	return boundedPrefix(text, maxChars)
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

// byteOffsetAfterChars returns the byte offset in text that lies count
// characters after the byte offset from.
func byteOffsetAfterChars(text string, from, count int) int {
	offset := from
	for count > 0 && offset < len(text) {
		offset++
		for offset < len(text) && !utf8.RuneStart(text[offset]) {
			offset++
		}
		count--
	}
	return offset
}

// byteOffsetBeforeChars returns the byte offset in text that lies count
// characters before the byte offset from.
func byteOffsetBeforeChars(text string, from, count int) int {
	offset := from
	for count > 0 && offset > 0 {
		offset--
		for offset > 0 && !utf8.RuneStart(text[offset]) {
			offset--
		}
		count--
	}
	return offset
}

// boundedWindow returns at most maxChars characters of text around the byte
// offset position (about a third of the window before it), trimmed to word
// boundaries where possible. It walks only the window, not the whole text.
func boundedWindow(text string, position, maxChars int) string {
	if maxChars <= 0 {
		return ""
	}
	if len(text) <= maxChars {
		return strings.TrimSpace(text)
	}
	// A character is at most utf8.UTFMax bytes, so a longer text cannot fit;
	// only texts in between need the character walk.
	if len(text) <= maxChars*utf8.UTFMax && byteOffsetAfterChars(text, 0, maxChars) >= len(text) {
		return strings.TrimSpace(text)
	}
	start := byteOffsetBeforeChars(text, position, maxChars/3)
	end := byteOffsetAfterChars(text, start, maxChars)
	if end >= len(text) {
		end = len(text)
		start = byteOffsetBeforeChars(text, end, maxChars)
	}
	if start > 0 {
		for start < position && start < len(text) && !isRerankBoundary(text[start]) {
			start++
		}
		for start < position && start < len(text) && isRerankBoundary(text[start]) {
			start++
		}
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

// boundedPrefix returns the first maxChars characters of text.
func boundedPrefix(text string, maxChars int) string {
	if maxChars <= 0 {
		return ""
	}
	return text[:byteOffsetAfterChars(text, 0, maxChars)]
}

// boundedSuffix returns the last maxChars characters of text.
func boundedSuffix(text string, maxChars int) string {
	if maxChars <= 0 {
		return ""
	}
	if len(text) <= maxChars {
		return text
	}
	return text[byteOffsetBeforeChars(text, len(text), maxChars):]
}
