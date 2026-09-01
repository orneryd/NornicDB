package cypher

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ========================================
// Neo4j Fulltext Index Procedures
// ========================================

// callDbIndexFulltextQueryNodes implements db.index.fulltext.queryNodes
// Syntax: CALL db.index.fulltext.queryNodes('indexName', query) YIELD node, score
//
// This is the primary text search procedure for:
//   - Keyword-based memory search
//   - Content discovery
//   - Text matching across node properties
//
// Parameters:
//   - indexName: Name of the fulltext index (from CREATE FULLTEXT INDEX)
//   - query: Search query string (supports AND, OR, NOT, wildcards)
//
// Returns:
//   - node: The matched node with all properties
//   - score: BM25-like relevance score
//
// Scoring Algorithm:
//
//	Uses a simplified BM25-like scoring that considers:
//	- Term frequency (TF): How often query terms appear
//	- Inverse document frequency (IDF): How rare terms are
//	- Field length normalization: Shorter fields score higher
func (e *StorageExecutor) callDbIndexFulltextQueryNodes(cypher string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{"node", "score"},
		Rows:    [][]interface{}{},
	}
	opts, err := e.extractFulltextQueryOptions(cypher)
	if err != nil {
		return nil, err
	}

	// Extract query string and index name
	indexName, query := e.extractFulltextParams(cypher)
	if query == "" {
		return result, nil
	}

	// Get index configuration if it exists
	var targetLabels []string
	var targetProperties []string

	schema := e.storage.GetSchema()
	if schema != nil {
		if ftIdx, exists := schema.GetFulltextIndex(indexName); exists {
			targetLabels = ftIdx.Labels
			targetProperties = ftIdx.Properties
		}
	}

	// Known built-in index names that don't require explicit creation
	// These use the default searchable properties for Neo4j compatibility
	builtInIndexes := map[string]bool{
		"default":     true,
		"node_search": true, // Built-in fulltext search index
	}

	// Neo4j compatibility: error if index doesn't exist and isn't a built-in
	if len(targetProperties) == 0 && indexName != "" && !builtInIndexes[indexName] {
		return nil, localizedError(localization.CypherSpecializedCallsFulltextIndexNotFound(indexName), nil)
	}

	// Default searchable properties if no index config or using built-in index
	// Includes additional searchable properties: path, workerRole, requirements
	if len(targetProperties) == 0 {
		targetProperties = []string{"content", "text", "title", "name", "description", "body", "summary", "path", "workerRole", "requirements"}
	}

	// Lucene wildcard family. Three Neo4j-compatible shapes resolve here:
	//
	//   *        — MatchAllDocsQuery; every doc in the index.
	//   *:*      — Solr/Lucene field-and-value wildcard; equivalent to *.
	//   <prop>:* — Field-presence query; every doc that has a value for
	//              <prop>. The property must be one the index declared,
	//              otherwise the query returns nothing (matching Neo4j
	//              behavior — an undeclared field has no postings list).
	//
	// All three branches still apply targetLabels — without that filter,
	// the index ignores its declared FOR (m:Memory) scope and returns
	// every node in the database. This was the May 2026 mcp-neo4j-memory
	// Bug 2: fulltext index ignored its declared label scope.
	if isFulltextWildcard(query) {
		nodes, err := e.storage.AllNodes()
		if err != nil {
			return nil, err
		}
		for _, node := range nodes {
			if !matchesLabels(node, targetLabels) {
				continue
			}
			// Skip nodes with no content in any of the indexed properties.
			// Without this, a wildcard query against an index declared on
			// e.g. `m.name, m.type` would still surface unrelated labels
			// that happen to share a property name.
			if extractTextContent(node, targetProperties) == "" {
				continue
			}
			result.Rows = append(result.Rows, []interface{}{node, 1.0})
		}
		applyFulltextOptions(result, opts)
		return result, nil
	}
	if propName, ok := fulltextFieldPresenceQuery(query); ok {
		// Reject queries that ask for a property the index didn't declare.
		// Neo4j-Lucene treats an undeclared field as having no postings,
		// so the result is empty. We match that by silently returning no
		// rows when propName isn't in targetProperties.
		if !containsString(targetProperties, propName) {
			return result, nil
		}
		nodes, err := e.storage.AllNodes()
		if err != nil {
			return nil, err
		}
		for _, node := range nodes {
			if !matchesLabels(node, targetLabels) {
				continue
			}
			if !nodeHasNonEmptyProperty(node, propName) {
				continue
			}
			result.Rows = append(result.Rows, []interface{}{node, 1.0})
		}
		applyFulltextOptions(result, opts)
		return result, nil
	}

	// Parse the query using the full Lucene-classic parser. Composite
	// queries like `group_id:"g" AND (term)` require this so the
	// parenthesized default-field clause is intersected with the field-
	// scoped clause rather than silently discarded (the pre-parser bug).
	parsed, err := ParseFulltextQuery(query)
	if err != nil {
		return nil, err
	}
	if parsed.IsEmpty() {
		return result, nil
	}

	nodes, err := e.storage.AllNodes()
	if err != nil {
		return nil, err
	}

	// First pass: build the per-doc content strings and populate docFreq
	// for BM25 IDF, restricted to the terms the parser actually scores.
	type nodeDoc struct {
		node *storage.Node
		doc  *ftDoc
	}
	docs := make([]nodeDoc, 0, len(nodes))
	docFreq := make(map[string]int, len(parsed.PrimaryTerms()))
	var totalDocLen int64
	for _, node := range nodes {
		if !matchesLabels(node, targetLabels) {
			continue
		}
		doc := buildNodeFulltextDoc(node, targetProperties)
		if doc == nil {
			continue
		}
		docs = append(docs, nodeDoc{node: node, doc: doc})
		totalDocLen += int64(doc.contentTokenN)
		for _, term := range parsed.PrimaryTerms() {
			if strings.Contains(doc.contentLower, term) {
				docFreq[term]++
			}
		}
	}
	totalDocs := len(docs)
	avgDocLen := 100.0
	if totalDocs > 0 {
		avgDocLen = float64(totalDocLen) / float64(totalDocs)
	}

	ctx := &ftEvalCtx{
		DefaultFields: targetProperties,
		avgDocLen:     avgDocLen,
		totalDocs:     totalDocs,
		docFreq:       docFreq,
	}

	type scoredNode struct {
		node  *storage.Node
		score float64
	}
	scoredNodes := make([]scoredNode, 0, len(docs))
	for _, d := range docs {
		matched, score := parsed.Match(ctx, d.doc)
		if !matched {
			continue
		}
		if score <= 0 {
			// A pure field-scoped or presence match still deserves a positive
			// score so the doc surfaces in the result. Use 1.0 as the floor
			// (matches the wildcard/presence fast-paths above).
			score = 1.0
		}
		scoredNodes = append(scoredNodes, scoredNode{node: d.node, score: score})
	}

	sort.Slice(scoredNodes, func(i, j int) bool {
		if scoredNodes[i].score == scoredNodes[j].score {
			return scoredNodes[i].node.ID < scoredNodes[j].node.ID
		}
		return scoredNodes[i].score > scoredNodes[j].score
	})

	for _, sn := range scoredNodes {
		result.Rows = append(result.Rows, []interface{}{sn.node, sn.score})
	}
	applyFulltextOptions(result, opts)

	return result, nil
}

// buildNodeFulltextDoc canonicalizes a node into an ftDoc: every declared
// index property is lower-cased into the properties map, all property
// values are concatenated into contentLower for BM25 scoring, and the
// token count is captured for length normalization. Returns nil if the
// node has no content in any indexed property (empty-doc filter is
// preserved from the pre-parser path).
func buildNodeFulltextDoc(node *storage.Node, properties []string) *ftDoc {
	if node == nil {
		return nil
	}
	props := make(map[string]string, len(node.Properties))
	rawProps := make(map[string]string, len(node.Properties))
	for k, v := range node.Properties {
		if v == nil {
			continue
		}
		raw := fmt.Sprintf("%v", v)
		if raw == "" {
			continue
		}
		props[strings.ToLower(k)] = strings.ToLower(raw)
		rawProps[strings.ToLower(k)] = raw
	}
	content := extractTextContent(node, properties)
	if content == "" {
		return nil
	}
	contentLower := strings.ToLower(content)
	return &ftDoc{
		properties:    props,
		rawProperties: rawProps,
		contentLower:  contentLower,
		contentTokenN: len(strings.Fields(content)),
	}
}

// extractFulltextParams extracts index name and query from a fulltext CALL statement
func (e *StorageExecutor) extractFulltextParams(cypher string) (indexName, query string) {
	indexName = "default"

	// Find the procedure call
	upper := strings.ToUpper(cypher)
	callIdx := strings.Index(upper, "DB.INDEX.FULLTEXT.QUERYNODES")
	if callIdx == -1 {
		callIdx = strings.Index(upper, "DB.INDEX.FULLTEXT.QUERYRELATIONSHIPS")
		if callIdx == -1 {
			return "", ""
		}
	}

	// Find the opening parenthesis
	rest := cypher[callIdx:]
	parenIdx := strings.Index(rest, "(")
	if parenIdx == -1 {
		return "", ""
	}

	// Find matching closing parenthesis
	parenContent := rest[parenIdx+1:]
	depth := 1
	endIdx := -1
	for i, c := range parenContent {
		if c == '(' {
			depth++
		} else if c == ')' {
			depth--
			if depth == 0 {
				endIdx = i
				break
			}
		}
	}
	if endIdx == -1 {
		return "", ""
	}

	params := parenContent[:endIdx]
	parts := splitParamsCarefully(params)

	if len(parts) >= 1 {
		indexName = decodeCypherStringLiteral(strings.TrimSpace(parts[0]))
	}

	if len(parts) >= 2 {
		query = decodeCypherStringLiteral(strings.TrimSpace(parts[1]))
	}

	return indexName, query
}

// decodeCypherStringLiteral strips surrounding single or double quotes from a
// Cypher string literal and undoes the standard backslash-escape encoding
// (`\\` -> `\`, `\'` -> `'`, `\"` -> `"`, `\n`, `\t`, `\r`). This is what
// parameter substitution produces in valueToLiteral for values that end up
// as the fulltext query string; without this decode step a user query of
// `Cloud\Trail` reaches the Lucene parser as `Cloud\\Trail`, defeating the
// escape rule.
func decodeCypherStringLiteral(s string) string {
	if len(s) < 2 {
		return s
	}
	first, last := s[0], s[len(s)-1]
	if (first != '\'' && first != '"') || first != last {
		return s
	}
	inner := s[1 : len(s)-1]
	var b strings.Builder
	b.Grow(len(inner))
	for i := 0; i < len(inner); i++ {
		c := inner[i]
		if c == '\\' && i+1 < len(inner) {
			next := inner[i+1]
			switch next {
			case '\\', '\'', '"':
				b.WriteByte(next)
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			case 'r':
				b.WriteByte('\r')
			default:
				// Unknown escape — preserve both characters so the Lucene
				// parser can apply its own rules (e.g. `\T` should still be
				// literal T after Lucene-level decoding).
				b.WriteByte('\\')
				b.WriteByte(next)
			}
			i++
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

type fulltextQueryOptions struct {
	skip  int
	limit int
}

func (e *StorageExecutor) extractFulltextQueryOptions(cypher string) (fulltextQueryOptions, error) {
	opts := fulltextQueryOptions{skip: 0, limit: -1}

	upper := strings.ToUpper(cypher)
	callIdx := strings.Index(upper, "DB.INDEX.FULLTEXT.QUERYNODES")
	if callIdx == -1 {
		callIdx = strings.Index(upper, "DB.INDEX.FULLTEXT.QUERYRELATIONSHIPS")
		if callIdx == -1 {
			return opts, nil
		}
	}

	rest := cypher[callIdx:]
	parenIdx := strings.Index(rest, "(")
	if parenIdx == -1 {
		return opts, nil
	}

	parenContent := rest[parenIdx+1:]
	depth := 1
	endIdx := -1
	for i, c := range parenContent {
		if c == '(' {
			depth++
		} else if c == ')' {
			depth--
			if depth == 0 {
				endIdx = i
				break
			}
		}
	}
	if endIdx == -1 {
		return opts, nil
	}

	params := parenContent[:endIdx]
	parts := splitParamsCarefully(params)
	if len(parts) < 3 {
		return opts, nil
	}
	if len(parts) > 3 {
		return opts, localizedError(localization.CypherSpecializedCallsFulltextArgumentCount(), nil)
	}

	raw := strings.TrimSpace(parts[2])
	if raw == "" {
		return opts, nil
	}
	if !strings.HasPrefix(raw, "{") || !strings.HasSuffix(raw, "}") {
		return opts, localizedError(localization.CypherSpecializedCallsProcedureOptionsMapRequired(), nil)
	}

	parsed := e.parseMapLiteral(context.Background(), raw)
	if v, ok := parsed["skip"]; ok {
		i, convOK := fulltextOptionToInt(v)
		if !convOK || i < 0 {
			return opts, localizedError(localization.CypherSpecializedCallsFulltextOptionInvalid("skip", v), nil)
		}
		opts.skip = i
	}
	if v, ok := parsed["limit"]; ok {
		i, convOK := fulltextOptionToInt(v)
		if !convOK || i < 0 {
			return opts, localizedError(localization.CypherSpecializedCallsFulltextOptionInvalid("limit", v), nil)
		}
		opts.limit = i
	}

	return opts, nil
}

func fulltextOptionToInt(v interface{}) (int, bool) {
	switch t := v.(type) {
	case int:
		return t, true
	case int64:
		return int(t), true
	case float64:
		return int(t), float64(int(t)) == t
	case float32:
		return int(t), float32(int(t)) == t
	case string:
		i, err := strconv.Atoi(strings.TrimSpace(t))
		return i, err == nil
	default:
		return 0, false
	}
}

func applyFulltextOptions(result *ExecuteResult, opts fulltextQueryOptions) {
	if result == nil || len(result.Rows) == 0 {
		return
	}
	if opts.skip > 0 {
		if opts.skip >= len(result.Rows) {
			result.Rows = [][]interface{}{}
			return
		}
		result.Rows = result.Rows[opts.skip:]
	}
	if opts.limit >= 0 && opts.limit < len(result.Rows) {
		result.Rows = result.Rows[:opts.limit]
	}
}

func appendFulltextOptionedRow(result *ExecuteResult, opts fulltextQueryOptions, seen *int, row []interface{}) bool {
	if result == nil || seen == nil {
		return false
	}
	current := *seen
	*seen = current + 1
	if current < opts.skip {
		return false
	}
	if opts.limit >= 0 && len(result.Rows) >= opts.limit {
		return true
	}
	result.Rows = append(result.Rows, row)
	return opts.limit >= 0 && len(result.Rows) >= opts.limit
}

// parseFulltextQuery parses a fulltext query into regular terms, exclude terms, and must-have terms
func parseFulltextQuery(query string) (terms, excludeTerms, mustHaveTerms []string) {
	query = strings.ToLower(query)

	// Handle quoted phrases.
	var phrases []string
	query, phrases = extractQuotedPhrasesAndStrip(query)
	mustHaveTerms = append(mustHaveTerms, phrases...)

	// Split by spaces and operators
	words := strings.Fields(query)

	for i := 0; i < len(words); i++ {
		word := words[i]

		// Handle NOT operator
		if word == "not" && i+1 < len(words) {
			excludeTerms = append(excludeTerms, words[i+1])
			i++
			continue
		}

		// Handle - prefix for exclusion
		if strings.HasPrefix(word, "-") && len(word) > 1 {
			excludeTerms = append(excludeTerms, word[1:])
			continue
		}

		// Handle + prefix for required
		if strings.HasPrefix(word, "+") && len(word) > 1 {
			mustHaveTerms = append(mustHaveTerms, word[1:])
			continue
		}

		// Skip AND/OR operators
		if word == "and" || word == "or" {
			continue
		}

		// Regular term
		if len(word) > 0 {
			terms = append(terms, word)
		}
	}

	return terms, excludeTerms, mustHaveTerms
}

func extractQuotedPhrasesAndStrip(s string) (stripped string, phrases []string) {
	var out strings.Builder
	out.Grow(len(s))

	inQuote := false
	phraseStart := 0

	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '"' && (i == 0 || s[i-1] != '\\') {
			if !inQuote {
				inQuote = true
				phraseStart = i + 1
			} else {
				inQuote = false
				if phraseStart <= i {
					phrases = append(phrases, s[phraseStart:i])
				}
			}
			// Preserve token spacing where phrase used to be.
			out.WriteByte(' ')
			continue
		}
		if !inQuote {
			out.WriteByte(ch)
		}
	}

	return out.String(), phrases
}

// isFulltextWildcard reports whether the query should be treated as
// the Lucene "match every document" pattern — bare "*" or "*:*". An
// empty query is handled separately by the caller (early-return with
// no rows). Trimming + lowercasing keeps `" * "`, `*:*`, and `*` all
// canonical.
func isFulltextWildcard(query string) bool {
	q := strings.ToLower(strings.TrimSpace(query))
	return q == "*" || q == "*:*"
}

// fulltextFieldPresenceQuery recognizes the Lucene field-presence
// shorthand `<prop>:*` (every document that has a non-empty value for
// the named property). Returns the property name and true on a match.
// The property name is preserved case-as-written so the caller can
// look it up in node.Properties without lowercasing.
//
// The full wildcard form ("*", "*:*") is recognized by isFulltextWildcard
// and short-circuited before this function is reached.
func fulltextFieldPresenceQuery(query string) (string, bool) {
	q := strings.TrimSpace(query)
	colon := strings.Index(q, ":")
	if colon <= 0 || colon == len(q)-1 {
		return "", false
	}
	name := strings.TrimSpace(q[:colon])
	value := strings.TrimSpace(q[colon+1:])
	if name == "" || value != "*" {
		return "", false
	}
	// Reject names that contain whitespace or operator characters; that
	// would mean the query is something like `name OR foo:*` (a more
	// complex Lucene expression that needs the BM25 path).
	for _, r := range name {
		if !(r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return "", false
		}
	}
	return name, true
}

// nodeHasNonEmptyProperty reports whether node carries a property
// named propName whose stringified value is non-empty. The "non-empty"
// check matches Neo4j-Lucene behavior: a field with an empty string
// value is treated as having no posting and is excluded from
// `<field>:*` queries.
func nodeHasNonEmptyProperty(node *storage.Node, propName string) bool {
	if node == nil {
		return false
	}
	val, ok := node.Properties[propName]
	if !ok || val == nil {
		return false
	}
	if s, ok := val.(string); ok && s == "" {
		return false
	}
	return true
}

// matchesLabels checks if a node has any of the target labels (empty = all match)
func matchesLabels(node *storage.Node, targetLabels []string) bool {
	if len(targetLabels) == 0 {
		return true
	}
	for _, nl := range node.Labels {
		for _, tl := range targetLabels {
			if nl == tl {
				return true
			}
		}
	}
	return false
}

// extractTextContent extracts searchable text content from a node
func extractTextContent(node *storage.Node, properties []string) string {
	var content strings.Builder

	for _, propName := range properties {
		if val, ok := node.Properties[propName]; ok {
			content.WriteString(fmt.Sprintf("%v ", val))
		}
	}

	return strings.TrimSpace(content.String())
}

// calculateBM25Score calculates a BM25-like score for a document
func calculateBM25Score(content string, terms []string, docFreq map[string]int, totalDocs int) float64 {
	if totalDocs == 0 {
		return 0
	}

	// BM25 parameters
	k1 := 1.2
	b := 0.75
	avgDocLen := 100.0 // Assume average document length

	docLen := float64(len(strings.Fields(content)))
	var score float64

	for _, term := range terms {
		tf := float64(strings.Count(content, term))
		if tf == 0 {
			continue
		}

		// IDF calculation using BM25 formula with smoothing
		df := float64(docFreq[term])
		if df == 0 {
			df = 0.5 // Smoothing for unseen terms
		}

		// Use IDF+ variant: log((N + 1) / df) to ensure positive IDF
		// This prevents common terms from having zero or negative IDF
		idf := math.Log((float64(totalDocs) + 1) / df)
		if idf < 0.1 {
			idf = 0.1 // Minimum IDF floor
		}

		// TF normalization
		tfNorm := (tf * (k1 + 1)) / (tf + k1*(1-b+b*(docLen/avgDocLen)))

		score += idf * tfNorm
	}

	return score
}

// extractFulltextQuery extracts the search query from a fulltext CALL statement (legacy)
func (e *StorageExecutor) extractFulltextQuery(cypher string) string {
	_, query := e.extractFulltextParams(cypher)
	return query
}
