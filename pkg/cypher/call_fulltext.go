package cypher

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// ========================================
// Neo4j Fulltext Index Procedures (CRITICAL for Mimir)
// ========================================

// callDbIndexFulltextQueryNodes implements db.index.fulltext.queryNodes
// Syntax: CALL db.index.fulltext.queryNodes('indexName', query) YIELD node, score
//
// This is the primary text search procedure used by Mimir for:
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
	// These use the default searchable properties for Mimir compatibility
	builtInIndexes := map[string]bool{
		"default":     true,
		"node_search": true, // Used by Mimir's UnifiedSearchService
	}

	// Neo4j compatibility: error if index doesn't exist and isn't a built-in
	if len(targetProperties) == 0 && indexName != "" && !builtInIndexes[indexName] {
		return nil, fmt.Errorf("there is no such fulltext schema index: %s", indexName)
	}

	// Default searchable properties if no index config or using built-in index
	// Includes Mimir-specific properties: path, workerRole, requirements
	if len(targetProperties) == 0 {
		targetProperties = []string{"content", "text", "title", "name", "description", "body", "summary", "path", "workerRole", "requirements"}
	}

	// Parse query into terms (supports basic AND/OR/NOT)
	queryTerms, excludeTerms, mustHaveTerms := parseFulltextQuery(query)
	if len(queryTerms) == 0 && len(mustHaveTerms) == 0 {
		return result, nil
	}

	// Get all nodes
	nodes, err := e.storage.AllNodes()
	if err != nil {
		return nil, err
	}

	// Calculate IDF for all terms (for BM25-like scoring)
	docFreq := make(map[string]int)
	totalDocs := 0
	for _, node := range nodes {
		if !matchesLabels(node, targetLabels) {
			continue
		}
		totalDocs++

		// Count documents containing each term
		content := extractTextContent(node, targetProperties)
		contentLower := strings.ToLower(content)

		allTerms := append(queryTerms, mustHaveTerms...)
		for _, term := range allTerms {
			if strings.Contains(contentLower, term) {
				docFreq[term]++
			}
		}
	}

	// Score each node
	type scoredNode struct {
		node  *storage.Node
		score float64
	}
	var scoredNodes []scoredNode

	for _, node := range nodes {
		// Check label filter
		if !matchesLabels(node, targetLabels) {
			continue
		}

		// Get searchable content
		content := extractTextContent(node, targetProperties)
		if content == "" {
			continue
		}
		contentLower := strings.ToLower(content)

		// Check exclude terms
		shouldExclude := false
		for _, term := range excludeTerms {
			if strings.Contains(contentLower, term) {
				shouldExclude = true
				break
			}
		}
		if shouldExclude {
			continue
		}

		// Check must-have terms
		hasMustHave := true
		for _, term := range mustHaveTerms {
			if !strings.Contains(contentLower, term) {
				hasMustHave = false
				break
			}
		}
		if !hasMustHave {
			continue
		}

		// Calculate BM25-like score
		score := calculateBM25Score(contentLower, queryTerms, docFreq, totalDocs)

		// Boost for must-have terms
		for _, term := range mustHaveTerms {
			if strings.Contains(contentLower, term) {
				score += 2.0
			}
		}

		if score > 0 {
			scoredNodes = append(scoredNodes, scoredNode{node: node, score: score})
		}
	}

	// Sort by score descending
	sort.Slice(scoredNodes, func(i, j int) bool {
		return scoredNodes[i].score > scoredNodes[j].score
	})

	// Convert to result rows
	for _, sn := range scoredNodes {
		result.Rows = append(result.Rows, []interface{}{
			sn.node,
			sn.score,
		})
	}

	return result, nil
}

// extractFulltextParams extracts index name and query from a fulltext CALL statement
func (e *StorageExecutor) extractFulltextParams(cypher string) (indexName, query string) {
	indexName = "default"

	// Find the procedure call
	upper := strings.ToUpper(cypher)
	callIdx := strings.Index(upper, "DB.INDEX.FULLTEXT.QUERYNODES")
	if callIdx == -1 {
		return "", ""
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
		indexName = strings.Trim(strings.TrimSpace(parts[0]), "'\"")
	}

	if len(parts) >= 2 {
		query = strings.Trim(strings.TrimSpace(parts[1]), "'\"")
	}

	return indexName, query
}

// parseFulltextQuery parses a fulltext query into regular terms, exclude terms, and must-have terms
func parseFulltextQuery(query string) (terms, excludeTerms, mustHaveTerms []string) {
	query = strings.ToLower(query)

	// Handle quoted phrases using pre-compiled pattern from regex_patterns.go
	phrases := fulltextPhrasePattern.FindAllStringSubmatch(query, -1)
	for _, match := range phrases {
		mustHaveTerms = append(mustHaveTerms, match[1])
	}
	query = fulltextPhrasePattern.ReplaceAllString(query, "")

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
