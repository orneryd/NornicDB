// Package search provides APOC search functions.
//
// This package implements all apoc.search.* functions for full-text
// search and pattern matching in graph data.
package search

import (
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"regexp"
	"strings"

	"github.com/orneryd/nornicdb/apoc/storage"
)

// NodeType represents a graph node.
type NodeType = storage.Node

// Relationship represents a graph relationship.
type Relationship = storage.Relationship

// Storage is the interface for database operations.
var Storage storage.Storage = storage.NewInMemoryStorage()

// Node searches nodes by property value.
//
// Example:
//
//	apoc.search.node('Person', 'name', 'Alice') => matching nodes
func Node(label, property string, value interface{}) []*NodeType {
	nodes, err := Storage.AllNodes()
	if err != nil {
		return []*NodeType{}
	}

	results := make([]*NodeType, 0)
	for _, node := range nodes {
		if !hasLabel(node, label) {
			continue
		}
		if propVal, ok := node.Properties[property]; ok {
			if propVal == value {
				results = append(results, node)
			}
		}
	}
	return results
}

// NodeAll searches nodes matching all criteria.
//
// Example:
//
//	apoc.search.nodeAll('Person', {name: 'Alice', age: 30}) => matching nodes
func NodeAll(label string, criteria map[string]interface{}) []*NodeType {
	nodes, err := Storage.AllNodes()
	if err != nil {
		return []*NodeType{}
	}

	results := make([]*NodeType, 0)
	for _, node := range nodes {
		if !hasLabel(node, label) {
			continue
		}

		matches := true
		for key, value := range criteria {
			if propVal, ok := node.Properties[key]; !ok || propVal != value {
				matches = false
				break
			}
		}
		if matches {
			results = append(results, node)
		}
	}
	return results
}

// NodeAny searches nodes matching any criteria.
//
// Example:
//
//	apoc.search.nodeAny('Person', {name: 'Alice', name: 'Bob'}) => matching nodes
func NodeAny(label string, criteria map[string]interface{}) []*NodeType {
	nodes, err := Storage.AllNodes()
	if err != nil {
		return []*NodeType{}
	}

	results := make([]*NodeType, 0)
	for _, node := range nodes {
		if !hasLabel(node, label) {
			continue
		}

		for key, value := range criteria {
			if propVal, ok := node.Properties[key]; ok && propVal == value {
				results = append(results, node)
				break
			}
		}
	}
	return results
}

// NodeReduced searches with reduced results.
//
// Example:
//
//	apoc.search.nodeReduced('Person', {name: 'A*'}, 10) => top 10 matches
func NodeReduced(label string, criteria map[string]interface{}, limit int) []*NodeType {
	nodes := NodeAll(label, criteria)
	if len(nodes) > limit {
		return nodes[:limit]
	}
	return nodes
}

// MultiSearchAll searches multiple labels.
//
// Example:
//
//	apoc.search.multiSearchAll(['Person', 'Company'], {name: 'Alice'}) => matches
func MultiSearchAll(labels []string, criteria map[string]interface{}) []*NodeType {
	results := make([]*NodeType, 0)
	for _, label := range labels {
		nodes := NodeAll(label, criteria)
		results = append(results, nodes...)
	}
	return results
}

// MultiSearchAny searches multiple labels with any match.
//
// Example:
//
//	apoc.search.multiSearchAny(['Person', 'Company'], {name: 'Alice'}) => matches
func MultiSearchAny(labels []string, criteria map[string]interface{}) []*NodeType {
	return MultiSearchAll(labels, criteria)
}

// Parallel searches in parallel across labels.
//
// Example:
//
//	apoc.search.parallel(['Person', 'Company'], 'name', 'Alice') => matches
func Parallel(labels []string, property string, value interface{}) []*NodeType {
	// Placeholder - would execute parallel searches
	results := make([]*NodeType, 0)
	for _, label := range labels {
		nodes := Node(label, property, value)
		results = append(results, nodes...)
	}
	return results
}

// FullText performs full-text search.
//
// Example:
//
//	apoc.search.fullText('Person', 'name', 'Alice Bob') => matching nodes
func FullText(label, property, query string) []*NodeType {
	nodes, err := Storage.AllNodes()
	if err != nil {
		return []*NodeType{}
	}

	queryLower := strings.ToLower(query)
	words := strings.Fields(queryLower)

	results := make([]*NodeType, 0)
	for _, node := range nodes {
		if !hasLabel(node, label) {
			continue
		}
		if propVal, ok := node.Properties[property]; ok {
			if strVal, ok := propVal.(string); ok {
				strLower := strings.ToLower(strVal)
				for _, word := range words {
					if strings.Contains(strLower, word) {
						results = append(results, node)
						break
					}
				}
			}
		}
	}
	return results
}

// Fuzzy performs fuzzy search.
//
// Example:
//
//	apoc.search.fuzzy('Person', 'name', 'Alise', 2) => matches within edit distance 2
func Fuzzy(label, property, value string, maxDistance int) []*NodeType {
	nodes, err := Storage.AllNodes()
	if err != nil {
		return []*NodeType{}
	}

	valueLower := strings.ToLower(value)

	results := make([]*NodeType, 0)
	for _, node := range nodes {
		if !hasLabel(node, label) {
			continue
		}
		if propVal, ok := node.Properties[property]; ok {
			if strVal, ok := propVal.(string); ok {
				distance := levenshteinDistance(strings.ToLower(strVal), valueLower)
				if distance <= maxDistance {
					results = append(results, node)
				}
			}
		}
	}
	return results
}

// Regex searches using regular expressions.
//
// Example:
//
//	apoc.search.regex('Person', 'email', '.*@example\\.com') => matching nodes
func Regex(label, property, pattern string) []*NodeType {
	nodes, err := Storage.AllNodes()
	if err != nil {
		return []*NodeType{}
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return []*NodeType{}
	}

	results := make([]*NodeType, 0)
	for _, node := range nodes {
		if !hasLabel(node, label) {
			continue
		}
		if propVal, ok := node.Properties[property]; ok {
			if strVal, ok := propVal.(string); ok {
				if re.MatchString(strVal) {
					results = append(results, node)
				}
			}
		}
	}
	return results
}

// Prefix searches by prefix.
//
// Example:
//
//	apoc.search.prefix('Person', 'name', 'Ali') => names starting with 'Ali'
func Prefix(label, property, prefix string) []*NodeType {
	nodes, err := Storage.AllNodes()
	if err != nil {
		return []*NodeType{}
	}

	prefixLower := strings.ToLower(prefix)

	results := make([]*NodeType, 0)
	for _, node := range nodes {
		if !hasLabel(node, label) {
			continue
		}
		if propVal, ok := node.Properties[property]; ok {
			if strVal, ok := propVal.(string); ok {
				if strings.HasPrefix(strings.ToLower(strVal), prefixLower) {
					results = append(results, node)
				}
			}
		}
	}
	return results
}

// Suffix searches by suffix.
//
// Example:
//
//	apoc.search.suffix('Person', 'email', '@example.com') => emails ending with suffix
func Suffix(label, property, suffix string) []*NodeType {
	nodes, err := Storage.AllNodes()
	if err != nil {
		return []*NodeType{}
	}

	suffixLower := strings.ToLower(suffix)

	results := make([]*NodeType, 0)
	for _, node := range nodes {
		if !hasLabel(node, label) {
			continue
		}
		if propVal, ok := node.Properties[property]; ok {
			if strVal, ok := propVal.(string); ok {
				if strings.HasSuffix(strings.ToLower(strVal), suffixLower) {
					results = append(results, node)
				}
			}
		}
	}
	return results
}

// Contains searches for substring.
//
// Example:
//
//	apoc.search.contains('Person', 'name', 'lic') => names containing 'lic'
func Contains(label, property, substring string) []*NodeType {
	nodes, err := Storage.AllNodes()
	if err != nil {
		return []*NodeType{}
	}

	substringLower := strings.ToLower(substring)

	results := make([]*NodeType, 0)
	for _, node := range nodes {
		if !hasLabel(node, label) {
			continue
		}
		if propVal, ok := node.Properties[property]; ok {
			if strVal, ok := propVal.(string); ok {
				if strings.Contains(strings.ToLower(strVal), substringLower) {
					results = append(results, node)
				}
			}
		}
	}
	return results
}

// Range searches within a range.
//
// Example:
//
//	apoc.search.range('Person', 'age', 18, 65) => nodes with age 18-65
func Range(label, property string, minVal, maxVal interface{}) []*NodeType {
	nodes, err := Storage.AllNodes()
	if err != nil {
		return []*NodeType{}
	}

	results := make([]*NodeType, 0)
	for _, node := range nodes {
		if !hasLabel(node, label) {
			continue
		}
		if propVal, ok := node.Properties[property]; ok {
			if inRange(propVal, minVal, maxVal) {
				results = append(results, node)
			}
		}
	}
	return results
}

// inRange checks if value is within range
func inRange(value, minVal, maxVal interface{}) bool {
	if integer, ok := valueconvert.IntegerValue(value); ok {
		lower, lowerOK := toInt64(minVal)
		upper, upperOK := toInt64(maxVal)
		return lowerOK && upperOK && integer >= lower && integer <= upper
	}
	if floating, ok := valueconvert.NumericFloat64(value); ok {
		lower, lowerOK := toFloat64(minVal)
		upper, upperOK := toFloat64(maxVal)
		return lowerOK && upperOK && floating >= lower && floating <= upper
	}
	if text, ok := value.(string); ok {
		lower, lowerOK := minVal.(string)
		upper, upperOK := maxVal.(string)
		return lowerOK && upperOK && text >= lower && text <= upper
	}
	return false
}

func toInt(v interface{}) (int, bool) {
	return valueconvert.ToInt(v, valueconvert.IntegerFloat)
}

func toInt64(v interface{}) (int64, bool) {
	return valueconvert.Int64(v, valueconvert.IntegerFloat)
}

func toFloat64(v interface{}) (float64, bool) {
	return valueconvert.NumericFloat64(v)
}

// In searches for values in a list.
//
// Example:
//
//	apoc.search.in('Person', 'status', ['active', 'pending']) => matching nodes
func In(label, property string, values []interface{}) []*NodeType {
	nodes, err := Storage.AllNodes()
	if err != nil {
		return []*NodeType{}
	}

	valueSet := make(map[interface{}]bool)
	for _, v := range values {
		valueSet[v] = true
	}

	results := make([]*NodeType, 0)
	for _, node := range nodes {
		if !hasLabel(node, label) {
			continue
		}
		if propVal, ok := node.Properties[property]; ok {
			if valueSet[propVal] {
				results = append(results, node)
			}
		}
	}
	return results
}

// NotIn searches for values not in a list.
//
// Example:
//
//	apoc.search.notIn('Person', 'status', ['deleted', 'banned']) => matching nodes
func NotIn(label, property string, values []interface{}) []*NodeType {
	nodes, err := Storage.AllNodes()
	if err != nil {
		return []*NodeType{}
	}

	valueSet := make(map[interface{}]bool)
	for _, v := range values {
		valueSet[v] = true
	}

	results := make([]*NodeType, 0)
	for _, node := range nodes {
		if !hasLabel(node, label) {
			continue
		}
		if propVal, ok := node.Properties[property]; ok {
			if !valueSet[propVal] {
				results = append(results, node)
			}
		}
	}
	return results
}

// Exists searches for nodes with property.
//
// Example:
//
//	apoc.search.exists('Person', 'email') => nodes with email property
func Exists(label, property string) []*NodeType {
	nodes, err := Storage.AllNodes()
	if err != nil {
		return []*NodeType{}
	}

	results := make([]*NodeType, 0)
	for _, node := range nodes {
		if !hasLabel(node, label) {
			continue
		}
		if _, ok := node.Properties[property]; ok {
			results = append(results, node)
		}
	}
	return results
}

// Missing searches for nodes without property.
//
// Example:
//
//	apoc.search.missing('Person', 'email') => nodes without email
func Missing(label, property string) []*NodeType {
	nodes, err := Storage.AllNodes()
	if err != nil {
		return []*NodeType{}
	}

	results := make([]*NodeType, 0)
	for _, node := range nodes {
		if !hasLabel(node, label) {
			continue
		}
		if _, ok := node.Properties[property]; !ok {
			results = append(results, node)
		}
	}
	return results
}

// Null searches for nodes with null property.
//
// Example:
//
//	apoc.search.null('Person', 'middleName') => nodes with null middleName
func Null(label, property string) []*NodeType {
	nodes, err := Storage.AllNodes()
	if err != nil {
		return []*NodeType{}
	}

	results := make([]*NodeType, 0)
	for _, node := range nodes {
		if !hasLabel(node, label) {
			continue
		}
		if propVal, ok := node.Properties[property]; ok && propVal == nil {
			results = append(results, node)
		}
	}
	return results
}

// NotNull searches for nodes with non-null property.
//
// Example:
//
//	apoc.search.notNull('Person', 'email') => nodes with non-null email
func NotNull(label, property string) []*NodeType {
	nodes, err := Storage.AllNodes()
	if err != nil {
		return []*NodeType{}
	}

	results := make([]*NodeType, 0)
	for _, node := range nodes {
		if !hasLabel(node, label) {
			continue
		}
		if propVal, ok := node.Properties[property]; ok && propVal != nil {
			results = append(results, node)
		}
	}
	return results
}

// hasLabel checks if node has a specific label
func hasLabel(node *NodeType, label string) bool {
	for _, l := range node.Labels {
		if l == label {
			return true
		}
	}
	return false
}

// Match matches pattern against property.
//
// Example:
//
//	apoc.search.match('Person', 'name', 'A*') => names matching pattern
func Match(label, property, pattern string) []*NodeType {
	// Convert wildcard pattern to regex
	regexPattern := strings.ReplaceAll(pattern, "*", ".*")
	regexPattern = strings.ReplaceAll(regexPattern, "?", ".")
	regexPattern = "^" + regexPattern + "$"

	return Regex(label, property, regexPattern)
}

// Score calculates relevance scores.
//
// Example:
//
//	apoc.search.score(nodes, 'name', 'Alice') => nodes with scores
func Score(nodes []*NodeType, property, query string) []map[string]interface{} {
	results := make([]map[string]interface{}, 0)

	queryLower := strings.ToLower(query)

	for _, node := range nodes {
		if val, ok := node.Properties[property]; ok {
			if strVal, ok := val.(string); ok {
				score := calculateScore(strings.ToLower(strVal), queryLower)
				results = append(results, map[string]interface{}{
					"node":  node,
					"score": score,
				})
			}
		}
	}

	return results
}

// calculateScore calculates simple relevance score.
func calculateScore(text, query string) float64 {
	if text == query {
		return 1.0
	}

	if strings.Contains(text, query) {
		return 0.8
	}

	if strings.HasPrefix(text, query) {
		return 0.6
	}

	// Levenshtein distance-based score
	distance := levenshteinDistance(text, query)
	maxLen := len(text)
	if len(query) > maxLen {
		maxLen = len(query)
	}

	if maxLen == 0 {
		return 0
	}

	return 1.0 - (float64(distance) / float64(maxLen))
}

// levenshteinDistance calculates edit distance.
func levenshteinDistance(s1, s2 string) int {
	if len(s1) == 0 {
		return len(s2)
	}
	if len(s2) == 0 {
		return len(s1)
	}

	matrix := make([][]int, len(s1)+1)
	for i := range matrix {
		matrix[i] = make([]int, len(s2)+1)
		matrix[i][0] = i
	}

	for j := 0; j <= len(s2); j++ {
		matrix[0][j] = j
	}

	for i := 1; i <= len(s1); i++ {
		for j := 1; j <= len(s2); j++ {
			cost := 0
			if s1[i-1] != s2[j-1] {
				cost = 1
			}

			matrix[i][j] = min(
				matrix[i-1][j]+1,
				min(matrix[i][j-1]+1,
					matrix[i-1][j-1]+cost),
			)
		}
	}

	return matrix[len(s1)][len(s2)]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Highlight highlights matching terms.
//
// Example:
//
//	apoc.search.highlight('Hello Alice', 'Alice', '<b>', '</b>') => 'Hello <b>Alice</b>'
func Highlight(text, query, prefix, suffix string) string {
	re := regexp.MustCompile("(?i)" + regexp.QuoteMeta(query))
	return re.ReplaceAllString(text, prefix+"$0"+suffix)
}

// Suggest provides search suggestions.
//
// Example:
//
//	apoc.search.suggest('Person', 'name', 'Ali', 5) => ['Alice', 'Alison', ...]
func Suggest(label, property, prefix string, limit int) []string {
	nodes := Prefix(label, property, prefix)

	suggestions := make([]string, 0, limit)
	for i, node := range nodes {
		if i >= limit {
			break
		}
		if propVal, ok := node.Properties[property]; ok {
			if strVal, ok := propVal.(string); ok {
				suggestions = append(suggestions, strVal)
			}
		}
	}
	return suggestions
}

// Autocomplete provides autocomplete suggestions.
//
// Example:
//
//	apoc.search.autocomplete('Person', 'name', 'Al') => suggestions
func Autocomplete(label, property, prefix string) []string {
	return Suggest(label, property, prefix, 10)
}

// DidYouMean provides spelling suggestions.
//
// Example:
//
//	apoc.search.didYouMean('Person', 'name', 'Alise') => ['Alice']
func DidYouMean(label, property, query string) []string {
	// Get fuzzy matches within distance 2
	nodes := Fuzzy(label, property, query, 2)

	suggestions := make([]string, 0)
	for _, node := range nodes {
		if propVal, ok := node.Properties[property]; ok {
			if strVal, ok := propVal.(string); ok {
				suggestions = append(suggestions, strVal)
			}
		}
	}
	return suggestions
}

// Index creates a search index.
//
// Example:
//
//	apoc.search.index.create('Person', ['name', 'email']) => index created
func Index(label string, properties []string) error {
	// Placeholder - would create search index
	return nil
}

// DropIndex drops a search index.
//
// Example:
//
//	apoc.search.index.drop('Person', ['name']) => index dropped
func DropIndex(label string, properties []string) error {
	// Placeholder - would drop search index
	return nil
}

// Reindex rebuilds search indexes.
//
// Example:
//
//	apoc.search.reindex('Person') => reindexed
func Reindex(label string) error {
	// Placeholder - would rebuild indexes
	return nil
}
