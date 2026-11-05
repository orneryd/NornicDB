// Package search provides APOC search functions.
//
// This package implements all apoc.search.* functions for full-text
// search and pattern matching in graph data.
package search

import (
	"context"
	"reflect"
	"regexp"
	"strings"

	"github.com/orneryd/nornicdb/apoc/coll"
	"github.com/orneryd/nornicdb/apoc/storage"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
)

// NodeType represents a graph node.
type NodeType = storage.Node

// Relationship represents a graph relationship.
type Relationship = storage.Relationship

// Functions binds this package's storage-dependent helpers to one storage
// backend. The backend is fixed when the instance is created, so a handler an
// already-admitted caller holds keeps the backend it was built for.
type Functions struct {
	backend storage.Storage
}

// New returns this package's helpers bound to backend. One instance serves one
// backend; a reconfiguration builds a new instance instead of rebinding this
// one.
func New(backend storage.Storage) *Functions {
	return &Functions{backend: backend}
}

// scanInterval is how many nodes a labeled scan examines between checks of its
// caller's context. The bulk read already checks its caller before, during and
// after the copy; a scan that copies a large store's matches must notice a
// stopped statement too, so it checks at the same interval. Checking at an
// interval keeps the scan tight while bounding how long a scan whose caller has
// gone keeps working.
const scanInterval = 256

// stringProperty reads one property and reports it with the string shape the
// text operations compare. It names the shape check the text operations share,
// so each of them states only the comparison that differs.
func stringProperty(node *NodeType, property string) (string, bool) {
	propertyValue, ok := node.Properties[property]
	if !ok {
		return "", false
	}
	text, ok := propertyValue.(string)
	return text, ok
}

// resultScan owns one labeled whole-graph read, traversal and publication.
// Operation methods supply only their matching rule. The range function lets
// the compiler fuse that rule into this traversal without an indirect per-node
// callback or a mutable cursor index stored between iterations.
type resultScan struct {
	ctx     context.Context
	nodes   []*NodeType
	label   string
	matches []*NodeType
}

// newResultScan admits one read and discards failed-read rows, preserving the
// helpers' empty-result contract. The returned state belongs to this call.
func (f *Functions) newResultScan(ctx context.Context, label string) (resultScan, error) {
	s := resultScan{ctx: ctx, label: label, matches: make([]*NodeType, 0)}
	if err := ctx.Err(); err != nil {
		return s, err
	}
	nodes, err := f.backend.AllNodes(ctx)
	if err != nil {
		return s, err
	}
	s.nodes = nodes
	return s, nil
}

// each yields labeled records in source order and polls the real caller once
// per block, including records rejected by the label filter.
func (s *resultScan) each() func(func(*NodeType) bool) {
	return func(yield func(*NodeType) bool) {
		done := s.ctx.Done()
		for start := 0; start < len(s.nodes); start += scanInterval {
			if done != nil {
				select {
				case <-done:
					return
				default:
				}
			}
			end := min(start+scanInterval, len(s.nodes))
			for _, node := range s.nodes[start:end] {
				if hasLabel(node, s.label) && !yield(node) {
					return
				}
			}
		}
	}
}

// add is the single writer of this scan's ordered matches.
func (s *resultScan) add(node *NodeType) { s.matches = append(s.matches, node) }

// result rejects publication after cancellation, including the final partial
// block, while retaining a non-nil empty result.
func (s *resultScan) result() ([]*NodeType, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	return s.matches, nil
}

// selectLabels preserves label order and multiplicity; a failed label read
// returns its original error without publishing earlier labels' matches.
func selectLabels(ctx context.Context, labels []string, selectOne func(string) ([]*NodeType, error)) ([]*NodeType, error) {
	results := make([]*NodeType, 0)
	for _, label := range labels {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		selected, err := selectOne(label)
		if err != nil {
			return nil, err
		}
		results = append(results, selected...)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// equalValues is the one equality owner of this package: every apoc.search helper
// compares a property value with an expected value here, whether it names a
// single value (Node), a criteria map (NodeAll, NodeAny) or a list of values (In,
// NotIn).
//
// A scalar property value compares with Go's own ==, so equality stays strict by
// dynamic type - int64(7), int(7) and float64(7) are three different values, and
// NaN equals nothing, itself included. Because a scalar property value is a
// comparable type, that == never compares two uncomparable dynamic types: an
// expected list or map of another type compares unequal, exactly as a second
// scalar check would make it. A list or a map property value compares by
// contents, which is exactly the comparison coll.Contains makes for those values.
// Nothing here hashes a property value, so a graph that stores list or map
// properties can be searched instead of aborting the statement with an
// unhashable key or an uncomparable-type panic.
func equalValues(propertyValue, expected interface{}) bool {
	if valueconvert.NativeScalar(propertyValue) {
		return propertyValue == expected
	}
	return reflect.DeepEqual(propertyValue, expected)
}

// memberIndexThreshold is the longest list of values a membership scan compares
// one by one. Comparing a handful of values with the equality owner is cheaper
// than building and probing a table, and it needs no table at all; a longer list
// pays for one table per call and answers each node in constant time instead. The
// threshold is where the two meet on the labeled-scan benchmark, which measures
// lists of two, four, eight and sixteen values both ways.
const memberIndexThreshold = 3

// listedValue reports whether a property value is one of the listed values. It is
// membership in the equality owner's terms: one by one when the request carries
// no table, and through the table of listed scalars when it does. A property
// value of a shape the table cannot hold - a list or a map - is compared by
// coll.Contains, which is the same contents comparison equalValues makes for it,
// so nothing value-shaped is ever a hash key.
func listedValue(index map[interface{}]struct{}, values []interface{}, propertyValue interface{}) bool {
	if !valueconvert.NativeScalar(propertyValue) {
		return coll.Contains(values, propertyValue)
	}
	if index != nil {
		_, found := index[propertyValue]
		return found
	}
	for _, listed := range values {
		if equalValues(propertyValue, listed) {
			return true
		}
	}
	return false
}

// membershipIndex is the table a membership request carries, and the whole of
// that decision: a list short enough to compare one by one carries none, so the
// common one-to-three-value search allocates nothing for its listed values.
func membershipIndex(values []interface{}) map[interface{}]struct{} {
	if len(values) <= memberIndexThreshold {
		return nil
	}
	return listedScalarIndex(values)
}

// listedScalarIndex is the index a membership request makes of the listed values
// whose comparison is Go's own == (see convert.NativeScalar): nil and the string, boolean
// and number kinds. It answers those comparisons in constant time without
// changing what they mean.
func listedScalarIndex(values []interface{}) map[interface{}]struct{} {
	var index map[interface{}]struct{}
	for _, value := range values {
		if !valueconvert.NativeScalar(value) {
			continue
		}
		if index == nil {
			index = make(map[interface{}]struct{})
		}
		index[value] = struct{}{}
	}
	return index
}

// Node searches nodes by property value.
//
// Example:
//
//	apoc.search.node('Person', 'name', 'Alice') => matching nodes
func (f *Functions) Node(ctx context.Context, label, property string, value interface{}) ([]*NodeType, error) {

	scan, scanError := f.newResultScan(ctx, label)
	if scanError != nil {
		return nil, scanError
	}
	matchProperty := property
	matchValue := value
	for node := range scan.each() {
		if propertyValue, present := node.Properties[matchProperty]; present && equalValues(propertyValue, matchValue) {
			scan.add(node)
		}
	}

	return scan.result()
}

// NodeAll searches nodes matching all criteria.
//
// Example:
//
//	apoc.search.nodeAll('Person', {name: 'Alice', age: 30}) => matching nodes
func (f *Functions) NodeAll(ctx context.Context, label string, criteria map[string]interface{}) ([]*NodeType, error) {

	scan, scanError := f.newResultScan(ctx, label)
	if scanError != nil {
		return nil, scanError
	}
	matchCriteria := criteria
	for node := range scan.each() {
		matched := true
		for key, value := range matchCriteria {
			propertyValue, present := node.Properties[key]
			if !present || !equalValues(propertyValue, value) {
				matched = false
				break
			}
		}
		if matched {
			scan.add(node)
		}
	}

	return scan.result()
}

// NodeAny searches nodes matching any criteria.
//
// Example:
//
//	apoc.search.nodeAny('Person', {name: 'Alice', status: 'active'}) => matching nodes
func (f *Functions) NodeAny(ctx context.Context, label string, criteria map[string]interface{}) ([]*NodeType, error) {

	scan, scanError := f.newResultScan(ctx, label)
	if scanError != nil {
		return nil, scanError
	}
	matchCriteria := criteria
	for node := range scan.each() {
		for key, value := range matchCriteria {
			if propertyValue, present := node.Properties[key]; present && equalValues(propertyValue, value) {
				scan.add(node)
				break
			}
		}
	}

	return scan.result()
}

// NodeReduced searches with reduced results. It is the node search of one label
// with all-criteria matching, truncated to limit matches, and it takes the
// caller's context like every other whole-graph helper in this package.
//
// Example:
//
//	apoc.search.nodeReduced('Person', {status: 'active'}, 10) => top 10 matches
func (f *Functions) NodeReduced(ctx context.Context, label string, criteria map[string]interface{}, limit int) ([]*NodeType, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	helperValue1, helperError1 := f.NodeAll(ctx, label, criteria)
	if helperError1 != nil {
		return nil, helperError1
	}
	nodes := helperValue1
	if len(nodes) > limit {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return nodes[:limit], nil
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return nodes, nil
}

// MultiSearchAll searches multiple labels with all-criteria matching. Every
// label is searched in the order given and each label's matches are appended, so
// a node carrying two of the labels appears once per label.
//
// Example:
//
//	apoc.search.multiSearchAll(['Person', 'Company'], {name: 'Alice'}) => matches
func (f *Functions) MultiSearchAll(ctx context.Context, labels []string, criteria map[string]interface{}) ([]*NodeType, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	return selectLabels(ctx, labels, func(label string) ([]*NodeType, error) { return f.NodeAll(ctx, label, criteria) })
}

// MultiSearchAny searches multiple labels with any-criteria matching: a node
// matches when it carries one of the labels and satisfies at least one of the
// criteria. Every label is searched in the order given and each label's matches
// are appended.
//
// Example:
//
//	apoc.search.multiSearchAny(['Person', 'Company'], {name: 'Alice', status: 'active'}) => matches
func (f *Functions) MultiSearchAny(ctx context.Context, labels []string, criteria map[string]interface{}) ([]*NodeType, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	return selectLabels(ctx, labels, func(label string) ([]*NodeType, error) { return f.NodeAny(ctx, label, criteria) })
}

// Parallel searches several labels for one property value, scanning each label
// in the order given and appending each label's matches.
//
// Example:
//
//	apoc.search.parallel(['Person', 'Company'], 'name', 'Alice') => matches
func (f *Functions) Parallel(ctx context.Context, labels []string, property string, value interface{}) ([]*NodeType, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	return selectLabels(ctx, labels, func(label string) ([]*NodeType, error) { return f.Node(ctx, label, property, value) })
}

// FullText performs full-text search.
//
// Example:
//
//	apoc.search.fullText('Person', 'name', 'Alice Bob') => matching nodes
func (f *Functions) FullText(ctx context.Context, label, property, query string) ([]*NodeType, error) {

	scan, scanError := f.newResultScan(ctx, label)
	if scanError != nil {
		return nil, scanError
	}
	matchProperty := property
	matchWords := strings.Fields(strings.ToLower(query))
	for node := range scan.each() {
		text, present := stringProperty(node, matchProperty)
		if !present {
			continue
		}
		textLower := strings.ToLower(text)
		for _, word := range matchWords {
			if strings.Contains(textLower, word) {
				scan.add(node)
				break
			}
		}
	}

	return scan.result()
}

// Fuzzy performs fuzzy search.
//
// Example:
//
//	apoc.search.fuzzy('Person', 'name', 'Alise', 2) => matches within edit distance 2
func (f *Functions) Fuzzy(ctx context.Context, label, property, value string, maxDistance int) ([]*NodeType, error) {

	scan, scanError := f.newResultScan(ctx, label)
	if scanError != nil {
		return nil, scanError
	}
	matchMaxDistance := maxDistance
	matchNeedle := strings.ToLower(value)
	matchProperty := property
	for node := range scan.each() {
		if text, present := stringProperty(node, matchProperty); present && levenshteinDistance(strings.ToLower(text), matchNeedle) <= matchMaxDistance {
			scan.add(node)
		}
	}

	return scan.result()
}

// Regex searches using regular expressions. An invalid pattern selects nothing
// and reads nothing.
//
// Example:
//
//	apoc.search.regex('Person', 'email', '.*@example\\.com') => matching nodes
func (f *Functions) Regex(ctx context.Context, label, property, pattern string) ([]*NodeType, error) {

	compiled, err := regexp.Compile(pattern)
	if err != nil {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return []*NodeType{}, nil
	}

	scan, scanError := f.newResultScan(ctx, label)
	if scanError != nil {
		return nil, scanError
	}
	matchPattern := compiled
	matchProperty := property
	for node := range scan.each() {
		if text, present := stringProperty(node, matchProperty); present && matchPattern.MatchString(text) {
			scan.add(node)
		}
	}

	return scan.result()
}

// Prefix searches by prefix.
//
// Example:
//
//	apoc.search.prefix('Person', 'name', 'Ali') => names starting with 'Ali'
func (f *Functions) Prefix(ctx context.Context, label, property, prefix string) ([]*NodeType, error) {

	scan, scanError := f.newResultScan(ctx, label)
	if scanError != nil {
		return nil, scanError
	}
	matchNeedle := strings.ToLower(prefix)
	matchProperty := property
	for node := range scan.each() {
		if text, present := stringProperty(node, matchProperty); present && strings.HasPrefix(strings.ToLower(text), matchNeedle) {
			scan.add(node)
		}
	}

	return scan.result()
}

// Suffix searches by suffix.
//
// Example:
//
//	apoc.search.suffix('Person', 'email', '@example.com') => emails ending with suffix
func (f *Functions) Suffix(ctx context.Context, label, property, suffix string) ([]*NodeType, error) {

	scan, scanError := f.newResultScan(ctx, label)
	if scanError != nil {
		return nil, scanError
	}
	matchNeedle := strings.ToLower(suffix)
	matchProperty := property
	for node := range scan.each() {
		if text, present := stringProperty(node, matchProperty); present && strings.HasSuffix(strings.ToLower(text), matchNeedle) {
			scan.add(node)
		}
	}

	return scan.result()
}

// Contains searches for substring.
//
// Example:
//
//	apoc.search.contains('Person', 'name', 'lic') => names containing 'lic'
func (f *Functions) Contains(ctx context.Context, label, property, substring string) ([]*NodeType, error) {

	scan, scanError := f.newResultScan(ctx, label)
	if scanError != nil {
		return nil, scanError
	}
	matchNeedle := strings.ToLower(substring)
	matchProperty := property
	for node := range scan.each() {
		if text, present := stringProperty(node, matchProperty); present && strings.Contains(strings.ToLower(text), matchNeedle) {
			scan.add(node)
		}
	}

	return scan.result()
}

// Range searches within a range.
//
// Example:
//
//	apoc.search.range('Person', 'age', 18, 65) => nodes with age 18-65
func (f *Functions) Range(ctx context.Context, label, property string, minVal, maxVal interface{}) ([]*NodeType, error) {

	scan, scanError := f.newResultScan(ctx, label)
	if scanError != nil {
		return nil, scanError
	}
	matchMaximum := maxVal
	matchMinimum := minVal
	matchProperty := property
	for node := range scan.each() {
		if propertyValue, present := node.Properties[matchProperty]; present && inRange(propertyValue, matchMinimum, matchMaximum) {
			scan.add(node)
		}
	}

	return scan.result()
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
func (f *Functions) In(ctx context.Context, label, property string, values []interface{}) ([]*NodeType, error) {

	scan, scanError := f.newResultScan(ctx, label)
	if scanError != nil {
		return nil, scanError
	}
	matchIndex := membershipIndex(values)
	matchProperty := property
	matchValues := values
	for node := range scan.each() {
		if propertyValue, present := node.Properties[matchProperty]; present && listedValue(matchIndex, matchValues, propertyValue) {
			scan.add(node)
		}
	}

	return scan.result()
}

// NotIn searches for values not in a list.
//
// As before, only a node that has the property can match: a node without it is
// not "not in" the list.
//
// Example:
//
//	apoc.search.notIn('Person', 'status', ['deleted', 'banned']) => matching nodes
func (f *Functions) NotIn(ctx context.Context, label, property string, values []interface{}) ([]*NodeType, error) {

	scan, scanError := f.newResultScan(ctx, label)
	if scanError != nil {
		return nil, scanError
	}
	matchIndex := membershipIndex(values)
	matchProperty := property
	matchValues := values
	for node := range scan.each() {
		if propertyValue, present := node.Properties[matchProperty]; present && !listedValue(matchIndex, matchValues, propertyValue) {
			scan.add(node)
		}
	}

	return scan.result()
}

// Exists searches for nodes with property.
//
// Example:
//
//	apoc.search.exists('Person', 'email') => nodes with email property
func (f *Functions) Exists(ctx context.Context, label, property string) ([]*NodeType, error) {

	scan, scanError := f.newResultScan(ctx, label)
	if scanError != nil {
		return nil, scanError
	}
	matchProperty := property
	for node := range scan.each() {
		if _, present := node.Properties[matchProperty]; present {
			scan.add(node)
		}
	}

	return scan.result()
}

// Missing searches for nodes without property.
//
// Example:
//
//	apoc.search.missing('Person', 'email') => nodes without email
func (f *Functions) Missing(ctx context.Context, label, property string) ([]*NodeType, error) {

	scan, scanError := f.newResultScan(ctx, label)
	if scanError != nil {
		return nil, scanError
	}
	matchProperty := property
	for node := range scan.each() {
		if _, present := node.Properties[matchProperty]; !present {
			scan.add(node)
		}
	}

	return scan.result()
}

// Null searches for nodes with null property.
//
// Example:
//
//	apoc.search.null('Person', 'middleName') => nodes with null middleName
func (f *Functions) Null(ctx context.Context, label, property string) ([]*NodeType, error) {

	scan, scanError := f.newResultScan(ctx, label)
	if scanError != nil {
		return nil, scanError
	}
	matchProperty := property
	for node := range scan.each() {
		if propertyValue, present := node.Properties[matchProperty]; present && propertyValue == nil {
			scan.add(node)
		}
	}

	return scan.result()
}

// NotNull searches for nodes with non-null property.
//
// Example:
//
//	apoc.search.notNull('Person', 'email') => nodes with non-null email
func (f *Functions) NotNull(ctx context.Context, label, property string) ([]*NodeType, error) {

	scan, scanError := f.newResultScan(ctx, label)
	if scanError != nil {
		return nil, scanError
	}
	matchProperty := property
	for node := range scan.each() {
		if propertyValue, present := node.Properties[matchProperty]; present && propertyValue != nil {
			scan.add(node)
		}
	}

	return scan.result()
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
func (f *Functions) Match(ctx context.Context, label, property, pattern string) ([]*NodeType, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	// Convert wildcard pattern to regex
	regexPattern := strings.ReplaceAll(pattern, "*", ".*")
	regexPattern = strings.ReplaceAll(regexPattern, "?", ".")
	regexPattern = "^" + regexPattern + "$"

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return f.Regex(ctx, label, property, regexPattern)
}

// Score calculates relevance scores.
//
// Example:
//
//	apoc.search.score(nodes, 'name', 'Alice') => nodes with scores
func (f *Functions) Score(ctx context.Context, nodes []*NodeType, property, query string) []map[string]interface{} {
	if err := ctx.Err(); err != nil {
		return []map[string]interface{}{}
	}
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

	if err := ctx.Err(); err != nil {
		return []map[string]interface{}{}
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

// suggestionLimit is the number of suggestions a call that names no limit
// returns. It is the default the autocomplete name has always used.
const suggestionLimit = 10

// Suggest provides search suggestions, and is the one owner of both suggestion
// names: apoc.search.suggest passes the limit it was given, apoc.search.autocomplete
// passes none and receives suggestionLimit. A limit of zero selects nothing and a
// negative limit is unbounded, so the two names need no forwarding wrapper.
//
// Example:
//
//	apoc.search.suggest('Person', 'name', 'Ali', 5) => ['Alice', 'Alison', ...]
//	apoc.search.autocomplete('Person', 'name', 'Ali') => up to 10 suggestions
func (f *Functions) Suggest(ctx context.Context, label, property, prefix string, limit ...int) ([]string, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	maximum := suggestionLimit
	if len(limit) > 0 {
		maximum = limit[0]
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	helperValue2, helperError2 := f.Prefix(ctx, label, property, prefix)
	if helperError2 != nil {
		return nil, helperError2
	}
	return collectStrings(ctx, helperValue2, property, maximum), nil
}

// collectStrings is the one owner of the helpers that publish property strings
// rather than nodes: it keeps the scan's order, stops after maximum strings
// (unbounded when maximum is negative), and observes the caller once more before
// it publishes, so a caller that stops while its suggestions are collected
// receives the documented empty result instead of a partial list.
func collectStrings(ctx context.Context, nodes []*NodeType, property string, maximum int) []string {
	var results []string
	if maximum > 0 {
		results = make([]string, 0, maximum)
	} else {
		results = make([]string, 0)
	}
	for index, node := range nodes {
		if maximum >= 0 && index >= maximum {
			break
		}
		if text, ok := stringProperty(node, property); ok {
			results = append(results, text)
		}
	}
	if err := ctx.Err(); err != nil {
		return []string{}
	}
	return results
}

// DidYouMean provides spelling suggestions.
//
// Example:
//
//	apoc.search.didYouMean('Person', 'name', 'Alise') => ['Alice']
func (f *Functions) DidYouMean(ctx context.Context, label, property, query string) ([]string, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	// Get fuzzy matches within distance 2

	helperValue3, helperError3 := f.Fuzzy(ctx, label, property, query, 2)
	if helperError3 != nil {
		return nil, helperError3
	}
	return collectStrings(ctx, helperValue3, property, -1), nil
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
