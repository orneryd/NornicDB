package cypher

import (
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// vector(value, dimension, coordinateType), vector_distance(a, b, metric)
// and vector_norm(vector, metric) name a coordinate type or metric with a
// bare word (INTEGER, COSINE), which no expression evaluates. The statement
// rewrite (desugarLabelExpressions) writes each call as its internal
// function (__nornic_vector, __nornic_vector_distance,
// __nornic_vector_norm) with the canonical name quoted, so validators and
// evaluators read an ordinary call; the restore maps columns and messages
// back. As in Neo4j, a name that isn't a coordinate type or a metric the
// function takes, a string in its place, and a literal dimension outside 1
// to 4096 are SyntaxErrors.

// vectorCallMetrics are the metrics each function takes, in Neo4j's order.
var vectorCallMetrics = map[string][]string{
	"vector_distance": {"EUCLIDEAN", "EUCLIDEAN_SQUARED", "MANHATTAN", "COSINE", "DOT", "HAMMING"},
	"vector_norm":     {"EUCLIDEAN", "MANHATTAN"},
}

// vectorCall rewrites the call whose function name is query[wordStart:wordEnd]
// when it is vector, vector_distance or vector_norm.
func (r *labelExpressionRewriter) vectorCall(wordStart, wordEnd, end int) error {
	q := r.query
	word := strings.ToLower(q[wordStart:wordEnd])
	if word != "vector" && word != "vector_distance" && word != "vector_norm" {
		return nil
	}
	open := skipASCIISpaces(q, wordEnd, end)
	if open >= end || q[open] != '(' {
		return nil
	}
	close := findMatchingParen(q[:end], open)
	if close < 0 {
		return nil
	}
	args := topLevelArgumentSpans(q, open+1, close)
	nameArg := map[string]int{"vector": 2, "vector_distance": 2, "vector_norm": 1}[word]
	if len(args) != nameArg+1 {
		return nil // the function reports its argument count
	}
	nameStart, nameEnd := args[nameArg][0], args[nameArg][1]
	name := q[nameStart:nameEnd]
	canonical := ""
	if word == "vector" {
		if coordinateType, ok := parseVectorCoordinateType(name); ok && isBareName(name) {
			canonical = coordinateType.String()
		} else {
			return labelExpressionSyntaxError(localization.CypherCoreVectorInnerTypeInvalid())
		}
		dimension := strings.TrimSpace(q[args[1][0]:args[1][1]])
		if value, err := strconv.ParseInt(dimension, 10, 64); err == nil && (value < 1 || value > vectorDimensionLimit) {
			return labelExpressionSyntaxError(localization.CypherCoreVectorDimensionRange(dimension))
		}
	} else {
		metrics := vectorCallMetrics[word]
		for _, metric := range metrics {
			if isBareName(name) && strings.EqualFold(name, metric) {
				canonical = metric
			}
		}
		if canonical == "" {
			expected := strings.Join(metrics[:len(metrics)-1], ", ") + " or " + metrics[len(metrics)-1]
			return labelExpressionSyntaxError(localization.CypherCoreVectorMetricInvalid(expected))
		}
	}
	r.edit(wordStart, wordEnd, "__nornic_"+word)
	r.edit(nameStart, nameEnd, "'"+canonical+"'")
	return nil
}

// isBareName reports whether text is a plain name (an identifier, as the
// lexer reads one), not a literal or an expression.
func isBareName(text string) bool {
	if text == "" || !isIdentStartByte(text[0]) {
		return false
	}
	for i := 0; i < len(text); i++ {
		if !isIdentByte(text[i]) {
			return false
		}
	}
	return true
}

// topLevelArgumentSpans returns the [start, end) of each comma-separated
// argument in query[start:end], spaces trimmed.
func topLevelArgumentSpans(query string, start, end int) [][2]int {
	var spans [][2]int
	for from := start; from <= end; {
		comma := topLevelByteIndex(query, from, end, ',')
		stop := end
		if comma >= 0 {
			stop = comma
		}
		argStart := skipASCIISpaces(query, from, stop)
		spans = append(spans, [2]int{argStart, trimRightIndex(query, argStart, stop)})
		if comma < 0 {
			break
		}
		from = comma + 1
	}
	return spans
}

// mayUseVectorCall reports whether query may call vector, vector_distance
// or vector_norm: the word, outside a longer name, then (. It never answers
// false for one.
func mayUseVectorCall(query string) bool {
	for from := 0; ; {
		at := indexASCIIFold(query[from:], "vector")
		if at < 0 {
			return false
		}
		at += from
		from = at + len("vector")
		if at > 0 && (isIdentByte(query[at-1]) || query[at-1] == '.') {
			continue
		}
		end := from
		for _, suffix := range []string{"_distance", "_norm"} {
			if len(query) >= end+len(suffix) && equalFoldASCII(query[end:end+len(suffix)], suffix) {
				end += len(suffix)
			}
		}
		if next := skipASCIISpaces(query, end, len(query)); next < len(query) && query[next] == '(' {
			return true
		}
	}
}
