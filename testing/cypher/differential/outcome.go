package differential

import (
	"encoding/json"
	"regexp"
	"sort"
	"strings"
)

// Outcome is one statement's answer through one route: its columns and rows,
// or the status code of its error. Values are in a route's normalized form
// (NormalizeBoltValue for Bolt, the response JSON for HTTP), the same form on
// both servers.
type Outcome struct {
	Columns []string `json:"columns,omitempty"`
	Rows    [][]any  `json:"rows,omitempty"`
	// Code is the error's Neo4j status code; "" when the statement succeeded.
	// A client-side failure (a timeout, a broken connection) is
	// "client:<description>".
	Code string `json:"code,omitempty"`
	// Message is the error's text, kept for reports only: the status code is
	// the contract, the wording is each server's own.
	Message string `json:"message,omitempty"`
}

// Failed reports whether the statement failed.
func (outcome Outcome) Failed() bool { return outcome.Code != "" }

var orderBy = regexp.MustCompile(`(?i)\bORDER\s+BY\b`)

// Same reports whether two outcomes of query agree, as the sweep compares
// them (#907):
//   - two errors agree when their status codes are equal, whatever the text;
//   - an error and rows never agree;
//   - rows agree when the columns are equal and the rows are equal, in order
//     when query has ORDER BY and as a multiset otherwise;
//   - a collect() with no ORDER BY before it has no defined element order (in
//     Neo4j either), so list values of such a statement compare as multisets;
//   - a statement that keeps arbitrary rows (Arbitrary) compares its columns
//     and its number of rows only.
func Same(query string, reference, actual Outcome) bool {
	if Arbitrary(query) && !reference.Failed() && !actual.Failed() {
		return equalStrings(reference.Columns, actual.Columns) && len(reference.Rows) == len(actual.Rows)
	}
	return same(reference, actual, orderBy.MatchString(query), unorderedCollect(query))
}

var (
	rowLimit       = regexp.MustCompile(`(?i)\b(LIMIT|SKIP)\b`)
	clauseKeywords = regexp.MustCompile(`(?i)\b(WITH|RETURN)\b`)
)

// Arbitrary reports whether query keeps arbitrary rows: a LIMIT or SKIP with
// no ORDER BY since its clause's WITH or RETURN. Which rows such a clause keeps
// isn't defined (in Neo4j either), so neither are its rows nor what it writes.
func Arbitrary(query string) bool {
	for _, limit := range rowLimit.FindAllStringIndex(query, -1) {
		clauseStart := 0
		for _, keyword := range clauseKeywords.FindAllStringIndex(query[:limit[0]], -1) {
			clauseStart = keyword[0]
		}
		if !orderBy.MatchString(query[clauseStart:limit[0]]) {
			return true
		}
	}
	return false
}

// SameGraphState reports whether two answers of a GraphStateQueries statement
// agree: rows as a multiset, label lists in any order.
func SameGraphState(reference, actual Outcome) bool {
	return same(reference, actual, false, true)
}

func same(reference, actual Outcome, ordered, unorderedLists bool) bool {
	if reference.Failed() || actual.Failed() {
		return reference.Code == actual.Code
	}
	if !equalStrings(reference.Columns, actual.Columns) {
		return false
	}
	if len(reference.Rows) != len(actual.Rows) {
		return false
	}
	encode := func(rows [][]any) []string {
		encoded := make([]string, len(rows))
		for index, row := range rows {
			encoded[index] = canonical(row, unorderedLists)
		}
		return encoded
	}
	left, right := encode(reference.Rows), encode(actual.Rows)
	if !ordered {
		sort.Strings(left)
		sort.Strings(right)
	}
	return equalStrings(left, right)
}

// unorderedCollect reports whether query's first collect( has no ORDER BY
// before it.
func unorderedCollect(query string) bool {
	index := strings.Index(strings.ToLower(query), "collect(")
	return index >= 0 && !orderBy.MatchString(query[:index])
}

// canonical is value's JSON with map keys sorted; with unorderedLists, the
// elements of every list are sorted too.
func canonical(value any, unorderedLists bool) string {
	if unorderedLists {
		value = sortLists(value)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "unencodable"
	}
	return string(encoded)
}

func sortLists(value any) any {
	switch typed := value.(type) {
	case []any:
		items := make([]any, len(typed))
		for index, item := range typed {
			items[index] = sortLists(item)
		}
		sort.Slice(items, func(left, right int) bool {
			return canonical(items[left], false) < canonical(items[right], false)
		})
		return items
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			result[key] = sortLists(item)
		}
		return result
	}
	return value
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

// GraphStateQueries read a graph's nodes and relationships in a form both
// servers and every route answer alike; the outcomes compare as multisets.
var GraphStateQueries = []string{
	"MATCH (n) RETURN labels(n) AS labels, properties(n) AS properties",
	"MATCH (a)-[r]->(b) RETURN properties(a) AS start, type(r) AS type, properties(r) AS properties, properties(b) AS end",
}

var writeClause = regexp.MustCompile(`(?i)\b(CREATE|MERGE|SET|DELETE|REMOVE|FOREACH|DROP)\b`)

// Writes reports whether query may change the graph in a defined way, so the
// graph state after it is compared too: it writes, and doesn't write to
// arbitrary rows (Arbitrary).
func Writes(query string) bool { return writeClause.MatchString(query) && !Arbitrary(query) }
