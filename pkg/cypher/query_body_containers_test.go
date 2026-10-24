package cypher

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// grammarQueryBodyContainers derives, from CypherParser.g4, the keywords
// that open a braced query body: every rule alternative whose LBRACE …
// RBRACE holds a subqueryBody or a regularQuery, named by the last keyword
// token before its LBRACE (CALL for OPTIONAL? CALL (LPAREN …)? LBRACE).
func grammarQueryBodyContainers(t *testing.T) []string {
	t.Helper()
	grammar, err := os.ReadFile("antlr/CypherParser.g4")
	require.NoError(t, err)
	keyword := regexp.MustCompile(`^[A-Z][A-Z_]*$`)
	punctuation := map[string]bool{"LPAREN": true, "RPAREN": true, "MULT": true, "COMMA": true, "LBRACE": true, "RBRACE": true}
	found := map[string]bool{}
	for _, rule := range strings.Split(string(grammar), ";") {
		for offset := 0; ; {
			open := strings.Index(rule[offset:], "LBRACE")
			if open < 0 {
				break
			}
			open += offset
			closing := strings.Index(rule[open:], "RBRACE")
			if closing < 0 {
				break
			}
			inside := rule[open : open+closing]
			offset = open + closing
			if !regexp.MustCompile(`\b(subqueryBody|regularQuery)\b`).MatchString(inside) {
				continue
			}
			before := rule[:open]
			if colon := strings.LastIndex(before, ":"); colon >= 0 {
				before = before[colon+1:]
			}
			if bar := strings.LastIndex(before, "\n    |"); bar >= 0 {
				before = before[bar:]
			}
			fields := strings.Fields(before)
			for index := len(fields) - 1; index >= 0; index-- {
				token := strings.Trim(fields[index], "()?*+|")
				if keyword.MatchString(token) && !punctuation[token] {
					found[token] = true
					break
				}
			}
		}
	}
	var kinds []string
	for kind := range found {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
}

// queryBodyContainerSamples hosts a body of each container kind, in the
// positions a hand-written traversal could miss: a body that ends with
// FINISH, an expression subquery, a list comprehension and a CASE.
var queryBodyContainerSamples = map[string][]string{
	"CALL": {
		"UNWIND [] AS s CALL { CALL { RETURN 1 AS b } FINISH } RETURN 1 AS a",
		"UNWIND [] AS s CALL (s) { RETURN 1 AS b } RETURN b",
		"UNWIND [] AS s OPTIONAL CALL { RETURN 1 AS b } RETURN b",
		"UNWIND [] AS s CALL { CREATE () } IN TRANSACTIONS RETURN 1 AS a",
	},
	"EXISTS":  {"UNWIND [] AS s RETURN [x IN [1] WHERE EXISTS { MATCH (n) } | x] AS a"},
	"COUNT":   {"UNWIND [] AS s RETURN CASE WHEN COUNT { MATCH (n) } > 0 THEN 1 END AS a"},
	"COLLECT": {"UNWIND [] AS s RETURN COLLECT { MATCH (n) RETURN n } AS a"},
}

// visitedQueryBodyContainers are the container kinds forEachQueryBody visits
// in queries, reading containers.
func visitedQueryBodyContainers(t *testing.T, containers []string, queries []string) map[string]bool {
	t.Helper()
	visited := map[string]bool{}
	for _, query := range queries {
		require.NoError(t, forEachQueryBody(query, containers, func(container string, _, _ int) error {
			visited[container] = true
			return nil
		}))
	}
	return visited
}

// missingQueryBodyContainers are the grammar's container kinds the
// traversal, reading containers, doesn't visit in their samples.
func missingQueryBodyContainers(t *testing.T, kinds, containers []string) []string {
	t.Helper()
	var missing []string
	for _, kind := range kinds {
		if !visitedQueryBodyContainers(t, containers, queryBodyContainerSamples[kind])[kind] {
			missing = append(missing, kind)
		}
	}
	return missing
}

// Both parsers open a query body with the same keywords, and the one list
// the brace scanners and the traversal read holds exactly the grammar's.
func TestQueryBodyContainersMatchGrammar(t *testing.T) {
	kinds := grammarQueryBodyContainers(t)
	table := append([]string(nil), queryBodyContainerKeywords[:]...)
	sort.Strings(table)
	require.Equal(t, kinds, table, "a container kind of CypherParser.g4 is missing from queryBodyContainerKeywords (or the reverse)")
	for _, kind := range kinds {
		require.NotEmpty(t, queryBodyContainerSamples[kind], "no sample hosts a %s body", kind)
	}
}

// The traversal visits a body of every container kind wherever it stands,
// and removing any one kind from what it reads (a mutation) is caught: that
// kind, and only it, is reported missing.
func TestQueryBodyTraversalVisitsEveryContainer(t *testing.T) {
	kinds := grammarQueryBodyContainers(t)
	require.Empty(t, missingQueryBodyContainers(t, kinds, queryBodyContainerKeywords[:]))
	for _, removed := range kinds {
		t.Run("without "+removed, func(t *testing.T) {
			var mutated []string
			for _, kind := range queryBodyContainerKeywords {
				if kind != removed {
					mutated = append(mutated, kind)
				}
			}
			require.Equal(t, []string{removed}, missingQueryBodyContainers(t, kinds, mutated))
		})
	}
}
