package cypher

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// queryBodyGrammar owns the source input for the two grammar-coverage views.
func queryBodyGrammar(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("antlr/CypherParser.g4")
	require.NoError(t, err)
	return string(data)
}

func parseGrammarQueryRules(source string) map[string]string {
	grammar := regexp.MustCompile(`(?s)/\*.*?\*/`).ReplaceAllStringFunc(source, func(comment string) string {
		return strings.Repeat("\n", strings.Count(comment, "\n"))
	})
	grammar = regexp.MustCompile(`//[^\n]*`).ReplaceAllString(grammar, "")
	rules := map[string]string{}
	for _, match := range regexp.MustCompile(`(?m)^\s*([a-z]\w*)\s*:([^;]*);`).FindAllStringSubmatch(grammar, -1) {
		rules[match[1]] = match[2]
	}
	return rules
}

// grammarQueryBodyContainers derives, from CypherParser.g4, the keywords
// that open a braced query body: every rule alternative whose LBRACE …
// RBRACE holds a subqueryBody or a regularQuery, named by the last keyword
// token before its LBRACE (CALL for OPTIONAL? CALL (LPAREN …)? LBRACE).
func grammarQueryBodyContainers(grammar string) []string {
	keyword := regexp.MustCompile(`^[A-Z][A-Z_]*$`)
	punctuation := map[string]bool{"LPAREN": true, "RPAREN": true, "MULT": true, "COMMA": true, "LBRACE": true, "RBRACE": true}
	found := map[string]bool{}
	for _, rule := range strings.Split(grammar, ";") {
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
		require.NoError(t, forEachQueryBody(t.Context(), query, containers, func(container string, _, _ int) error {
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
	t.Run("production layout", func(t *testing.T) {
		for _, source := range []string{
			"newGuardBody : RETURN LBRACE subqueryBody RBRACE;",
			"newGuardBody\n    : RETURN LBRACE subqueryBody RBRACE;",
			"    newGuardBody : RETURN LBRACE subqueryBody RBRACE;",
			"\tnewGuardBody\n    : RETURN LBRACE subqueryBody RBRACE;",
		} {
			rules := parseGrammarQueryRules(source)
			require.Contains(t, rules, "newGuardBody", "a valid production cannot disappear from grammar coverage")
			require.Equal(t, []string{"RETURN"}, grammarQueryBodyContainers(source))
		}
	})
	t.Run("container keywords", func(t *testing.T) {
		kinds := grammarQueryBodyContainers(queryBodyGrammar(t))
		table := append([]string(nil), queryBodyContainerKeywords[:]...)
		sort.Strings(table)
		require.Equal(t, kinds, table, "a container kind of CypherParser.g4 is missing from queryBodyContainerKeywords (or the reverse)")
		for _, kind := range kinds {
			require.NotEmpty(t, queryBodyContainerSamples[kind], "no sample hosts a %s body", kind)
		}
	})
	// Each body-bearing rule has a witness or a reasoned exemption; ending
	// CALL bodies must preserve their public semantic rejection.
	t.Run("body-bearing rules and witnesses", func(t *testing.T) {
		rules := grammarBodyBearingRules(t)
		require.Len(t, rules, len(queryBodyManifest), "the manifest and the grammar's body-bearing rules differ")
		for _, rule := range rules {
			witness, listed := queryBodyManifest[rule]
			require.True(t, listed, "grammar rule %s carries a query body but has no witness or exemption", rule)
			if strings.HasPrefix(witness, "exempt: ") {
				continue
			}
			template, known := queryBodyWitnesses[witness]
			require.True(t, known, "rule %s names unknown witness %q", rule, witness)
			bad := strings.ReplaceAll(template, "@X@", "CALL { RETURN 1 AS b }")
			good := strings.ReplaceAll(template, "@X@", "CALL { RETURN 1 AS b } RETURN 1 AS c")
			err := validateQueryBodyComposition(t.Context(), bad)
			require.Error(t, err, "%s (%s): %s", rule, witness, bad)
			require.Contains(t, err.Error(), "Query cannot conclude with CALL", bad)
			require.NoError(t, validateQueryBodyComposition(t.Context(), good), "%s (%s): %s", rule, witness, good)
		}
	})
}

// The traversal visits a body of every container kind wherever it stands,
// and removing any one kind from what it reads (a mutation) is caught: that
// kind, and only it, is reported missing.
func TestQueryBodyTraversalVisitsEveryContainer(t *testing.T) {
	kinds := grammarQueryBodyContainers(queryBodyGrammar(t))
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
