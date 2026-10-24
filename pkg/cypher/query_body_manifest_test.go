package cypher

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// grammarBodyBearingRules are the CypherParser.g4 rules that transitively
// contain a query body: starting from query, regularQuery, singleQuery,
// singlePartQ, multiPartQ and subqueryBody, a rule is added while one of its
// alternatives names a rule already added.
func grammarBodyBearingRules(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile("antlr/CypherParser.g4")
	require.NoError(t, err)
	grammar := regexp.MustCompile(`(?s)/\*.*?\*/`).ReplaceAllStringFunc(string(data), func(comment string) string {
		return strings.Repeat("\n", strings.Count(comment, "\n"))
	})
	grammar = regexp.MustCompile(`//[^\n]*`).ReplaceAllString(grammar, "")
	rules := map[string]string{}
	for _, match := range regexp.MustCompile(`(?m)^([a-z]\w*)\s*\n\s*:([^;]*);`).FindAllStringSubmatch(grammar, -1) {
		rules[match[1]] = match[2]
	}
	identifier := regexp.MustCompile(`\b([a-z]\w*)\b`)
	bearing := map[string]bool{"query": true, "regularQuery": true, "singleQuery": true, "singlePartQ": true, "multiPartQ": true, "subqueryBody": true}
	for changed := true; changed; {
		changed = false
		for rule, alternatives := range rules {
			if bearing[rule] {
				continue
			}
			for _, reference := range identifier.FindAllString(alternatives, -1) {
				if _, isRule := rules[reference]; isRule && bearing[reference] {
					bearing[rule], changed = true, true
					break
				}
			}
		}
	}
	var names []string
	for rule := range bearing {
		names = append(names, rule)
	}
	sort.Strings(names)
	return names
}

// queryBodyWitnesses place a CALL body @X@ in each carrier position the
// grammar has, as the default parser reads it: lexically, by its braces
// (forEachQueryBody); there is no default-parser AST to walk.
var queryBodyWitnesses = map[string]string{
	"batch-size":            "CALL { CREATE () } IN TRANSACTIONS OF COUNT { CALL { @X@ } } ROWS",
	"statement":             "CALL { @X@ } RETURN 1 AS a",
	"union":                 "RETURN 1 AS a UNION CALL { @X@ } RETURN 1 AS a",
	"use":                   "USE neo4j CALL { @X@ } RETURN 1 AS a",
	"use-body":              "CALL { USE neo4j CALL { @X@ } RETURN 1 AS z } RETURN 1 AS a",
	"finish":                "CALL { CALL { @X@ } FINISH } RETURN 1 AS a",
	"show":                  "SHOW TRANSACTIONS YIELD transactionId CALL { @X@ } RETURN transactionId",
	"terminate":             "TERMINATE TRANSACTIONS 'neo4j-transaction-1' YIELD transactionId CALL { @X@ } RETURN transactionId",
	"exists":                "RETURN EXISTS { CALL { @X@ } } AS a",
	"exists-query":          "MATCH (n) WHERE EXISTS { CALL { @X@ } RETURN 1 AS z } RETURN n",
	"count":                 "RETURN COUNT { CALL { @X@ } } AS a",
	"collect":               "RETURN COLLECT { CALL { @X@ } RETURN 1 AS z } AS a",
	"return":                "RETURN COUNT { CALL { @X@ } } AS a",
	"with":                  "WITH COUNT { CALL { @X@ } } AS c RETURN c",
	"skip":                  "RETURN 1 AS a SKIP COUNT { CALL { @X@ } }",
	"limit":                 "RETURN 1 AS a LIMIT COUNT { CALL { @X@ } }",
	"order":                 "RETURN 1 AS a ORDER BY COUNT { CALL { @X@ } }",
	"where":                 "MATCH (n) WHERE EXISTS { CALL { @X@ } } RETURN n",
	"unwind":                "UNWIND [COUNT { CALL { @X@ } }] AS u RETURN u",
	"let":                   "LET v = COUNT { CALL { @X@ } } RETURN v",
	"filter":                "UNWIND [1] AS u FILTER COUNT { CALL { @X@ } } > 0 RETURN u",
	"for":                   "FOR u IN [COUNT { CALL { @X@ } }] RETURN u",
	"create":                "CREATE (n {p: COUNT { CALL { @X@ } }})",
	"delete":                "MATCH (n) DELETE CASE WHEN COUNT { CALL { @X@ } } > 0 THEN n END",
	"remove":                "MATCH (n) REMOVE n[toString(COUNT { CALL { @X@ } })]",
	"foreach":               "FOREACH (x IN [COUNT { CALL { @X@ } }] | CREATE ())",
	"procedure":             "CALL db.labels() YIELD label WHERE COUNT { CALL { @X@ } } > 0 RETURN label",
	"procedure-argument":    "CALL db.index.fulltext.queryNodes('i', toString(COUNT { CALL { @X@ } })) YIELD node RETURN node",
	"merge":                 "MERGE (n:A {p: COUNT { CALL { @X@ } }}) ON CREATE SET n.q = COUNT { CALL { @X@ } }",
	"set":                   "MATCH (n) SET n.p = COUNT { CALL { @X@ } }",
	"dynamic-property":      "MATCH (n) SET n[toString(COUNT { CALL { @X@ } })] = 1",
	"dynamic-label":         "MATCH (n:$(toString(COUNT { CALL { @X@ } }))) RETURN n",
	"node-property":         "MATCH (n {p: COUNT { CALL { @X@ } }}) RETURN n",
	"relationship":          "MATCH ()-[r {p: COUNT { CALL { @X@ } }}]->() RETURN r",
	"quantified-path":       "MATCH ((a)-[r]->(b) WHERE COUNT { CALL { @X@ } } > 0)+ RETURN a",
	"path-function":         "MATCH p = shortestPath((a)-[*]-(b)) WHERE COUNT { CALL { @X@ } } > 0 RETURN p",
	"boolean":               "RETURN NOT (COUNT { CALL { @X@ } } > 0) AND true XOR false AS a",
	"arithmetic":            "RETURN -(COUNT { CALL { @X@ } } + 1 * 2 ^ 1) AS a",
	"label-predicate":       "MATCH (n) RETURN n:$(toString(COUNT { CALL { @X@ } })) AS a",
	"list-expression":       "RETURN [1][COUNT { CALL { @X@ } }] AS a",
	"string-expression":     "RETURN 'a' STARTS WITH toString(COUNT { CALL { @X@ } }) AS a",
	"property":              "RETURN {k: COUNT { CALL { @X@ } }}.k AS a",
	"literal":               "RETURN [{k: COUNT { CALL { @X@ } }}] AS a",
	"function":              "RETURN toString(COUNT { CALL { @X@ } }) AS a",
	"parenthesized":         "RETURN (COUNT { CALL { @X@ } }) AS a",
	"expression-chain":      "RETURN [1, COUNT { CALL { @X@ } }] AS a",
	"case":                  "RETURN CASE WHEN COUNT { CALL { @X@ } } > 0 THEN 1 END AS a",
	"reduce":                "RETURN reduce(s = 0, x IN [COUNT { CALL { @X@ } }] | s + x) AS a",
	"all-reduce":            "RETURN allReduce(s = 0, x IN [COUNT { CALL { @X@ } }] | s + x, s > 0) AS a",
	"list-comprehension":    "RETURN [x IN [1] WHERE COUNT { CALL { @X@ } } > 0 | x] AS a",
	"quantifier":            "RETURN all(x IN [1] WHERE COUNT { CALL { @X@ } } > 0) AS a",
	"pattern-comprehension": "MATCH (n) RETURN [(n)-->(m) WHERE COUNT { CALL { @X@ } } > 0 | m] AS a",
	"map-projection":        "MATCH (n) RETURN n{.p, c: COUNT { CALL { @X@ } }} AS a",
}

// queryBodyManifest names, for every body-bearing grammar rule, the witness
// that places a body in its position, or the reason none applies.
var queryBodyManifest = map[string]string{
	"syntaxFragment": "statement", "transactionBatch": "batch-size",
	"script": "statement", "query": "statement", "regularQuery": "statement", "singleQuery": "statement",
	"singlePartQ": "statement", "multiPartQ": "with", "readingStatement": "statement", "callSubquery": "statement",
	"subqueryBody": "finish", "unionSt": "union", "useClause": "use-body", "showCommand": "show", "showTail": "show",
	"terminateCommand": "terminate", "existsSubquery": "exists", "subqueryExist": "exists-query",
	"countSubquery": "count", "collectSubquery": "collect", "returnSt": "return", "projectionBody": "return",
	"projectionItems": "return", "projectionItem": "return", "withSt": "with", "skipSt": "skip", "limitSt": "limit",
	"orderItem": "order", "orderSt": "order", "matchSt": "where", "patternWhere": "where", "where": "where",
	"unwindSt": "unwind", "letSt": "let", "letItem": "let", "filterSt": "filter", "forSt": "for",
	"updatingStatement": "create", "createSt": "create", "deleteSt": "delete", "removeSt": "remove",
	"removeItem": "remove", "foreachSt": "foreach", "queryCallSt": "procedure", "yieldItems": "procedure",
	"parenExpressionChain": "procedure-argument", "standaloneCall": "procedure-argument", "mergeSt": "merge",
	"mergeAction": "merge", "setSt": "set", "setItem": "set", "dynamicPropertyExpression": "dynamic-property",
	"nodeLabels": "dynamic-label", "labelExpression": "dynamic-label", "labelConjunction": "dynamic-label",
	"labelNegation": "dynamic-label", "dynamicLabel": "dynamic-label", "pattern": "node-property",
	"patternPart": "node-property", "patternElem": "node-property", "patternElemStart": "node-property",
	"patternElemPart": "relationship", "patternElemChain": "relationship", "nodePattern": "node-property",
	"properties": "node-property", "relationshipPattern": "relationship", "relationDetail": "relationship",
	"relationshipTypes": "relationship", "quantifiedPath": "quantified-path", "pathFunction": "path-function",
	"expression": "return", "xorExpression": "boolean", "andExpression": "boolean", "notExpression": "boolean",
	"comparisonExpression": "boolean", "addSubExpression": "arithmetic", "multDivExpression": "arithmetic",
	"powerExpression": "arithmetic", "unaryAddSubExpression": "arithmetic", "atomicExpression": "parenthesized",
	"labelPredicate": "label-predicate", "listExpression": "list-expression", "stringExpression": "string-expression",
	"propertyOrLabelExpression": "property", "propertyExpression": "property", "atom": "parenthesized",
	"literal": "literal", "listLit": "literal", "mapLit": "literal", "mapPair": "literal",
	"functionInvocation": "function", "parenthesizedExpression": "parenthesized", "expressionChain": "expression-chain",
	"caseExpression": "case", "reduceExpression": "reduce", "allReduceExpression": "all-reduce",
	"listComprehension": "list-comprehension", "filterExpression": "quantifier", "filterWith": "list-comprehension",
	"patternComprehension": "pattern-comprehension", "relationshipsChainPattern": "pattern-comprehension",
	"mapProjection": "map-projection", "mapProjectionItem": "map-projection",
	"shellCommand":          "exempt: a shell command (:param, :use, …) is the client's, never an engine statement",
	"shellCommandElement":   "exempt: a shell command's element (see shellCommand)",
	"schemaCommand":         "exempt: CREATE/DROP INDEX and CONSTRAINT take no query body, and the semantic entry skips schema statements",
	"constraintRequirement": "exempt: a schema constraint's requirement (see schemaCommand)",
	"constraintBlock":       "exempt: a schema constraint's block (see schemaCommand)",
}

// Every body-bearing grammar rule has a witness (or a reasoned exemption),
// and each witness is read as Neo4j 5.26 does: a CALL body inside it that
// ends with a CALL returning columns is "Query cannot conclude with CALL",
// one that concludes is accepted, whatever carries it.
func TestQueryBodyManifestCoversGrammar(t *testing.T) {
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
		err := validateQueryBodyComposition(bad)
		require.Error(t, err, "%s (%s): %s", rule, witness, bad)
		require.Contains(t, err.Error(), "Query cannot conclude with CALL", bad)
		require.NoError(t, validateQueryBodyComposition(good), "%s (%s): %s", rule, witness, good)
	}
}

// Every clause kind the pipeline splits a body into says how it ends one.
func TestQueryBodyEndingsCoverEveryClauseKind(t *testing.T) {
	for kind := pipelineClauseMatch; kind <= pipelineClauseFilter; kind++ {
		_, listed := queryBodyEndings[kind]
		require.True(t, listed, "clause kind %d has no queryBodyEndings entry", kind)
	}
	require.Len(t, queryBodyEndings, int(pipelineClauseFilter)+1)
	// An update clause concludes a body.
	for kind := range updatingClauses {
		require.Empty(t, queryBodyEndings[kind], "update clause kind %d", kind)
	}
}
