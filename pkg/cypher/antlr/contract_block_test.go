package antlr

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"

	antlr4 "github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

// A contract block's entries are read by the grammar, one after another,
// with or without ';' and on one line or many.
func TestContractBlockEntriesAreReadByTheGrammar(t *testing.T) {
	for _, tc := range []struct{ name, statement string }{
		{"one entry", "CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { n.id IS UNIQUE }"},
		{"comparison", "CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { n.age > 0 }"},
		{"entries on one line", "CREATE CONSTRAINT c FOR (n:Person) REQUIRE { n.id IS UNIQUE n.age IS :: INTEGER n.status IN ['active'] NOT EXISTS { (n)-[:BAD]->() } }"},
		{"entries on lines", "CREATE CONSTRAINT c FOR (n:Doc) REQUIRE {\n  n.id IS UNIQUE\n  n.age IS :: INTEGER\n  n.status IN ['active', 'closed']\n  (n.a, n.b) IS NODE KEY\n}"},
		{"entries by semicolon", "CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { n.id IS UNIQUE; n.age > 0; }"},
		{"blank and comment", "CREATE CONSTRAINT c FOR (n:Doc) REQUIRE {\n\n  // the identity\n  n.id IS UNIQUE /* always */\n\n}"},
		{"entry over lines", "CREATE CONSTRAINT c FOR (n:Doc) REQUIRE {\n  n.status IN ['active',\n    'closed']\n  n.age >\n    0\n  NOT EXISTS {\n    (n)-[:BAD]->()\n  }\n}"},
		{"empty block", "CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { }"},
		{"contract in an entry", "CREATE CONSTRAINT c FOR (n:Doc) REQUIRE {\n  FOR ()-[r:KNOWS]-() REQUIRE { r.id IS UNIQUE }\n}"},
		{"block in a block", "CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { n.id IS UNIQUE { n.age > 0 n.age < 200 } }"},
		{"type before an entry", "CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { n.age IS :: INTEGER n.status IN ['active'] }"},
		{"two word type before an entry", "CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { n.at IS :: ZONED DATETIME n.id IS UNIQUE }"},
		{"type before a negation", "CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { n.age IS :: INTEGER NOT EXISTS { (n)-[:BAD]->() } }"},
		{"relationship contract", "CREATE CONSTRAINT c FOR ()-[r:WORKS_ON]-() REQUIRE {\n  r.since IS NOT NULL\n  r.role IS :: STRING\n}"},
		// A block is found by the word REQUIRE as the lexer reads it, in any
		// case, and nowhere else: not in a string, a comment or a quoted name.
		{"lower-case words", "create constraint c for (n:Doc) require { n.id is unique n.age > 0 }"},
		{"mixed-case require", "CREATE CONSTRAINT c FOR (n:Doc) rEqUiRe { n.id IS UNIQUE; n.age > 0 }"},
		{"the word in an entry's string", "CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { n.note <> 'REQUIRE { n.age > }' }"},
		{"no block", "CREATE CONSTRAINT c FOR (n:Doc) REQUIRE n.id IS UNIQUE"},
		{"the word in a string", "RETURN 'REQUIRE { n.age > }' AS s"},
		{"the word in a comment", "RETURN 1 /* REQUIRE { n.age > } */ AS s"},
		{"the word as a quoted name", "MATCH (`REQUIRE`:Doc) RETURN `REQUIRE`.id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, Validate(tc.statement))
		})
	}
	// The single-line positions are the ones these entries were reported at
	// when the grammar read a block as a list of predicates.
	for _, tc := range []struct{ statement, position string }{
		{"CREATE CONSTRAINT c2 FOR (n:Doc) REQUIRE { n.age > }", "line 1:51"},
		{"CREATE CONSTRAINT c3 FOR (n:Doc) REQUIRE { n.age + }", "line 1:51"},
		{"create constraint c2 for (n:Doc) require { n.age > }", "line 1:51"},
		{"CREATE CONSTRAINT c3 FOR (n:Doc) rEqUiRe { n.age + }", "line 1:51"},
		{"CREATE CONSTRAINT c5 FOR (n:Doc) REQUIRE { (( }", "line 1:46"},
		{"CREATE CONSTRAINT c6 FOR (n:Doc) REQUIRE { n.status IN ['a' }", "line 1:60"},
		{"CREATE CONSTRAINT c7 FOR (n:Doc) REQUIRE { n.age > 1 ) }", "line 1:53"},
		{"CREATE CONSTRAINT c13 FOR (n:Doc) REQUIRE { ] }", "line 1:44"},
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { n.id IS UNIQUE;; n.age > 0 }", "line 1:57"},
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE {\n  n.id IS UNIQUE\n  n.age >\n}", "line 4:0"},
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE {\n  n.id IS UNIQUE\n  { n.age > }\n}", "line 3:12"},
	} {
		t.Run(tc.statement, func(t *testing.T) {
			err := Validate(tc.statement)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.position)
		})
	}
	// When an entry before the malformed one read, the error is reported once,
	// where the malformed entry's reading stopped. The grammar's list of
	// predicates reported these blocks elsewhere: the first two at the block's
	// first entry (line 1:40) and not at the malformed one; the last two at
	// the entry's own contract (line 1:81) and then at the same place as here.
	for _, tc := range []struct{ statement, message string }{
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE { n.a IS :: LIST<INTEGER> n.age > }", "syntax error: line 1:72 unexpected input"},
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE { n.a IS :: LIST<INTEGER>\n/* entry */ n.age > }", "syntax error: line 2:20 unexpected input"},
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE { FOR ()-[r:T]-() REQUIRE { r.id IS UNIQUE } n.age > }", "syntax error: line 1:91 unexpected input"},
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE { FOR ()-[r:T]-() REQUIRE { r.id IS UNIQUE }\n/* entry */ n.age > }", "syntax error: line 2:20 unexpected input"},
	} {
		t.Run(tc.statement, func(t *testing.T) {
			err := Validate(tc.statement)
			require.Error(t, err)
			require.Equal(t, tc.message, err.Error())
			_, err = Parse(tc.statement)
			require.Error(t, err)
			require.Equal(t, tc.message, err.Error())
		})
	}
}

// The entries a block reads as are nodes of the statement. A reader that
// walks the statement finds each entry's names where they are written, in
// source order, and every token of the statement is a leaf of the tree once:
// an entry stands where its tokens stood. A block inside an entry is read
// the same way.
func TestContractBlockEntriesAreNodesOfTheStatement(t *testing.T) {
	for _, tc := range []struct {
		statement string
		names     map[string]int
	}{
		{"CREATE CONSTRAINT c FOR ()-[`r``x`:T]-() REQUIRE { (`r``x`.id) IS RELATIONSHIP KEY }", map[string]int{"r`x": 2}},
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE {\n  n.id IS UNIQUE; n.age > $min\n  n.status IN ['a', 'b'] /* closed */ NOT EXISTS { (n)-[:BAD]->(m) }\n}", map[string]int{"n": 5, "m": 1}},
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { n.id IS UNIQUE { n.age > 0 n.age < 200 } }", map[string]int{"n": 4}},
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE {\n  FOR ()-[r:KNOWS]-() REQUIRE { r.id IS UNIQUE }\n}", map[string]int{"n": 1, "r": 2}},
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { }", map[string]int{"n": 1}},
	} {
		t.Run(tc.statement, func(t *testing.T) {
			syntax, err := ParseSyntax(context.Background(), tc.statement, StatementEntry, false, AcceptedSource)
			require.NoError(t, err)
			names := map[string]int{}
			leaves := map[int]int{}
			written := -1
			syntax.Walk(func(node SyntaxNode) func() {
				first, after := node.TokenRange()
				if node.Rule() < 0 {
					require.Equal(t, first+1, after)
					leaves[first]++
				}
				if name, symbol := node.Name(); symbol {
					start, end := node.Span()
					require.Equal(t, name, cyphertext.SymbolicNameValue(tc.statement[start:end]), "a name's place in the statement")
					require.Greater(t, start, written, "names in source order")
					written = start
					names[name]++
				}
				return nil
			})
			for name, count := range tc.names {
				require.Equal(t, count, names[name], name)
			}
			for index := 0; index < syntax.TokenCount(); index++ {
				token := syntax.Token(index)
				if token.Channel() != 0 || token.Kind() < 0 {
					continue
				}
				require.Equal(t, 1, leaves[index], "token %d is one leaf", index)
			}
		})
	}
}

// The tree Parse returns and the retained statement syntax hold the same
// parse: the same nodes over the same tokens of the statement, in the same
// order, with a contract block's entries among them. A reader of the tree
// (QueryAnalyzer, ClauseExtractor, a listener) finds an entry's names and
// parameters as a reader of the retained syntax does.
func TestContractBlockEntriesAreInTheParseTree(t *testing.T) {
	type node struct{ rule, first, after int }
	for _, tc := range []struct {
		statement string
		symbols   map[string]int
	}{
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE { n.age > $min }", map[string]int{"age": 1, "n": 2}},
		{"create constraint c for (n:N) require { n.id is unique n.age > $min }", map[string]int{"age": 1, "id": 1, "n": 3}},
		{"CREATE CONSTRAINT c FOR (n:N) rEqUiRe { n.age > $min; n.note <> 'REQUIRE { n.age > }' }", map[string]int{"age": 1, "note": 1, "n": 3}},
		{"MATCH (`REQUIRE`:Doc) RETURN `REQUIRE`.id", map[string]int{"`REQUIRE`": 2, "id": 1}},
		{"CREATE CONSTRAINT c FOR ()-[`r``x`:T]-() REQUIRE { (`r``x`.id) IS RELATIONSHIP KEY }", map[string]int{"`r``x`": 2, "id": 1}},
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE {\n  n.id IS UNIQUE; n.age > $min\n  n.status IN ['a', 'b'] /* closed */ NOT EXISTS { (n)-[:BAD]->(m) }\n}", map[string]int{"n": 5, "m": 1, "status": 1, "BAD": 1}},
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { n.id IS UNIQUE { n.age > 0 n.age < 200 } }", map[string]int{"n": 4, "age": 2}},
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE {\n  FOR ()-[r:KNOWS]-() REQUIRE { r.id IS UNIQUE }\n}", map[string]int{"n": 1, "r": 2, "KNOWS": 1}},
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { }", map[string]int{"n": 1}},
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE n.id IS UNIQUE", map[string]int{"n": 2, "id": 1}},
		{"MATCH (n:Doc) WHERE n.age > $min RETURN n.id", map[string]int{"n": 3, "age": 1}},
	} {
		t.Run(tc.statement, func(t *testing.T) {
			parsed, err := Parse(tc.statement)
			require.NoError(t, err)
			var tree []node
			symbols := map[string]int{}
			var text []string
			var visit func(antlr4.Tree)
			visit = func(item antlr4.Tree) {
				switch item := item.(type) {
				case antlr4.ParserRuleContext:
					entry := node{rule: item.GetRuleIndex(), first: -1, after: -1}
					if first, last := item.GetStart(), item.GetStop(); first != nil && last != nil {
						entry.first, entry.after = first.GetTokenIndex(), last.GetTokenIndex()+1
					}
					tree = append(tree, entry)
					if symbol, ok := item.(*SymbolContext); ok {
						symbols[symbol.GetText()]++
					}
				case antlr4.TerminalNode:
					token := item.GetSymbol()
					tree = append(tree, node{rule: -1, first: token.GetTokenIndex(), after: token.GetTokenIndex() + 1})
					if token.GetTokenType() != antlr4.TokenEOF {
						// A token of the tree is the statement's: it reads the
						// statement's text at its place.
						require.Equal(t, tc.statement[token.GetStart():token.GetStop()+1], token.GetText())
						text = append(text, token.GetText())
					}
				}
				_, block := item.(*ConstraintBlockContext)
				for _, child := range item.GetChildren() {
					if rule, ok := child.(antlr4.RuleContext); ok && block {
						require.True(t, rule.GetParent() == item, "an entry's parent is its block")
					}
					visit(child)
				}
			}
			visit(parsed.Tree)
			for name, count := range tc.symbols {
				require.Equal(t, count, symbols[name], "%s in %v", name, symbols)
			}

			syntax, err := ParseSyntax(context.Background(), tc.statement, StatementEntry, false, RawSource)
			require.NoError(t, err)
			var retained []node
			var tokens []string
			syntax.Walk(func(item SyntaxNode) func() {
				first, after := item.TokenRange()
				retained = append(retained, node{rule: item.Rule(), first: first, after: after})
				return nil
			})
			for index := 0; index < syntax.TokenCount(); index++ {
				token := syntax.Token(index)
				if token.Channel() != 0 || token.Kind() < 0 {
					continue
				}
				start, end := token.Span()
				tokens = append(tokens, tc.statement[start:end])
			}
			require.Equal(t, retained, tree, "the tree and the retained syntax hold the same nodes")
			require.Equal(t, tokens, text, "the tree's leaves are the statement's tokens, each once and in order")

			// The package's readers of the tree walk it whole.
			require.NotNil(t, NewQueryAnalyzer().Analyze(tc.statement, parsed))
			require.NotNil(t, ExtractClauses(tc.statement, parsed))
		})
	}
}

// The package's readers of the tree Parse returns get what an entry of a
// contract block holds: QueryAnalyzer the labels and relationship types it
// names, ClauseExtractor its variables.
// A TEMPORAL requirement reads with and without NO OVERLAP, as the one
// requirement of a constraint and as an entry of a contract block. NO and
// OVERLAP are also names: after TEMPORAL they belong to the requirement only
// together, and an entry that starts with either is the next entry. These are
// the readings the grammar's list of predicates gave.
func TestTemporalRequirementReadsWithAndWithoutNoOverlap(t *testing.T) {
	for _, tc := range []struct {
		statement string
		reads     []string
	}{
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE n.a IS TEMPORAL", []string{"n.aISTEMPORAL"}},
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE n.a IS TEMPORAL NO OVERLAP", []string{"n.aISTEMPORALNOOVERLAP"}},
		{"create constraint c for (n:N) require n.a is temporal no overlap", []string{"n.aistemporalnooverlap"}},
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE (n.a, n.b) IS TEMPORAL", []string{"(n.a,n.b)ISTEMPORAL"}},
		{"CREATE CONSTRAINT c FOR ()-[r:T]-() REQUIRE (r.a, r.b) IS TEMPORAL NO OVERLAP", []string{"(r.a,r.b)ISTEMPORALNOOVERLAP"}},
		{"CREATE CONSTRAINT c FOR (NO:N) REQUIRE NO.OVERLAP IS TEMPORAL NO OVERLAP", []string{"NO.OVERLAPISTEMPORALNOOVERLAP"}},
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE { n.a IS TEMPORAL }", []string{"n.aISTEMPORAL"}},
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE { n.a IS TEMPORAL NO OVERLAP }", []string{"n.aISTEMPORALNOOVERLAP"}},
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE { n.a IS TEMPORAL\nNO OVERLAP }", []string{"n.aISTEMPORALNOOVERLAP"}},
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE { n.a IS TEMPORAL n.age > 0 }", []string{"n.aISTEMPORAL", "n.age>0"}},
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE { n.a IS TEMPORAL NO OVERLAP n.age > 0 }", []string{"n.aISTEMPORALNOOVERLAP", "n.age>0"}},
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE { (n.a, n.b) IS TEMPORAL NO OVERLAP; n.age > 0 }", []string{"(n.a,n.b)ISTEMPORALNOOVERLAP", "n.age>0"}},
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE { n.age > 0\n  n.a IS TEMPORAL\n  n.b IS TEMPORAL NO OVERLAP\n}", []string{"n.age>0", "n.aISTEMPORAL", "n.bISTEMPORALNOOVERLAP"}},
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE { n.a IS TEMPORAL NO.x > 0 }", []string{"n.aISTEMPORAL", "NO.x>0"}},
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE { n.a IS TEMPORAL OVERLAP.x > 0 }", []string{"n.aISTEMPORAL", "OVERLAP.x>0"}},
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE { n.a IS TEMPORAL NO OVERLAP NO.x > 0 }", []string{"n.aISTEMPORALNOOVERLAP", "NO.x>0"}},
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE { n.a IS TEMPORAL no.overlap IS UNIQUE }", []string{"n.aISTEMPORAL", "no.overlapISUNIQUE"}},
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE { NO.x > 0 n.a IS TEMPORAL }", []string{"NO.x>0", "n.aISTEMPORAL"}},
	} {
		t.Run(tc.statement, func(t *testing.T) {
			parsed, err := Parse(tc.statement)
			require.NoError(t, err)
			// The entries of the statement's block, or without a block its
			// one requirement.
			var reads []string
			var visit func(antlr4.Tree) bool
			visit = func(item antlr4.Tree) bool {
				switch item := item.(type) {
				case *ConstraintBlockContext:
					for _, child := range item.GetChildren() {
						if entry, ok := child.(antlr4.ParserRuleContext); ok {
							reads = append(reads, entry.GetText())
						}
					}
					return true
				case *ConstraintRequirementContext:
					if _, block := item.GetChild(0).(*ConstraintBlockContext); !block {
						reads = append(reads, item.GetText())
						return true
					}
				}
				for _, child := range item.GetChildren() {
					if visit(child) {
						return true
					}
				}
				return false
			}
			visit(parsed.Tree)
			require.Equal(t, tc.reads, reads)
		})
	}
	for _, tc := range []struct{ statement, message string }{
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE n.a IS TEMPORAL NO OVERLAP NO OVERLAP", "syntax error: line 1:65 unexpected input"},
		{"CREATE CONSTRAINT c FOR (n:N) REQUIRE n.a IS TEMPORAL NO OVERLAP extra", "syntax error: line 1:65 unexpected input"},
	} {
		t.Run(tc.statement, func(t *testing.T) {
			err := Validate(tc.statement)
			require.Error(t, err)
			require.Equal(t, tc.message, err.Error())
		})
	}
}

// A requirement comes before an expression among a block's entries, as it did
// when the grammar read a block as a list of predicates: where a run of
// tokens reads as a requirement and a longer one as an expression, the entry
// is the requirement. LIST<STRING> before a name is the type and then the
// next entry, not a comparison with '<' and '>', and ALLOWED before a block
// is the requirement and then the block, not a map projection. The readers of
// the tree therefore find no type word and no requirement word among the
// statement's names.
func TestContractBlockRequirementComesBeforeAnExpression(t *testing.T) {
	for _, tc := range []struct {
		statement string
		reads     []string
		variables []string
	}{
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { n.tags IS :: LIST<STRING> n.age > 0 }", []string{"n.tagsIS::LIST<STRING>", "n.age>0"}, []string{"c", "n", "Doc", "tags", "age"}},
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE {\n  n.tags IS :: LIST<STRING>\n  n.age > 0\n}", []string{"n.tagsIS::LIST<STRING>", "n.age>0"}, []string{"c", "n", "Doc", "tags", "age"}},
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE {\n  n.tags IS :: LIST<STRING>\n  n.status IN ['a', 'b']\n  n.id IS UNIQUE\n}", []string{"n.tagsIS::LIST<STRING>", "n.statusIN['a','b']", "n.idISUNIQUE"}, []string{"c", "n", "Doc", "tags", "status", "id"}},
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { n.tags IS :: LIST<INTEGER NOT NULL>\n n.age > $min }", []string{"n.tagsIS::LIST<INTEGERNOTNULL>", "n.age>$min"}, []string{"c", "n", "Doc", "tags", "age", "min"}},
		{"CREATE CONSTRAINT c FOR ()-[r:T]-() REQUIRE { r.tags IS :: LIST<STRING>\n MAX COUNT 1 }", []string{"r.tagsIS::LIST<STRING>", "MAXCOUNT1"}, []string{"c", "r", "T", "tags"}},
		{"CREATE CONSTRAINT c FOR ()-[r:T]-() REQUIRE { r.tags IS :: LIST<STRING> ALLOWED }", []string{"r.tagsIS::LIST<STRING>", "ALLOWED"}, []string{"c", "r", "T", "tags"}},
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { n.tags IS :: LIST<STRING> n.status IN ['a'] }", []string{"n.tagsIS::LIST<STRING>", "n.statusIN['a']"}, []string{"c", "n", "Doc", "tags", "status"}},
		{"CREATE CONSTRAINT c FOR ()-[r:T]-() REQUIRE { ALLOWED {  } ALLOWED }", []string{"ALLOWED", "{}", "ALLOWED"}, []string{"c", "r", "T"}},
		{"CREATE CONSTRAINT c FOR ()-[r:T]-() REQUIRE { DISALLOWED { ALLOWED } DISALLOWED }", []string{"DISALLOWED", "{ALLOWED}", "DISALLOWED"}, []string{"c", "r", "T"}},
		// The requirement also wins where what follows it reads only because
		// AND and OR are names too. That is the list form's reading of these
		// blocks, kept as it was.
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { n.a IS NOT NULL AND n.b > 0 }", []string{"n.aISNOTNULL", "AND", "n.b>0"}, nil},
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { n.a IN ['x'] OR n.b > 0; n.id IS UNIQUE }", []string{"n.aIN['x']", "OR", "n.b>0", "n.idISUNIQUE"}, nil},
		// A requirement whose expression holds the comparison is one entry.
		{"CREATE CONSTRAINT c FOR (n:Doc) REQUIRE { n.age > 0 AND n.tags IS :: LIST<STRING> }", []string{"n.age>0ANDn.tagsIS::LIST<STRING>"}, []string{"c", "n", "Doc", "age", "tags"}},
	} {
		t.Run(tc.statement, func(t *testing.T) {
			parsed, err := Parse(tc.statement)
			require.NoError(t, err)
			var reads []string
			var visit func(antlr4.Tree) bool
			visit = func(item antlr4.Tree) bool {
				if block, ok := item.(*ConstraintBlockContext); ok {
					for _, child := range block.GetChildren() {
						if entry, ok := child.(antlr4.ParserRuleContext); ok {
							reads = append(reads, entry.GetText())
						}
					}
					return true
				}
				for _, child := range item.GetChildren() {
					if visit(child) {
						return true
					}
				}
				return false
			}
			visit(parsed.Tree)
			require.Equal(t, tc.reads, reads)
			if tc.variables != nil {
				require.Equal(t, tc.variables, ExtractClauses(tc.statement, parsed).Variables)
			}
		})
	}
}

// The reader looks for a requirement by an earlier alternative in a run only
// when the run holds a word such a requirement has, and takes the order of
// the alternatives from the rule (earlierRequirementMayRead,
// contractEntryChoice). This is the grammar's rule both are taken from: a
// change of the rule is a change of them.
func TestContractRequirementWordsAreTheGrammars(t *testing.T) {
	grammar, err := os.ReadFile("CypherParser.g4")
	require.NoError(t, err)
	text := strings.ReplaceAll(string(grammar), "\r\n", "\n")
	start := strings.Index(text, "\nconstraintRequirement\n")
	require.GreaterOrEqual(t, start, 0)
	rule := text[start+1:]
	rule = rule[:strings.Index(rule, "\n    ;")]
	require.Equal(t, strings.Join(strings.Fields(`constraintRequirement
    : (expression | parenExpressionChain) IS ((NODE | RELATIONSHIP | REL)? (UNIQUE | KEY) | NOT NULL_W | COLON COLON propertyTypeName | TYPED propertyTypeName | TEMPORAL (NO OVERLAP)?)
    | expression IN listLit
    | MAX COUNT integerLit
    | ALLOWED
    | DISALLOWED
    | constraintBlock
    | constraintEpochRequirement`), " "), strings.Join(strings.Fields(rule), " "))
	start = strings.Index(text, "\nconstraintEpochRequirement\n")
	require.GreaterOrEqual(t, start, 0)
	rule = text[start+1:]
	rule = rule[:strings.Index(rule, "\n    ;")]
	require.Equal(t, strings.Join(strings.Fields(`constraintEpochRequirement
    : functionInvocation (GROUP BY expression (COMMA expression)*)?`), " "), strings.Join(strings.Fields(rule), " "))
}

// Epoch grouping belongs to its own requirement even when adjacent entries
// have no semicolons. The retained tree must keep that boundary for every
// consumer, rather than merely accepting the complete token sequence.
func TestContractEpochEntriesRetainTheirGroupingBoundaries(t *testing.T) {
	for _, separator := range []string{" ", "; ", "\n"} {
		t.Run(fmt.Sprintf("separator=%q", separator), func(t *testing.T) {
			parts := []string{
				"n.id IS UNIQUE",
				"temporal.epoch(n.starts,n.ends) GROUP BY n.assetId,n.tenant",
				"n.hours > 0",
				"temporal.epoch(n.starts,n.ends) GROUP BY n.employeeId",
				"n.status IN ['active','inactive']",
			}
			statement := "CREATE CONSTRAINT c FOR (n:Period) REQUIRE { " + strings.Join(parts, separator) + " }"
			syntax, err := ParseSyntax(context.Background(), statement, StatementEntry, false, AcceptedSource)
			require.NoError(t, err)
			var epochs []string
			syntax.Walk(func(node SyntaxNode) func() {
				if node.Rule() == CypherParserRULE_constraintEpochRequirement {
					start, end := node.Span()
					epochs = append(epochs, statement[start:end])
				}
				return nil
			})
			require.Equal(t, []string{parts[1], parts[3]}, epochs)
		})
	}
}

func TestContractBlockEntriesReachTheReadersOfTheTree(t *testing.T) {
	statement := "CREATE CONSTRAINT c FOR (n:Doc) REQUIRE {\n  n.id IS UNIQUE\n  NOT EXISTS { (n)-[:BAD]->(inner:Inner) }\n}"
	parsed, err := Parse(statement)
	require.NoError(t, err)
	info := NewQueryAnalyzer().Analyze(statement, parsed)
	require.True(t, info.HasSchema)
	require.Subset(t, info.Labels, []string{"Doc", "BAD", "Inner"}, "the analyzer reads the entry's pattern")
	clauses := ExtractClauses(statement, parsed)
	require.Subset(t, clauses.Variables, []string{"n", "inner", "id"}, "the extractor reads the entry's names")
}

// Reading a contract block costs what its entries hold, however many follow:
// an entry of a block sixteen times as long costs no more to read. Entries
// are separated by ';', stand on lines or on one line, or are blocks.
func TestContractBlockReadingCostsItsEntries(t *testing.T) {
	entry := func(i int) string {
		switch i % 5 {
		case 0:
			return fmt.Sprintf("n.p%d IS :: INTEGER", i)
		case 1:
			return fmt.Sprintf("n.p%d IN ['a', 'b']", i)
		case 2:
			return fmt.Sprintf("n.p%d > 0", i)
		case 3:
			return fmt.Sprintf("n.p%d IS NOT NULL", i)
		default:
			return "NOT EXISTS { (n)-[:BAD]->() }"
		}
	}
	block := func(i int) string { return fmt.Sprintf("{ n.p%d > 0 n.p%d < 200 }", i, i) }
	for _, shape := range []struct {
		name, separator string
		entry           func(int) string
	}{
		{"entries by semicolon", "; ", entry},
		{"entries on lines", "\n  ", entry},
		{"entries on one line", " ", entry},
		{"a block an entry", " ", block},
	} {
		t.Run(shape.name, func(t *testing.T) {
			// allocations is how many allocations one parse of a block of
			// count entries makes, for each entry.
			allocations := func(count int) float64 {
				entries := make([]string, count)
				for i := range entries {
					entries[i] = shape.entry(i)
				}
				statement := "CREATE CONSTRAINT c FOR (n:N) REQUIRE { " + strings.Join(entries, shape.separator) + " }"
				_, err := Parse(statement)
				require.NoError(t, err)
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				_, err = Parse(statement)
				runtime.ReadMemStats(&after)
				require.NoError(t, err)
				return float64(after.Mallocs-before.Mallocs) / float64(count)
			}
			short, long := allocations(40), allocations(640)
			t.Logf("allocations an entry: %.0f in a block of 40, %.0f in a block of 640", short, long)
			require.LessOrEqual(t, long, short*1.25, "an entry of the longer block costs what an entry of the shorter one costs")
		})
	}
}

// A shell command's arguments are read as tokens: their delimiters balance
// and a map is a map literal.
func TestShellCommandArgumentsBalance(t *testing.T) {
	for _, command := range []string{":param x => 1", ":param x => (1 + 2) * [3][0]", ":params {a: 1, b: 1 + 1}", ":use system", ":params"} {
		require.NoError(t, Validate(command), command)
	}
	for _, command := range []string{":param x => )", ":param {a: 1, b: }", ":use system extra ]", ":param x => (1 + 2"} {
		require.Error(t, Validate(command), command)
	}
}
