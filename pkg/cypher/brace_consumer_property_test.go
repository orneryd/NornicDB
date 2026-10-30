package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"unicode"

	"github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// This is a behavioral property, not a list of approved source readers. The
// grammar oracle sees the original query; each consumer sees its real input.
// No expected brace kind is supplied to a consumer.
func TestBraceConsumerAgreementProperty(t *testing.T) {
	entries := []string{"", "k: 1", "k: 1, other: 2", "`k-x`: 1", "a$b: 1",
		"bad", "1", ".k", ".*", "1bad: 1", "k:", "k: 1,", ",k: 1", "k: 1,, other: 2", ",", "$p", "k 1", "'k': 1", "\"k\": 1"}
	ctx := context.Background()
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "brace_property"))
	for entryIndex, entry := range entries {
		for gapIndex, gap := range braceOwnerGaps {
			literal := "{" + gap + strings.ReplaceAll(entry, " ", gap) + gap + "}"
			query := "RETURN " + literal + " AS out"
			t.Run(fmt.Sprintf("entry%d/gap%d", entryIndex, gapIndex), func(t *testing.T) {
				oracle := cypherantlr.Validate(query) == nil
				check := func(consumer string, accepted bool) {
					t.Helper()
					if accepted != oracle {
						t.Errorf("%s accepted=%v, ANTLR=%v: %s", consumer, accepted, oracle, query)
					}
				}
				check("preflight", validateStaticMapKeys(query) == nil)
				_, err := NewParser().Parse(query)
				check("public Parser.Parse", err == nil)
				_, err = NewASTBuilder().Build(query)
				check("public ASTBuilder.Build", err == nil)
				literalView := canonicalFragmentView("RETURN ", literal, " AS out")
				check("entry materializer", forEachMapEntry(literal, func(string, string) {}) == nil)
				_, err = parsePropertyExpressions(literalView)
				check("property plan", err == nil)
				_, err = executor.parseSetMergeMapLiteralStrict(ctx, literalView)
				check("SET materialization", err == nil)
				_, err = executor.materializePropertyMap(ctx, literalView, nil, false, false)
				check("CREATE materialization", err == nil)
				_, err = executor.materializePropertyMap(ctx, literalView, nil, true, false)
				check("MERGE materialization", err == nil)
				_, compiled := compileExecutionInput(literalView, 0)
				check("execution input", compiled)
				check("static map keys", visitStaticMapLiteralKeys(literalView, func(string) {}))
				_, evaluated := executor.evaluatePipelineAggregateExpressionWithContext(ctx, nil, literalView)
				check("aggregate evaluation", evaluated)
				_, resolved, err := executor.evaluateRowValue(literalView, nil)
				check("row evaluation", resolved && err == nil)
				ast := NewASTBuilder().parseExpression(cyphertext.NormalizeSource(literal))
				check("AST expression", ast.Type == ASTExprMap)
				_, _, _, single := parseCallTailSinglePropertyMap(literalView)
				wantSingle := oracle && entry != "" && !strings.Contains(entry, ",")
				if single != wantSingle {
					t.Errorf("CALL fast path=%v, supported ANTLR map=%v: %s", single, wantSingle, literal)
				}
				for _, prefix := range []string{"CREATE (n ", "MERGE (n "} {
					_, err := executor.Execute(ctx, prefix+literal+") RETURN n", nil)
					check(prefix, err == nil)
				}
				_, err = executor.Execute(ctx, "CREATE (n) SET n += "+literal+" RETURN n", nil)
				check("public SET", err == nil)
			})
			for _, template := range []string{
				"RETURN [(a %s)-->(b) | a] AS out",
				"WITH [(a %s)-->(b) | a] AS out RETURN out",
				"RETURN [(a)-[r %s]->(b) | r] AS out",
				"CALL { RETURN %s AS out } RETURN out",
				"RETURN COUNT { WITH %s AS out RETURN out } AS out",
			} {
				query := fmt.Sprintf(template, literal)
				t.Run(fmt.Sprintf("context%d/gap%d/%s", entryIndex, gapIndex, template), func(t *testing.T) {
					oracle := cypherantlr.Validate(query) == nil
					if accepted := validateStaticMapKeys(query) == nil; accepted != oracle {
						t.Errorf("context preflight=%v, ANTLR=%v: %s", accepted, oracle, query)
					}
					// Exercise both real text rewrites, then check the resulting
					// brace admission. A rewrite must not hide a malformed map.
					labels, _, labelErr := desugarLabelExpressions(query, nil, false)
					if (labelErr == nil && validateStaticMapKeys(labels) == nil) != oracle {
						t.Errorf("label rewrite changed map verdict: %s -> %s", query, labels)
					}
					structure, _, structureErr := desugarQueryStructure(ctx, query, executor)
					if (structureErr == nil && validateStaticMapKeys(structure) == nil) != oracle {
						t.Errorf("structure rewrite changed map verdict: %s -> %s", query, structure)
					}
				})
			}
			projection := "n" + gap + literal
			projectionQuery := "WITH {k: 1} AS n, 2 AS bad RETURN " + projection + " AS out"
			t.Run(fmt.Sprintf("projection%d/gap%d", entryIndex, gapIndex), func(t *testing.T) {
				projectionView := canonicalFragmentView("WITH {k: 1} AS n, 2 AS bad RETURN ", projection, " AS out")
				oracle := cypherantlr.Validate(projectionQuery) == nil
				// The owner declines the split of a malformed projection; release
				// rejects those statements too (differential agreement/projection).
				if variable, _, projection := rowMapProjectionSplit(projectionView); oracle && (!projection || variable.text() != "n") {
					t.Fatalf("projection splitter lost the variable across trivia: %q", variable.text())
				}
				_, handled, resolved, err := executor.evaluateRowMapProjection(projectionView, map[string]interface{}{"n": map[string]interface{}{"k": int64(1)}, "bad": int64(2)})
				if accepted := handled && resolved && err == nil; accepted != oracle {
					t.Errorf("projection evaluation=%v, ANTLR=%v: %s (err=%v)", accepted, oracle, projectionQuery, err)
				}
			})
		}
	}
}

// ANTLR classifies each original brace independently. The properties wrapper
// distinguishes a pattern's map from an expression's literal map.
func TestBraceConsumerKindsMatchANTLR(t *testing.T) {
	for _, template := range []string{
		"RETURN «L{k: 1} AS out",
		"WITH «L{k: 1} AS n RETURN (n «P{.k}).k AS out",
		"RETURN [(a «T{k: 1})-->(b) | a] AS out",
		"WITH [(a)-[r «T{k: 1}]->(b) | r] AS out RETURN out",
		"CALL () «S{ CREATE (n «T{k: 1}) RETURN n } RETURN n",
		"MATCH (n) WHERE EXISTS «S{ MATCH (n)-->(m «T{k: 1}) } RETURN n",
	} {
		for _, gap := range braceOwnerGaps {
			query, slots := expandBraceTemplate(template, gap)
			t.Run(query, func(t *testing.T) {
				parsed, err := cypherantlr.Parse(query)
				if err != nil {
					t.Fatal(err)
				}
				oracle := make(map[int]mapBraceKind)
				var walk func(antlr.Tree)
				walk = func(tree antlr.Tree) {
					if rule, ok := tree.(antlr.ParserRuleContext); ok {
						kind, owns := literalBrace, true
						switch rule.GetRuleIndex() {
						case cypherantlr.CypherParserRULE_mapLit:
							if parent, ok := rule.GetParent().(antlr.ParserRuleContext); ok && parent.GetRuleIndex() == cypherantlr.CypherParserRULE_properties {
								kind = patternPropertyBrace
							}
						case cypherantlr.CypherParserRULE_mapProjection:
							kind = projectionBrace
						case cypherantlr.CypherParserRULE_callSubquery, cypherantlr.CypherParserRULE_subqueryExist:
							kind = subqueryBrace
						default:
							owns = false
						}
						if owns {
							for _, child := range rule.GetChildren() {
								if terminal, ok := child.(antlr.TerminalNode); ok && terminal.GetSymbol().GetTokenType() == cypherantlr.CypherParserLBRACE {
									oracle[terminal.GetSymbol().GetStart()] = kind
									break
								}
							}
						}
					}
					for _, child := range tree.GetChildren() {
						walk(child)
					}
				}
				walk(parsed.Tree)
				for _, slot := range slots {
					want, found := oracle[slot.open]
					if !found {
						t.Fatalf("oracle did not own brace at %d", slot.open)
					}
					if kind := mapBraceAt(query, slot.open); kind != want {
						t.Errorf("brace at %d: owner=%v ANTLR=%v", slot.open, kind, want)
					}
				}
			})
		}
	}
}

func TestBraceBodyConsumersProperty(t *testing.T) {
	for _, gap := range braceOwnerGaps {
		for _, query := range []string{
			"CALL" + gap + "()" + gap + "{ RETURN 1 AS out } RETURN out",
			"RETURN EXISTS" + gap + "{ MATCH (n) } AS out",
			"RETURN NOT EXISTS" + gap + "{ MATCH (n) } AS out",
			"WITH {} AS exists RETURN exists" + gap + "{.k} AS out",
			"RETURN 'NOT EXISTS { MATCH (n) }' AS out",
		} {
			t.Run(query, func(t *testing.T) {
				// The reference is the one brace owner's final roles.
				view := originalBraceView(query)
				call, exists, notExists := false, false, false
				view.braceOccurrences(func(_, _ int, fact braceFact) {
					if fact.kind != subqueryBrace {
						return
					}
					switch fact.bodyKeyword {
					case "":
						call = true
					case "EXISTS":
						exists = true
						before := strings.TrimRightFunc(query[:fact.introducer.Start], unicode.IsSpace)
						notExists = notExists || len(before) >= 3 && strings.EqualFold(before[len(before)-3:], "NOT")
					}
				})
				if got := callSubqueryAt(t.Context(), query, 0); got != call {
					t.Errorf("CALL=%v owner=%v", got, call)
				}
				if got := hasKeywordFollowedByBrace(query, "EXISTS"); got != exists {
					t.Errorf("EXISTS=%v owner=%v", got, exists)
				}
				if got := hasNotExistsFollowedByBrace(query); got != notExists {
					t.Errorf("NOT EXISTS=%v owner=%v", got, notExists)
				}
			})
		}
	}
}

func TestBraceQuantifierConsumersProperty(t *testing.T) {
	for _, bounds := range []string{"1", "1,2", "1,", ",2", "bad", "1 2", ",", "1,,2", "k: 1", "+1"} {
		for _, gap := range braceOwnerGaps {
			for _, pattern := range []string{"(a)-[:R]->%s(b)", "((a)-[:R]->(b))%s"} {
				query := "MATCH " + fmt.Sprintf(pattern, "{"+gap+bounds+gap+"}") + " RETURN a"
				t.Run(query, func(t *testing.T) {
					open := strings.Index(query, "{")
					owned := mapBraceAt(query, open) == quantifierBrace
					want := cypherantlr.Validate(query) == nil
					if owned != want {
						t.Errorf("owner=%v, ANTLR bounds verdict=%v", owned, want)
					}
					_, read := relationshipQuantifierAt(originalBraceView(query), open, len(query))
					if read != owned {
						t.Errorf("quantifier reader=%v owner=%v", read, owned)
					}
					if close := strings.Index(query, "]"); close >= 0 && strings.HasPrefix(pattern, "(a)-") {
						_, _, afterArrow := quantifierAfterArrow(originalBraceView(query), close, len(query))
						if afterArrow != owned {
							t.Errorf("arrow reader=%v owner=%v", afterArrow, owned)
						}
					}
					rewritten, _, err := desugarLabelExpressions(query, nil, false)
					accepted := err == nil && cypherantlr.Validate(rewritten) == nil
					if accepted != owned {
						t.Errorf("rewrite/ANTLR=%v owner=%v: %s (err=%v)", accepted, owned, rewritten, err)
					}
				})
			}
		}
	}
	for _, suffix := range []string{"+", "*"} {
		for _, path := range []string{"((a)-->(b))", "(1 + 2)"} {
			query := "MATCH " + path + suffix + " RETURN a"
			at := len("MATCH ") + len(path)
			want := path == "((a)-->(b))"
			if got := quantifiedGroupQuantifierAt(originalBraceView(query), at); got != want {
				t.Errorf("%s: group reader=%v want=%v", query, got, want)
			}
		}
	}
}

func TestBraceNestedEntryConsumersProperty(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "brace_nested_property"))
	for _, value := range []string{"{k: 1}", "[{k: 1}]", "{k: [1, 2]}", "{bad}", "[{1bad: 1}]", "{k: 1,}"} {
		for _, gap := range braceOwnerGaps {
			literal := "{" + gap + "outer: " + value + gap + "}"
			t.Run(literal, func(t *testing.T) {
				oracle := cypherantlr.Validate("RETURN "+literal+" AS out") == nil
				check := func(name string, accepted bool) {
					t.Helper()
					if accepted != oracle {
						t.Errorf("%s=%v ANTLR=%v", name, accepted, oracle)
					}
				}
				check("owner", validateStaticMapKeys(literal) == nil)
				literalView := canonicalFragmentView("RETURN ", literal, " AS out")
				check("materializer entries", forEachMapEntry(literal, func(string, string) {}) == nil)
				_, err := executor.materializePropertyMap(t.Context(), literalView, nil, true, true)
				check("expression map materialization", err == nil)
				_, compiled := compileExecutionInput(literalView, 0)
				check("execution input", compiled)
				check("AST expression", NewASTBuilder().parseExpression(cyphertext.NormalizeSource(literal)).Type == ASTExprMap)
				_, evaluated := executor.evaluatePipelineAggregateExpressionWithContext(t.Context(), nil, literalView)
				check("aggregate evaluation", evaluated)
			})
		}
	}
}

func TestBraceDistinctConsumerProperty(t *testing.T) {
	for _, gap := range braceOwnerGaps {
		for _, entry := range []string{".k", ".*", "k", "k: 1"} {
			rest := "{" + gap + entry + gap + "}"
			t.Run(rest, func(t *testing.T) {
				query := "WITH {k: 1} AS distinct, 2 AS k RETURN distinct " + rest + " AS out"
				parsed, err := cypherantlr.Parse(query)
				if err != nil {
					t.Fatal(err)
				}
				projection := false
				var walk func(antlr.Tree)
				walk = func(tree antlr.Tree) {
					if rule, ok := tree.(antlr.ParserRuleContext); ok && rule.GetRuleIndex() == cypherantlr.CypherParserRULE_mapProjection {
						projection = true
					}
					for _, child := range tree.GetChildren() {
						walk(child)
					}
				}
				walk(parsed.Tree)
				open := strings.Index(query, "RETURN distinct ") + len("RETURN distinct ")
				want := literalBrace
				if projection {
					want = projectionBrace
				}
				if got := mapBraceAt(query, open); got != want {
					t.Errorf("owner=%v, ANTLR kind=%v", got, want)
				}
				if got := distinctFollowsAsName(canonicalFragmentView("WITH {k: 1} AS distinct, 2 AS k RETURN distinct ", rest, " AS out")); got != projection {
					t.Errorf("DISTINCT name=%v, ANTLR projection=%v", got, projection)
				}
			})
		}
	}
}

func TestBraceReviewQueriesThroughExecute(t *testing.T) {
	for _, query := range []string{"UNWIND [] AS x RETURN [(a {bad})-->(b) | a] AS out", "RETURN {1bad: 1} AS out"} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%t/%s", explicit, query), func(t *testing.T) {
				executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "brace_review"))
				if explicit {
					if _, err := executor.Execute(t.Context(), "BEGIN", nil); err != nil {
						t.Fatal(err)
					}
					defer executor.Execute(context.Background(), "ROLLBACK", nil)
				}
				_, err := executor.Execute(t.Context(), query, nil)
				if !isMapEntrySyntaxError(err) {
					t.Fatalf("expected owner's map diagnostic, got %v", err)
				}
			})
		}
	}
}

// Reference-valued maps force the label rewriter to move entries. Structural
// wrappers exercise the real NEXT, UNION and nested-block transformations.
func TestBraceCourseChangeRewriteProperty(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "brace_rewrites"))
	for _, value := range []string{"a.k", "coalesce(a.k, 1)", "a{.k}"} {
		for _, suffix := range []string{"", ",", ",, other: 2", ", other:", ", 1bad: 2"} {
			for _, gap := range braceOwnerGaps {
				literal := "{" + gap + "k:" + gap + value + suffix + gap + "}"
				inner := "UNWIND [] AS x MATCH (a)-->(b " + literal + ") RETURN b AS out"
				oracle := cypherantlr.Validate("MATCH (a)-->(b "+literal+") RETURN b AS out") == nil
				for _, query := range []string{
					inner,
					"EXPLAIN " + inner,
					inner + " UNION RETURN null AS out",
					"CYPHER 25 " + inner + " NEXT RETURN out",
					"CYPHER 25 { " + inner + " } UNION { RETURN null AS out }",
					"CALL { " + inner + " } RETURN out",
					"CYPHER 25 CALL { { " + inner + " } } RETURN out",
				} {
					t.Run(query, func(t *testing.T) {
						rewriteQuery, _ := stripCypherPreamble(query)
						labels, _, err := desugarLabelExpressions(rewriteQuery, nil, true)
						if accepted := err == nil && validateStaticMapKeys(labels) == nil; accepted != oracle {
							t.Errorf("label rewrite=%v oracle=%v: %s (%v)", accepted, oracle, labels, err)
						}
						structure, _, err := desugarQueryStructure(withCypherVersion(t.Context(), "CYPHER 25 "+query), rewriteQuery, executor)
						if accepted := err == nil && validateStaticMapKeys(structure) == nil; accepted != oracle {
							t.Errorf("structure rewrite=%v oracle=%v: %s (%v)", accepted, oracle, structure, err)
						}
						_, err = executor.Execute(t.Context(), query, nil)
						if !oracle && !isMapEntrySyntaxError(err) {
							t.Errorf("public Execute lost original map diagnostic: %v", err)
						}
						// A projection value moved into WHERE (b.k = a{.k}) is rejected
						// by release f0c932b9 too (differential receipts 1620-1661,
						// jobs i20261030043624-49852-d3ee / -c325): not this owner.
						if oracle && err != nil && value != "a{.k}" {
							t.Errorf("valid rewrite: %v", err)
						}
					})
				}
			}
		}
	}
}

func TestBraceCourseChangeBatchProperty(t *testing.T) {
	for _, entry := range []string{"", "k: 1", "k: null", "k: row.k", "k: 1, other: 'x'", "k: 1,", ",k: 1", "k: 1,,other: 2", "bad", "1bad: 1"} {
		for _, gap := range braceOwnerGaps {
			body := gap + entry + gap
			oracle := cypherantlr.Validate("UNWIND [1] AS row CREATE (n {"+body+"}) RETURN n") == nil
			if _, _, accepted := parsePropsBodyForUnwindFastPath(canonicalFragmentView("UNWIND [1] AS row CREATE (n ", "{"+body+"}", ") RETURN n"), "row"); accepted != oracle {
				t.Errorf("batch=%v ANTLR=%v: %s", accepted, oracle, body)
			}
		}
	}
}

func TestBraceCourseChangeSemanticConsumersProperty(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "brace_semantics"))
	for _, entry := range []string{"k: 1", "k: null", "k: 1, other: 2", "k: 1,", ",k: 1", "k: 1,,other: 2", "bad", "1bad: 1"} {
		for _, gap := range braceOwnerGaps {
			body := gap + entry + gap
			literal := "{" + body + "}"
			pattern := "(n:L " + literal + ")"
			oracle := cypherantlr.Validate("CREATE "+pattern+" RETURN n") == nil
			t.Run(literal, func(t *testing.T) {
				if got := len(propertyMapPredicates("n", expressionBraceView(literal))) > 0; got != oracle {
					t.Errorf("path predicates=%v oracle=%v", got, oracle)
				}
				if got := patternMapsAreConstantText(canonicalFragmentView("CREATE ", pattern, " RETURN n")); got != oracle {
					t.Errorf("pattern constants=%v oracle=%v", got, oracle)
				}
				if _, got := parseUnwindSimpleMergeMatchAssignments(expressionBraceView(literal)); got != oracle {
					t.Errorf("batch match=%v oracle=%v", got, oracle)
				}
				if got := executor.validateCreatePropertyExpressions(newSemanticBindingScope(), canonicalFragmentView("CREATE (n:L ", literal, ") RETURN n")) == nil; got != oracle {
					t.Errorf("CREATE expression validation=%v oracle=%v", got, oracle)
				}
				if got := executor.validateCreateSamePatternReferences(newSemanticBindingScope(), canonicalFragmentView("CREATE ", pattern, " RETURN n"), nil, map[string]string{}, "CREATE") == nil; got != oracle {
					t.Errorf("CREATE reference validation=%v oracle=%v", got, oracle)
				}
				if got := len(appendPatternPropertyExpressions(nil, canonicalFragmentView("CREATE ", pattern, " RETURN n"), "L")) > 0; got != oracle {
					t.Errorf("query property expressions=%v oracle=%v", got, oracle)
				}
				tokens := stretchTokens{}
				addStretchMapKeys(expressionBraceView(literal), &tokens)
				if got := len(tokens.keys) > 0; got != oracle {
					t.Errorf("stretch map keys=%v oracle=%v", got, oracle)
				}
				if got := setItemKeepsMergeKey(originalBraceView("n += "+literal), "missing", nil, nil); got != oracle {
					t.Errorf("MERGE key preservation=%v oracle=%v", got, oracle)
				}
				if oracle {
					wantNull := strings.Contains(entry, "null")
					if got := executor.mergePatternContainsNullProperty(canonicalFragmentView("MERGE ", pattern, " RETURN n")); got != wantNull {
						t.Errorf("MERGE null=%v want=%v", got, wantNull)
					}
				}
			})
		}
	}
}
