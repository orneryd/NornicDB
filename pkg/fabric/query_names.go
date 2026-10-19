package fabric

import (
	antlr4 "github.com/antlr4-go/antlr/v4"
	grammar "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"strings"
	"sync"
)

type QueryNameOccurrence struct {
	Start, End                                              int
	Name, Written                                           string
	Variable, Binding, Parameter, ProcedureColumn, Selector bool
}

// NameOccurrences supplies the grammar role of every symbolic name to the
// existing identifier canonicalizer, including comprehension/CALL bindings.
func NameOccurrences(text string) ([]QueryNameOccurrence, bool) {
	q := ReadQueryStructure(text)
	if q.Err != nil {
		return nil, false
	}
	r := &structureReader{text: text}
	for i := range text {
		r.bytes = append(r.bytes, i)
	}
	r.bytes = append(r.bytes, len(text))
	var names []QueryNameOccurrence
	var visit func(antlr4.Tree, antlr4.Tree, bool)
	visit = func(tree, parent antlr4.Tree, inParameter bool) {
		if _, ok := tree.(grammar.IParameterContext); ok {
			inParameter = true
		}
		if symbol, ok := tree.(grammar.ISymbolContext); ok {
			n := QueryNameOccurrence{Start: r.start(symbol.GetStart()), End: r.end(symbol.GetStop()), Name: unquoteSyntaxName(symbol.GetText()), Written: r.source(symbol), Parameter: inParameter}
			switch p := parent.(type) {
			case grammar.IAtomContext, grammar.IMapProjectionContext, grammar.IFabricCallScopeContext:
				n.Variable = true
			case grammar.IMapProjectionItemContext:
				n.Variable = true
				n.Selector = true
			case grammar.INodePatternContext, grammar.IRelationDetailContext, grammar.IPatternPartContext, grammar.IUnwindStContext, grammar.IForStContext, grammar.IProjectionItemContext, grammar.ILetItemContext, grammar.IForeachStContext, grammar.IFilterExpressionContext, grammar.IReduceExpressionContext, grammar.IAllReduceExpressionContext, grammar.IPatternComprehensionContext:
				n.Variable, n.Binding = true, true
			case grammar.IYieldItemContext:
				symbols := p.AllSymbol()
				n.Variable, n.Binding = true, true
				n.ProcedureColumn = len(symbols) == 1 || symbols[0] == symbol
			}
			names = append(names, n)
		}
		for i := 0; i < tree.GetChildCount(); i++ {
			visit(tree.GetChild(i), tree, inParameter)
		}
	}
	visit(q.tree, nil, false)
	return names, true
}

var keywordNames struct {
	sync.Once
	words map[string]bool
}

func KeywordName(name string) bool {
	keywordNames.Do(func() {
		keywordNames.words = make(map[string]bool)
		lexer := grammar.NewCypherLexer(antlr4.NewInputStream(""))
		for _, literal := range lexer.GetLiteralNames() {
			word := strings.Trim(literal, "'")
			if word != "" {
				keywordNames.words[strings.ToUpper(word)] = true
			}
		}
	})
	return keywordNames.words[strings.ToUpper(name)]
}

type QueryTextEdit struct {
	Start, End int
	Text       string
}

// StandaloneOrderEdits renders only modifiers that the grammar classified as
// separate clauses. Attached projection modifiers require no keyword scan.
func StandaloneOrderEdits(text string) ([]QueryTextEdit, bool) {
	q := ReadQueryStructure(text)
	if q.Err != nil {
		return nil, false
	}
	var edits []QueryTextEdit
	var visit func(*QueryStructure)
	visit = func(q *QueryStructure) {
		stage, inProjection := 0, false
		for _, c := range q.Clauses {
			next := 0
			switch c.Kind {
			case "ORDER BY":
				next = 1
			case "SKIP":
				next = 2
			case "LIMIT":
				next = 3
			}
			if next != 0 {
				replacement := ""
				if !inProjection || next <= stage {
					replacement = "WITH * "
				}
				inProjection, stage = true, next
				// OFFSET is a synonym whose spelling belongs to this actual clause.
				if node, ok := c.tree.(grammar.IFabricOffsetStContext); ok {
					end := c.Start + len(node.OFFSET().GetText())
					edits = append(edits, QueryTextEdit{c.Start, end, replacement + "SKIP"})
				} else if replacement != "" {
					edits = append(edits, QueryTextEdit{c.Start, c.Start, replacement})
				}
			} else {
				inProjection, stage = c.Projection != nil, 0
				if p := c.Projection; p != nil {
					if len(p.Order) > 0 {
						stage = 1
					}
					if p.Skip != "" {
						stage = 2
					}
					if p.Limit != "" {
						stage = 3
					}
					if c.Predicate != "" {
						stage = 4
					}
				}
			}
			if c.Nested != nil {
				visit(c.Nested)
			}
		}
	}
	visit(q)
	return edits, true
}
