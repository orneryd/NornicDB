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

// The maintained grammar's complete Symbol-parent inventory. Projection and
// query bindings, local bindings, references, assignment targets and protocol
// column names are different roles. Every name consumer selects its needed
// roles here instead of rediscovering them from source text or parent lists.
type grammarSymbolRole struct {
	variable, binding, queryBinding, expression, selector, procedureColumn bool
}

func symbolRole(symbol grammar.ISymbolContext, parent antlr4.Tree) grammarSymbolRole {
	r := grammar.RoleOfSymbol(symbol, parent)
	return grammarSymbolRole{variable: r.Variable, binding: r.Binding, queryBinding: r.QueryBinding, expression: r.Expression, selector: r.Selector, procedureColumn: r.ProcedureColumn}
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
			role := symbolRole(symbol, parent)
			n := QueryNameOccurrence{Start: r.start(symbol.GetStart()), End: r.end(symbol.GetStop()), Name: unquoteSyntaxName(symbol.GetText()), Written: r.source(symbol), Parameter: inParameter,
				Variable: role.variable, Binding: role.binding, Selector: role.selector, ProcedureColumn: role.procedureColumn}
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

// PreserveNameRoles freezes grammar-owned keyword/name decisions before a
// planner removes imports or splits a query into fragments. Quoting a variable
// preserves its role even when its binding belongs to an earlier fragment.
func PreserveNameRoles(text string) string {
	names, ok := NameOccurrences(text)
	if !ok {
		return text
	}
	var out strings.Builder
	end := 0
	for _, name := range names {
		if !name.Variable || name.Parameter || strings.HasPrefix(name.Written, "`") || !KeywordName(name.Name) {
			continue
		}
		out.WriteString(text[end:name.Start])
		out.WriteString("`" + strings.ReplaceAll(name.Name, "`", "``") + "`")
		end = name.End
	}
	if end == 0 {
		return text
	}
	out.WriteString(text[end:])
	return out.String()
}
