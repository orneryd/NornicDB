package fabric

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	antlr4 "github.com/antlr4-go/antlr/v4"
	grammar "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// QueryStructure is Fabric's syntax boundary. Clause and expression ownership
// comes from the maintained Cypher grammar, independently of the execution
// parser and of query normalization. The Fabric envelope adds graph selection
// between importing WITH and a subquery, which the single-graph script rule
// cannot represent. No consumer recognizes keywords in source text.
type QueryStructure struct {
	Clauses    []QueryClause
	Err        error
	LocalText  string
	LocalEdits []QueryTextEdit
	tree       antlr4.ParserRuleContext
	unionErr   error
	parsed     bool
	statements int
}

type QueryClause struct {
	Kind                                      string
	Start, End, BodyStart                     int
	Text, Body, Remaining                     string
	Use                                       *UseClause
	Nested                                    *QueryStructure
	Projection                                *QueryProjection
	Yield                                     *QueryYield
	Pattern, Predicate                        string
	Expression, Name                          string
	Distinct                                  bool
	Optional                                  bool
	ExecutionText                             string
	InTransactions                            bool
	BatchSize                                 string
	Scoped                                    bool
	ScopeText                                 string
	embeddingStart, embeddingEnd              int
	EmbeddingCandidate                        string
	ScopeNames                                []string
	BodyOffset                                int
	tree                                      antlr4.ParserRuleContext
	endsStatement, schemaDrop, administration bool
	statement                                 int
	graphStart                                int
}

type QueryProjection struct {
	Items                             []QueryProjectionItem
	Distinct, Star                    bool
	Text, Modifiers, Tail             string
	OrderText                         string
	Aggregates                        bool
	itemsEnd                          int
	Order                             []QueryOrderItem
	Skip, Limit                       string
	OrderStart, SkipStart, LimitStart int
}

type QueryProjectionItem struct {
	Expression, Alias, Name string
	Column                  string
	HasAlias                bool
	tree                    grammar.IExpressionContext
}

type QueryOrderItem struct {
	Expression, Name, Key string
	Desc                  bool
}

type QueryYield struct {
	Items                       []QueryYieldItem
	Star                        bool
	Where, OrderBy, Skip, Limit string
}
type QueryYieldItem struct{ Name, Alias string }

type syntaxErrors struct {
	*antlr4.DefaultErrorListener
	err error
}

// QuerySyntaxError retains the parser's status through every Fabric consumer.
type QuerySyntaxError struct{ Message localization.Message }

func (e *QuerySyntaxError) Error() string         { return e.Message.Fallback }
func (e *QuerySyntaxError) BoltErrorCode() string { return "Neo.ClientError.Statement.SyntaxError" }

// Structural consumers require a complete parse; recovery cannot provide a
// usable structure. Stop at the first error instead of repairing a tree that
// callers will reject, particularly for ordinary extensions outside this grammar.
type structureErrorStrategy struct{ *antlr4.DefaultErrorStrategy }
type structureSyntaxAbort struct{}

func (s *structureErrorStrategy) Sync(antlr4.Parser) {}
func (s *structureErrorStrategy) Recover(parser antlr4.Parser, err antlr4.RecognitionException) {
	s.ReportError(parser, err)
	panic(structureSyntaxAbort{})
}
func (s *structureErrorStrategy) RecoverInline(parser antlr4.Parser) antlr4.Token {
	s.Recover(parser, antlr4.NewInputMisMatchException(parser))
	return nil
}

func (e *syntaxErrors) SyntaxError(_ antlr4.Recognizer, _ interface{}, line, column int, message string, _ antlr4.RecognitionException) {
	if e.err == nil {
		e.err = &QuerySyntaxError{Message: localization.CypherANTLRParseSyntaxError(fmt.Sprintf("at %d:%d: %s", line, column, message))}
	}
}

type structureReader struct {
	text   string
	bytes  []int
	stream *antlr4.CommonTokenStream
	parser *grammar.CypherParser
	errors *syntaxErrors
	shift  int
}

func newStructureReader(text string) *structureReader {
	errors := &syntaxErrors{DefaultErrorListener: antlr4.NewDefaultErrorListener()}
	lexer := grammar.NewCypherLexer(antlr4.NewInputStream(text))
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(errors)
	stream := antlr4.NewCommonTokenStream(lexer, antlr4.TokenDefaultChannel)
	parser := grammar.NewCypherParser(stream)
	parser.SetErrorHandler(&structureErrorStrategy{antlr4.NewDefaultErrorStrategy()})
	parser.RemoveErrorListeners()
	parser.AddErrorListener(errors)
	r := &structureReader{text: text, stream: stream, parser: parser, errors: errors}
	for i := range text {
		r.bytes = append(r.bytes, i)
	}
	r.bytes = append(r.bytes, len(text))
	return r
}

func (r *structureReader) offset(runeOffset int) int {
	runeOffset -= r.shift
	if runeOffset < 0 {
		return 0
	}
	if runeOffset >= len(r.bytes) {
		return len(r.text)
	}
	return r.bytes[runeOffset]
}
func (r *structureReader) start(t antlr4.Token) int { return r.offset(t.GetStart()) }
func (r *structureReader) end(t antlr4.Token) int   { return r.offset(t.GetStop() + 1) }
func (r *structureReader) source(tree antlr4.ParserRuleContext) string {
	if tree == nil || tree.GetStart() == nil || tree.GetStop() == nil {
		return ""
	}
	start, end := r.start(tree.GetStart()), r.end(tree.GetStop())
	if start > end {
		return ""
	}
	return strings.TrimSpace(r.text[start:end])
}

// Syntax results are immutable and bounded. Rewrites and CALL/batch execution
// reuse the same grammar result rather than reparsing for each input record.
// Long statements are not retained. This is a syntax cache, never session state.
var structureCache = struct {
	sync.Mutex
	entries map[string]*QueryStructure
	fifo    []string
}{entries: make(map[string]*QueryStructure)}

func ReadQueryStructure(text string) *QueryStructure {
	structureCache.Lock()
	cached := structureCache.entries[text]
	structureCache.Unlock()
	if cached != nil {
		return cached
	}
	// Use the runtime's SLL prediction cache first, retrying an incomplete
	// parse in full context. Both paths retain the complete grammar tree.
	r := newStructureReader(text)
	r.parser.Interpreter.SetPredictionMode(antlr4.PredictionModeSLL)
	tree := r.parseScript()
	if r.errors.err != nil {
		r = newStructureReader(text)
		r.parser.Interpreter.SetPredictionMode(antlr4.PredictionModeLL)
		tree = r.parseScript()
	}
	var result *QueryStructure
	if r.errors.err == nil {
		result = r.body(tree.FabricBody(), false)
	} else {
		result = r.partialUses(tree.FabricBody())
	}
	if result.Err == nil {
		result.LocalText = r.lower(result)
	}
	if len(text) <= 16384 {
		structureCache.Lock()
		if _, exists := structureCache.entries[text]; !exists {
			if len(structureCache.fifo) == 128 {
				delete(structureCache.entries, structureCache.fifo[0])
				structureCache.fifo = structureCache.fifo[1:]
			}
			structureCache.entries[text] = result
			structureCache.fifo = append(structureCache.fifo, text)
		}
		structureCache.Unlock()
	}
	return result
}

func (r *structureReader) parseScript() (tree grammar.IFabricScriptContext) {
	defer func() {
		if caught := recover(); caught != nil {
			if _, syntax := caught.(structureSyntaxAbort); !syntax {
				panic(caught)
			}
			root := r.parser.GetParserRuleContext()
			for root.GetParent() != nil {
				root = root.GetParent().(antlr4.ParserRuleContext)
			}
			tree = root.(grammar.IFabricScriptContext)
		}
	}()
	return r.parser.FabricScript()
}

func (r *structureReader) partialUses(body grammar.IFabricBodyContext) *QueryStructure {
	q := &QueryStructure{Err: r.errors.err, tree: body}
	var visit func(antlr4.Tree, bool)
	visit = func(tree antlr4.Tree, inCall bool) {
		if tree == nil {
			return
		}
		if _, ok := tree.(grammar.IFabricCallSubqueryContext); ok {
			inCall = true
		}
		if use, ok := tree.(grammar.IFabricUseClauseContext); ok && use.GetStart() != nil {
			start := r.start(use.GetStart())
			target, found := r.useReference(use)
			if found {
				clause := QueryClause{Kind: "USE", Start: start, Use: &target, graphStart: r.useReferenceStart(use)}
				if use.GetStop() != nil {
					clause.End = r.end(use.GetStop())
					clause.Remaining = strings.TrimSpace(r.text[clause.End:])
				}
				q.Clauses = append(q.Clauses, clause)
				var preset *QueryClause
				if clause.End > clause.Start {
					preset = &clause
				}
				text := r.text
				if preset == nil {
					text = r.text[start:]
				}
				if _, _, _, err := parseUseClauseFallback(text, inCall, preset); err != nil {
					q.Err = err
				}
			} else if _, _, _, err := parseUseClauseFallback(r.text[start:], inCall, nil); err != nil {
				q.Err = err
			}
		}
		for i := 0; i < tree.GetChildCount(); i++ {
			visit(tree.GetChild(i), inCall)
		}
	}
	visit(body, false)
	return q
}

// body projects the complete Fabric grammar tree into immutable execution
// syntax. Parsing a clause in isolation loses its following-clause context;
// in particular NOT, DISTINCT and USE may all be legitimate variable names.
func (r *structureReader) body(body grammar.IFabricBodyContext, inCall bool) *QueryStructure {
	q := &QueryStructure{tree: body, parsed: true}
	if body == nil {
		return q
	}
	startsStatement := true
	for _, clause := range body.AllFabricClause() {
		if clause.SEMI() != nil {
			if len(q.Clauses) > 0 {
				q.Clauses[len(q.Clauses)-1].endsStatement = true
			}
			startsStatement = true
			continue
		}
		if clause.GetStart() == nil || clause.GetStop() == nil {
			continue
		}
		if startsStatement {
			q.statements++
			startsStatement = false
		}
		c := QueryClause{Start: r.start(clause.GetStart()), End: r.end(clause.GetStop()), statement: q.statements}
		if option := clause.EmbeddingSt(); option != nil {
			c.embeddingStart, c.embeddingEnd = r.start(option.GetStart()), r.end(option.GetStop())
		}
		headEnd := r.end(clause.GetStart())
		switch {
		case clause.FabricUseClause() != nil:
			c.Kind = "USE"
			c.tree = clause.FabricUseClause()
			use, _ := r.useReference(clause.FabricUseClause())
			c.Use = &use
			c.graphStart = r.useReferenceStart(clause.FabricUseClause())
		case clause.WithSt() != nil:
			c.Kind = "WITH"
			t := clause.WithSt()
			c.tree = t
			c.Projection = r.projection(t.ProjectionBody())
			if t.Where() != nil {
				c.Predicate = r.source(t.Where().Expression())
			}
		case clause.ReturnSt() != nil:
			c.Kind = "RETURN"
			t := clause.ReturnSt()
			c.tree = t
			c.Projection = r.projection(t.ProjectionBody())
		case clause.MatchSt() != nil:
			c.Kind = "MATCH"
			t := clause.MatchSt()
			c.tree = t
			if t.OPTIONAL() != nil {
				c.Kind = "OPTIONAL MATCH"
				c.Optional = true
				headEnd = r.end(t.MATCH().GetSymbol())
			}
			if pw := t.PatternWhere(); pw != nil {
				c.Pattern = r.source(pw.Pattern())
				if pw.Where() != nil {
					c.Predicate = r.source(pw.Where().Expression())
				}
			}
		case clause.UnwindSt() != nil:
			c.Kind = "UNWIND"
			t := clause.UnwindSt()
			c.tree = t
			c.Expression = r.source(t.Expression())
			c.Name = unquoteSyntaxName(r.source(t.Symbol()))
		case clause.CreateSt() != nil:
			c.Kind = "CREATE"
			c.tree = clause.CreateSt()
		case clause.FabricInsertSt() != nil:
			c.Kind = "CREATE"
			c.tree = clause.FabricInsertSt()
		case clause.MergeSt() != nil:
			c.Kind = "MERGE"
			c.tree = clause.MergeSt()
		case clause.DeleteSt() != nil:
			c.Kind = "DELETE"
			t := clause.DeleteSt()
			c.tree = t
			if t.DETACH() != nil {
				c.Kind = "DETACH DELETE"
				headEnd = r.end(t.DELETE().GetSymbol())
			}
		case clause.FabricNoDetachDeleteSt() != nil:
			c.Kind = "DELETE"
			t := clause.FabricNoDetachDeleteSt()
			c.tree = t
			headEnd = r.end(t.DELETE().GetSymbol())
		case clause.SetSt() != nil:
			c.Kind = "SET"
			c.tree = clause.SetSt()
		case clause.RemoveSt() != nil:
			c.Kind = "REMOVE"
			c.tree = clause.RemoveSt()
		case clause.ForeachSt() != nil:
			c.Kind = "FOREACH"
			c.tree = clause.ForeachSt()
		case clause.LetSt() != nil:
			c.Kind = "LET"
			c.tree = clause.LetSt()
		case clause.FilterSt() != nil:
			c.Kind = "FILTER"
			c.tree = clause.FilterSt()
		case clause.ForSt() != nil:
			c.Kind = "FOR"
			c.tree = clause.ForSt()
		case clause.FabricCallSubquery() != nil:
			c.Kind = "CALL"
			t := clause.FabricCallSubquery()
			c.tree = t
			c.Optional = t.OPTIONAL() != nil
			c.InTransactions = t.IN() != nil
			c.Body = strings.TrimSpace(r.text[r.end(t.LBRACE().GetSymbol()):r.start(t.RBRACE().GetSymbol())])
			c.BodyOffset = r.start(t.LBRACE().GetSymbol())
			if t.NumLit() != nil {
				c.BatchSize = r.source(t.NumLit())
			}
			if t.Parameter() != nil {
				c.BatchSize = r.source(t.Parameter())
			}
			if scope := t.FabricCallScope(); scope != nil {
				c.Scoped = true
				start, end := r.end(scope.LPAREN().GetSymbol()), r.start(scope.RPAREN().GetSymbol())
				c.ScopeText = strings.TrimSpace(r.text[start:end])
				if scope.MULT() == nil {
					c.ScopeNames = []string{}
				}
				for _, symbol := range scope.AllSymbol() {
					c.ScopeNames = append(c.ScopeNames, unquoteSyntaxName(r.source(symbol)))
				}
			}
			c.Nested = r.body(t.FabricBody(), true)
			if c.Nested.Err != nil && q.Err == nil {
				q.Err = c.Nested.Err
			}
		case clause.QueryCallSt() != nil:
			c.Kind = "CALL"
			t := clause.QueryCallSt()
			c.tree = t
			c.Optional = t.OPTIONAL() != nil
			if t.YIELD() != nil {
				y := &QueryYield{Star: t.MULT() != nil}
				if items := t.YieldItems(); items != nil {
					for _, item := range items.AllYieldItem() {
						symbols := item.AllSymbol()
						v := QueryYieldItem{Name: unquoteSyntaxName(r.source(symbols[0]))}
						if len(symbols) > 1 {
							v.Alias = unquoteSyntaxName(r.source(symbols[1]))
						}
						y.Items = append(y.Items, v)
					}
					if items.Where() != nil {
						y.Where = r.source(items.Where().Expression())
					}
				}
				if order := t.OrderSt(); order != nil {
					var items []string
					for _, item := range order.AllOrderItem() {
						items = append(items, r.source(item))
					}
					y.OrderBy = strings.Join(items, ", ")
				}
				if t.SkipSt() != nil {
					y.Skip = r.source(t.SkipSt().Expression())
				}
				if t.LimitSt() != nil {
					y.Limit = r.source(t.LimitSt().Expression())
				}
				c.Yield = y
			}
		case clause.FabricLoadCsv() != nil:
			c.Kind = "LOAD CSV"
			c.tree = clause.FabricLoadCsv()
			c.Name = unquoteSyntaxName(r.source(clause.FabricLoadCsv().Symbol()))
			c.Expression = r.source(clause.FabricLoadCsv().Expression())
			headEnd = r.end(clause.FabricLoadCsv().CSV().GetSymbol())
		case clause.SchemaCommand() != nil:
			c.Kind = "SCHEMA"
			c.tree = clause.SchemaCommand()
			c.schemaDrop = clause.SchemaCommand().DROP() != nil
		case clause.ShowCommand() != nil:
			c.Kind = "SHOW"
			c.tree = clause.ShowCommand()
			show := clause.ShowCommand()
			c.administration = show.DATABASE() != nil || show.DATABASES() != nil || show.ALIASES() != nil || show.USERS() != nil || show.CURRENT() != nil || show.ROLES() != nil || show.PRIVILEGES() != nil || show.SERVERS() != nil
		case clause.TerminateCommand() != nil:
			c.Kind = "TERMINATE"
			c.tree = clause.TerminateCommand()
		case clause.OrderSt() != nil:
			c.Kind = "ORDER BY"
			c.tree = clause.OrderSt()
			headEnd = r.end(clause.OrderSt().BY().GetSymbol())
		case clause.SkipSt() != nil:
			c.Kind = "SKIP"
			c.tree = clause.SkipSt()
		case clause.FabricOffsetSt() != nil:
			c.Kind = "SKIP"
			c.tree = clause.FabricOffsetSt()
		case clause.LimitSt() != nil:
			c.Kind = "LIMIT"
			c.tree = clause.LimitSt()
		case clause.UNION() != nil:
			c.Kind = "UNION"
			c.Distinct = clause.ALL() == nil
		case clause.FINISH() != nil:
			c.Kind = "FINISH"
		case clause.QueryPrefix() != nil:
			c.Kind = strings.ToUpper(clause.GetStart().GetText())
			c.tree = clause.QueryPrefix()
		case clause.CypherGroup() != nil:
			c.Kind = "CYPHER"
			c.tree = clause.CypherGroup()
		}
		if c.End < c.Start {
			continue
		}
		c.Text = strings.TrimSpace(r.text[c.Start:c.End])
		c.Remaining = strings.TrimSpace(r.text[c.End:])
		c.ExecutionText = c.Text
		if c.Kind != "CALL" {
			c.BodyStart = min(skipSpacesAndComments(r.text, headEnd), c.End)
			c.Body = strings.TrimSpace(r.text[c.BodyStart:c.End])
		}
		if c.Optional && c.Kind == "CALL" {
			var call antlr4.TerminalNode
			if t := clause.FabricCallSubquery(); t != nil {
				call = t.CALL()
			} else {
				call = clause.QueryCallSt().CALL()
			}
			c.ExecutionText = strings.TrimSpace(r.text[r.start(call.GetSymbol()):c.End])
		}
		if c.Projection != nil {
			c.Projection.Tail = strings.TrimSpace(r.text[min(c.Projection.itemsEnd, c.End):c.End])
		}
		// The documented nonterminal option sits after an updating clause and
		// immediately before RETURN. This is syntax eligibility, not a scope
		// decision: the existing pipeline frame will resolve the variable.
		if c.Kind == "WITH" && len(q.Clauses) > 0 && c.Projection != nil {
			previous := q.Clauses[len(q.Clauses)-1].Kind
			updating := previous == "CREATE" || previous == "MERGE" || previous == "SET" || previous == "REMOVE" || previous == "DELETE" || previous == "DETACH DELETE" || previous == "FOREACH"
			projection := c.Projection
			if updating && len(projection.Items) == 1 && !projection.Star && !projection.Distinct && c.Predicate == "" && projection.Tail == "" {
				item := projection.Items[0]
				if !item.HasAlias && strings.EqualFold(item.Name, "embedding") && !strings.HasPrefix(strings.TrimSpace(item.Expression), "`") {
					c.EmbeddingCandidate = item.Name
				}
			}
		}
		q.Clauses = append(q.Clauses, c)
	}
	for i := range q.Clauses {
		if q.Clauses[i].EmbeddingCandidate != "" && (i+1 >= len(q.Clauses) || q.Clauses[i+1].Kind != "RETURN") {
			q.Clauses[i].EmbeddingCandidate = ""
		}
	}
	if q.Err == nil {
		q.Err = q.validateUses(inCall)
	}
	q.unionErr = q.validateUnionSchemas()
	return q
}

func unquoteSyntaxName(s string) string {
	if len(s) > 1 && s[0] == '`' {
		return strings.ReplaceAll(s[1:len(s)-1], "``", "`")
	}
	return s
}

func (r *structureReader) projection(body grammar.IProjectionBodyContext) *QueryProjection {
	if r.errors.err != nil {
		return nil
	}
	if body == nil || body.ProjectionItems() == nil {
		return nil
	}
	p := &QueryProjection{Distinct: body.DISTINCT() != nil, Star: body.ProjectionItems().MULT() != nil, Text: r.source(body.ProjectionItems()), OrderStart: -1, SkipStart: -1, LimitStart: -1}
	for _, item := range body.ProjectionItems().AllProjectionItem() {
		v := QueryProjectionItem{Expression: r.source(item.Expression()), tree: item.Expression()}
		if item.Symbol() != nil {
			v.Alias = unquoteSyntaxName(r.source(item.Symbol()))
			v.HasAlias = true
		}
		v.Name = syntaxIdentifier(item.Expression())
		v.Column = v.Expression
		if symbol, ok := syntaxOnly[grammar.ISymbolContext](item.Expression()); ok {
			v.Column = unquoteSyntaxName(symbol.GetText())
		}
		if v.HasAlias {
			v.Column = v.Alias
		}
		walkSyntax(item.Expression(), func(node antlr4.Tree) {
			switch value := node.(type) {
			case grammar.ICountAllContext:
				p.Aggregates = true
			case grammar.IFunctionInvocationContext:
				if value.InvocationName() == nil {
					break // The grammar's TRIM ... FROM alternative has no invocationName.
				}
				switch strings.ToLower(value.InvocationName().GetText()) {
				case "count", "sum", "avg", "min", "max", "collect", "stdev", "stdevp", "percentilecont", "percentiledisc":
					p.Aggregates = true
				}
			}
		})
		p.Items = append(p.Items, v)
	}
	if order := body.OrderSt(); order != nil {
		p.OrderStart = r.start(order.GetStart())
		for _, item := range order.AllOrderItem() {
			p.Order = append(p.Order, QueryOrderItem{Expression: r.source(item.Expression()), Name: syntaxIdentifier(item.Expression()), Key: syntaxExpressionKey(item.Expression()), Desc: item.DESC() != nil || item.DESCENDING() != nil})
		}
		var items []string
		for _, item := range order.AllOrderItem() {
			items = append(items, r.source(item))
		}
		p.OrderText = strings.Join(items, ", ")
	}
	if skip := body.SkipSt(); skip != nil {
		p.Skip = r.source(skip.Expression())
		p.SkipStart = r.start(skip.GetStart())
	}
	if limit := body.LimitSt(); limit != nil {
		p.Limit = r.source(limit.Expression())
		p.LimitStart = r.start(limit.GetStart())
	}
	end := r.end(body.ProjectionItems().GetStop())
	p.itemsEnd = end
	p.Tail = strings.TrimSpace(r.text[end:r.end(body.GetStop())])
	p.Modifiers = p.Tail
	return p
}

// A simple expression is a single path down the grammar, not a textual prefix.
func expressionLeaf(tree antlr4.Tree) antlr4.Tree {
	for tree != nil && tree.GetChildCount() == 1 {
		tree = tree.GetChild(0)
	}
	return tree
}
func syntaxIdentifier(tree grammar.IExpressionContext) string {
	symbol, ok := syntaxOnly[grammar.ISymbolContext](tree)
	if !ok {
		return ""
	}
	return unquoteSyntaxName(symbol.GetText())
}

func (q *QueryStructure) First(kind string) *QueryClause {
	for i := range q.Clauses {
		if q.Clauses[i].Kind == kind {
			return &q.Clauses[i]
		}
	}
	return nil
}
func (q *QueryStructure) Last(kind string) *QueryClause {
	for i := len(q.Clauses) - 1; i >= 0; i-- {
		if q.Clauses[i].Kind == kind {
			return &q.Clauses[i]
		}
	}
	return nil
}
func (q *QueryStructure) Starts(kind string) bool {
	return q.Err == nil && len(q.Clauses) > 0 && q.Clauses[0].Kind == kind
}
func (q *QueryStructure) Writes() bool {
	for _, c := range q.Clauses {
		switch c.Kind {
		case "CREATE", "MERGE", "SET", "REMOVE", "DELETE", "DETACH DELETE", "FOREACH", "SCHEMA", "TERMINATE":
			return true
		}
		if c.Nested != nil && c.Nested.Writes() {
			return true
		}
	}
	return false
}

// AnyUse visits grammar-owned references until one satisfies matches. Routing
// can decide its target without materializing a list of every nested graph.
func (q *QueryStructure) AnyUse(matches func(UseClause) bool) bool {
	for _, c := range q.Clauses {
		if c.Use != nil && matches(*c.Use) {
			return true
		}
		if c.Nested != nil && c.Nested.AnyUse(matches) {
			return true
		}
	}
	return false
}

func (q *QueryStructure) Uses() []UseClause {
	var uses []UseClause
	q.AnyUse(func(use UseClause) bool {
		uses = append(uses, use)
		return false
	})
	return uses
}
func (q *QueryStructure) ImportNames() []string {
	if !q.Starts("WITH") {
		return nil
	}
	p := q.Clauses[0].Projection
	if p == nil || p.Star {
		return nil
	}
	var names []string
	for _, item := range p.Items {
		if item.Name == "" {
			return nil
		}
		names = append(names, item.Name)
	}
	return names
}

func StripLeadingImportsForRecord(text string, bindings map[string]interface{}) string {
	q := ReadQueryStructure(text)
	if !q.Starts("WITH") || len(q.Clauses) < 2 || len(bindings) == 0 {
		return text
	}
	switch q.Clauses[1].Kind {
	case "MATCH", "OPTIONAL MATCH", "RETURN", "UNWIND", "USE":
	default:
		return text
	}
	names := q.ImportNames()
	if len(names) == 0 {
		return text
	}
	p := q.Clauses[0].Projection
	if p.Distinct || p.Modifiers != "" || q.Clauses[0].Predicate != "" {
		return text
	}
	for i, name := range names {
		if _, ok := bindings[name]; !ok || p.Items[i].HasAlias {
			return text
		}
	}
	// The local execution frame carries these names as records. Replacing
	// them with parameters would merge two independent binding namespaces.
	return q.Clauses[0].Remaining
}

func (q *QueryStructure) keywordAt(pos int, kind string) bool {
	for _, c := range q.Clauses {
		if c.Start == pos && (c.Kind == kind || kind == "MATCH" && c.Kind == "OPTIONAL MATCH") {
			return true
		}
		if c.Nested != nil && c.Nested.keywordAt(pos, kind) {
			return true
		}
		if p := c.Projection; p != nil {
			switch kind {
			case "ORDER BY":
				if p.OrderStart == pos {
					return true
				}
			case "SKIP":
				if p.SkipStart == pos {
					return true
				}
			case "LIMIT":
				if p.LimitStart == pos {
					return true
				}
			}
		}
	}
	return false
}

func readProjection(text string) *QueryProjection {
	q := ReadQueryStructure("RETURN " + text)
	if q.Err != nil || len(q.Clauses) != 1 || !q.Starts("RETURN") {
		return nil
	}
	return q.Clauses[0].Projection
}

func readExpression(text string) (*structureReader, grammar.IExpressionContext) {
	r := &structureReader{text: text, shift: len("RETURN ")}
	for i := range text {
		r.bytes = append(r.bytes, i)
	}
	r.bytes = append(r.bytes, len(text))
	p := readProjection(text)
	if p == nil || p.Star || p.Distinct || p.Modifiers != "" || len(p.Items) != 1 || p.Items[0].HasAlias {
		return r, nil
	}
	return r, p.Items[0].tree
}

func syntaxProperty(tree grammar.IExpressionContext) (string, string, bool) {
	var current antlr4.Tree = tree
	for current != nil {
		if prop, ok := current.(grammar.IPropertyExpressionContext); ok {
			if len(prop.AllName()) != 1 || prop.Atom() == nil || prop.Atom().Symbol() == nil {
				return "", "", false
			}
			return unquoteSyntaxName(prop.Atom().Symbol().GetText()), unquoteSyntaxName(prop.Name(0).GetText()), true
		}
		if current.GetChildCount() != 1 {
			break
		}
		current = current.GetChild(0)
	}
	return "", "", false
}

func syntaxExpressionKey(tree grammar.IExpressionContext) string {
	if name := syntaxIdentifier(tree); name != "" {
		return "\x00variable:" + name
	}
	if v, p, ok := syntaxProperty(tree); ok {
		return fmt.Sprintf("\x00property:%d:%s%s", len(v), v, p)
	}
	return ""
}

func syntaxFunction(tree grammar.IExpressionContext, name string) grammar.IFunctionInvocationContext {
	var current antlr4.Tree = tree
	for current != nil {
		if fn, ok := current.(grammar.IFunctionInvocationContext); ok {
			if fn.InvocationName() != nil && strings.EqualFold(fn.InvocationName().GetText(), name) {
				return fn
			}
			return nil
		}
		if current.GetChildCount() != 1 {
			break
		}
		current = current.GetChild(0)
	}
	return nil
}

func syntaxOnly[T any](tree antlr4.Tree) (T, bool) {
	for tree != nil {
		if found, ok := tree.(T); ok {
			return found, true
		}
		if tree.GetChildCount() != 1 {
			break
		}
		tree = tree.GetChild(0)
	}
	var zero T
	return zero, false
}

func syntaxSimpleCount(text string) bool {
	_, tree := readExpression(text)
	if tree == nil {
		return false
	}
	if _, ok := syntaxOnly[grammar.ICountAllContext](tree); ok {
		return true
	}
	fn := syntaxFunction(tree, "count")
	if fn == nil || fn.DISTINCT() != nil || fn.ExpressionChain() == nil {
		return false
	}
	args := fn.ExpressionChain().AllExpression()
	if len(args) != 1 {
		return false
	}
	if syntaxIdentifier(args[0]) != "" {
		return true
	}
	_, _, ok := syntaxProperty(args[0])
	return ok
}

func syntaxCollect(text string) (string, bool) {
	r, tree := readExpression(text)
	fn := syntaxFunction(tree, "collect")
	if fn == nil || fn.ExpressionChain() == nil || len(fn.ExpressionChain().AllExpression()) != 1 {
		return "", false
	}
	inner := r.source(fn.ExpressionChain())
	if fn.DISTINCT() != nil {
		inner = "DISTINCT " + inner
	}
	return inner, true
}

func syntaxAndTerms(text string) []string {
	r, tree := readExpression(text)
	and, ok := syntaxOnly[grammar.IAndExpressionContext](tree)
	if !ok {
		return []string{text}
	}
	var terms []string
	for _, term := range and.AllNotExpression() {
		terms = append(terms, r.source(term))
	}
	return terms
}

func syntaxEquality(text string) (string, string, bool) {
	r, tree := readExpression(text)
	eq, ok := syntaxOnly[grammar.IComparisonExpressionContext](tree)
	if !ok || len(eq.AllAddSubExpression()) != 2 || len(eq.AllComparisonSigns()) != 1 || eq.ComparisonSigns(0).ASSIGN() == nil {
		return "", "", false
	}
	return r.source(eq.AddSubExpression(0)), r.source(eq.AddSubExpression(1)), true
}

func syntaxNotNullGuard(text, name string) bool {
	_, tree := readExpression(text)
	atom, ok := syntaxOnly[grammar.IAtomicExpressionContext](tree)
	if !ok || len(atom.AllNullExpression()) != 1 || atom.NullExpression(0).NOT() == nil || atom.GetChildCount() != 2 {
		return false
	}
	prop, ok := syntaxOnly[grammar.IPropertyExpressionContext](atom.PropertyOrLabelExpression())
	return ok && len(prop.AllName()) == 0 && prop.Atom().Symbol() != nil && unquoteSyntaxName(prop.Atom().Symbol().GetText()) == name
}

func walkSyntax(tree antlr4.Tree, visit func(antlr4.Tree)) {
	if tree == nil {
		return
	}
	visit(tree)
	for i := 0; i < tree.GetChildCount(); i++ {
		walkSyntax(tree.GetChild(i), visit)
	}
}

func walkSyntaxParents(tree, parent antlr4.Tree, visit func(antlr4.Tree, antlr4.Tree)) {
	if tree == nil {
		return
	}
	visit(tree, parent)
	for i := 0; i < tree.GetChildCount(); i++ {
		walkSyntaxParents(tree.GetChild(i), tree, visit)
	}
}

func syntaxVariable(tree, parent antlr4.Tree) (grammar.ISymbolContext, bool) {
	symbol, ok := tree.(grammar.ISymbolContext)
	if !ok {
		return nil, false
	}
	return symbol, symbolRole(symbol, parent).expression
}

// BranchDeclaresVariable distinguishes a binding from a read by its grammar
// parent. A quoted name has exactly the same role as an unquoted name.
func BranchDeclaresVariable(text, name string) bool {
	return BranchBindings(text)[name]
}

// BranchBindings supplies the first query binding or expression reference of
// every name from the native grammar. True denotes a branch-local declaration;
// false denotes a read from its incoming frame. CALL compilation and execution
// consume these same facts instead of rescanning names in source text.
func BranchBindings(text string) map[string]bool {
	q := ReadQueryStructure(text)
	if q.Err != nil {
		return nil
	}
	bindings := make(map[string]bool)
	for _, clause := range q.Clauses {
		walkSyntaxParents(clause.tree, nil, func(node, parent antlr4.Tree) {
			symbol, ok := node.(grammar.ISymbolContext)
			if !ok {
				return
			}
			name := unquoteSyntaxName(symbol.GetText())
			if _, found := bindings[name]; found {
				return
			}
			role := symbolRole(symbol, parent)
			if role.queryBinding || role.expression {
				bindings[name] = role.queryBinding
			}
		})
	}
	return bindings
}
func syntaxHasVariable(text, name string) bool {
	_, tree := readExpression(text)
	found := false
	walkSyntaxParents(tree, nil, func(node, parent antlr4.Tree) {
		if symbol, ok := syntaxVariable(node, parent); ok && unquoteSyntaxName(symbol.GetText()) == name {
			found = true
		}
	})
	return found
}

// RewriteQueryVariable edits only grammar-owned variable references. Property
// keys, function/graph names, parameters, literals and comments are untouched.
func RewriteQueryVariable(text, name, replacement string) string {
	q := ReadQueryStructure(text)
	positions := &structureReader{text: text}
	for i := range text {
		positions.bytes = append(positions.bytes, i)
	}
	positions.bytes = append(positions.bytes, len(text))
	var tree antlr4.Tree = q.tree
	if q.Err != nil {
		reader, expression := readExpression(text)
		if expression == nil {
			return text
		}
		positions = reader
		tree = expression
	}
	var spans [][2]int
	walkSyntaxParents(tree, nil, func(node, parent antlr4.Tree) {
		if symbol, ok := syntaxVariable(node, parent); ok && unquoteSyntaxName(symbol.GetText()) == name {
			spans = append(spans, [2]int{positions.start(symbol.GetStart()), positions.end(symbol.GetStop())})
		}
	})
	if len(spans) == 0 {
		return text
	}
	var out strings.Builder
	start := 0
	for _, span := range spans {
		if span[0] < start {
			continue
		}
		out.WriteString(text[start:span[0]])
		out.WriteString(replacement)
		start = span[1]
	}
	out.WriteString(text[start:])
	return out.String()
}

func syntaxSimpleModifiers(p *QueryProjection) bool {
	for _, order := range p.Order {
		_, tree := readExpression(order.Expression)
		name := syntaxIdentifier(tree)
		v, property, isProperty := syntaxProperty(tree)
		found := false
		for _, item := range p.Items {
			if name != "" && (item.Alias == name || item.Name == name) {
				found = true
			}
			if isProperty {
				iv, ip, ok := syntaxProperty(item.tree)
				if ok && iv == v && ip == property {
					found = true
				}
			}
		}
		if !found {
			return false
		}
	}
	if p.Skip != "" {
		if _, ok := parseSimplePositiveInt(p.Skip); !ok {
			return false
		}
	}
	if p.Limit != "" {
		if _, ok := parseSimplePositiveInt(p.Limit); !ok {
			return false
		}
	}
	return true
}

func simpleClauseProjection(c QueryClause, count int) *QueryProjection {
	p := c.Projection
	if p == nil || p.Star || p.Distinct || p.Modifiers != "" || c.Predicate != "" || len(p.Items) != count {
		return nil
	}
	return p
}

func syntaxMapCollect(item QueryProjectionItem) string {
	fn := syntaxFunction(item.tree, "collect")
	if fn == nil || fn.DISTINCT() != nil || fn.ExpressionChain() == nil || len(fn.ExpressionChain().AllExpression()) != 1 {
		return ""
	}
	m, ok := syntaxOnly[grammar.IMapLitContext](fn.ExpressionChain().Expression(0))
	if !ok {
		return ""
	}
	var pairs []string
	for _, pair := range m.AllMapPair() {
		pairs = append(pairs, pair.GetText())
	}
	return strings.Join(pairs, ",")
}

func readCollectMapProjection(text string) []string {
	q := ReadQueryStructure(text)
	if q.Err != nil || !q.Starts("WITH") || len(q.Clauses) > 2 {
		return nil
	}
	p := simpleClauseProjection(q.Clauses[0], 1)
	if p == nil || p.Items[0].Alias == "" {
		return nil
	}
	m := syntaxMapCollect(p.Items[0])
	if m == "" {
		return nil
	}
	ret := ""
	if len(q.Clauses) == 2 {
		p := simpleClauseProjection(q.Clauses[1], 1)
		if q.Clauses[1].Kind != "RETURN" || p == nil || p.Items[0].HasAlias || p.Items[0].Name == "" {
			return nil
		}
		ret = p.Items[0].Name
	}
	return []string{"", m, q.Clauses[0].Projection.Items[0].Alias, ret}
}

func readCollectDistinctProjection(text string) []string {
	q := ReadQueryStructure(text)
	if q.Err != nil || len(q.Clauses) != 4 || !q.Starts("WITH") || q.Clauses[1].Kind != "UNWIND" || q.Clauses[2].Kind != "WITH" || q.Clauses[3].Kind != "RETURN" {
		return nil
	}
	p := simpleClauseProjection(q.Clauses[0], 1)
	last := simpleClauseProjection(q.Clauses[3], 1)
	keys := simpleClauseProjection(q.Clauses[2], 1)
	if p == nil || last == nil || keys == nil || p.Items[0].Alias == "" || keys.Items[0].Alias == "" || last.Items[0].Name == "" || last.Items[0].HasAlias {
		return nil
	}
	m := syntaxMapCollect(p.Items[0])
	fn := syntaxFunction(keys.Items[0].tree, "collect")
	if m == "" || fn == nil || fn.DISTINCT() == nil || fn.ExpressionChain() == nil || len(fn.ExpressionChain().AllExpression()) != 1 {
		return nil
	}
	v, k, ok := syntaxProperty(fn.ExpressionChain().Expression(0))
	_, list := readExpression(q.Clauses[1].Expression)
	name := syntaxIdentifier(list)
	if !ok || name == "" {
		return nil
	}
	return []string{"", m, p.Items[0].Alias, name, q.Clauses[1].Name, v, k, keys.Items[0].Alias, last.Items[0].Name}
}

func readCollectJoinProjection(text string) []string {
	q := ReadQueryStructure(text)
	if q.Err != nil || len(q.Clauses) != 5 || !q.Starts("WITH") || q.Clauses[1].Kind != "UNWIND" || q.Clauses[2].Kind != "WITH" || q.Clauses[3].Kind != "UNWIND" || q.Clauses[4].Kind != "RETURN" {
		return nil
	}
	p := simpleClauseProjection(q.Clauses[0], 2)
	join := simpleClauseProjection(q.Clauses[2], 2)
	ret := q.Clauses[4].Projection
	if p == nil || join == nil || ret == nil || ret.Distinct || ret.Star || ret.Modifiers != "" || p.Items[0].Name == "" || p.Items[1].Alias == "" || join.Items[0].Name == "" || join.Items[1].Alias == "" {
		return nil
	}
	m := syntaxMapCollect(p.Items[1])
	list, ok := syntaxOnly[grammar.IListComprehensionContext](join.Items[1].tree)
	if m == "" || !ok || list.Expression() != nil {
		return nil
	}
	filter := list.FilterExpression()
	if filter == nil || filter.Where() == nil {
		return nil
	}
	group := syntaxIdentifier(filter.Expression())
	r := newStructureReader(text)
	lhs, rhs, ok := syntaxEquality(r.source(filter.Where().Expression()))
	if !ok {
		return nil
	}
	v1, k1, ok1 := parseFabricVarProp(lhs)
	v2, k2, ok2 := parseFabricVarProp(rhs)
	if !ok1 || !ok2 {
		return nil
	}
	_, u1 := readExpression(q.Clauses[1].Expression)
	_, u2 := readExpression(q.Clauses[3].Expression)
	return []string{"", p.Items[0].Name, m, p.Items[1].Alias, syntaxIdentifier(u1), q.Clauses[1].Name, join.Items[0].Name, unquoteSyntaxName(filter.Symbol().GetText()), group, v1, k1, v2, k2, join.Items[1].Alias, syntaxIdentifier(u2), q.Clauses[3].Name, ret.Text}
}

type syntaxMapField struct{ key, col string }

func readMapFields(text string) []syntaxMapField {
	_, tree := readExpression("{" + text + "}")
	m, ok := syntaxOnly[grammar.IMapLitContext](tree)
	if !ok {
		return nil
	}
	var fields []syntaxMapField
	for _, pair := range m.AllMapPair() {
		name := syntaxIdentifier(pair.Expression())
		if name == "" {
			return nil
		}
		fields = append(fields, syntaxMapField{key: unquoteSyntaxName(pair.Name().GetText()), col: name})
	}
	return fields
}

func sameSyntax(a, b antlr4.Tree) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.GetChildCount() != b.GetChildCount() {
		return false
	}
	if ar, ok := a.(antlr4.ParserRuleContext); ok {
		br, ok := b.(antlr4.ParserRuleContext)
		if !ok || ar.GetRuleIndex() != br.GetRuleIndex() {
			return false
		}
		if _, ok := a.(grammar.ISymbolContext); ok {
			return ar.GetText() == br.GetText()
		}
	}
	if at, ok := a.(antlr4.TerminalNode); ok {
		bt, ok := b.(antlr4.TerminalNode)
		if !ok || at.GetSymbol().GetTokenType() != bt.GetSymbol().GetTokenType() {
			return false
		}
		kind := at.GetSymbol().GetTokenType()
		if kind == grammar.CypherParserSTRING_LITERAL || kind == grammar.CypherParserCHAR_LITERAL {
			return at.GetText() == bt.GetText()
		}
		return strings.EqualFold(at.GetText(), bt.GetText())
	}
	for i := 0; i < a.GetChildCount(); i++ {
		if !sameSyntax(a.GetChild(i), b.GetChild(i)) {
			return false
		}
	}
	return true
}

func isRowsGroupedJoin(text string) bool {
	const shape = "WITH rows, collect({k: k, texts: texts}) AS grouped UNWIND rows AS r WITH r, [g IN grouped WHERE g.k = r.textKey128][0] AS hit RETURN r.textKey AS textKey, r.textKey128 AS textKey128, coalesce(hit.texts, []) AS texts"
	a, b := ReadQueryStructure(text), ReadQueryStructure(shape)
	if a.Err != nil || b.Err != nil || len(a.Clauses) != len(b.Clauses) {
		return false
	}
	for i := range a.Clauses {
		if a.Clauses[i].Kind != b.Clauses[i].Kind || !sameSyntax(a.Clauses[i].tree, b.Clauses[i].tree) {
			return false
		}
	}
	return true
}

// ProjectionSyntax accepts a projection body at its grammar entry point.
func ProjectionSyntax(text string) *QueryProjection               { return readProjection(text) }
func ReturnColumns(text string) []string                          { return inferReturnColumnsFromQuery(text) }
func SplitQueryUnion(text string) ([]string, []bool, bool, error) { return splitTopLevelUnion(text) }
func ExpressionAndTerms(text string) []string                     { return syntaxAndTerms(text) }
func ExpressionEquality(text string) (string, string, bool)       { return syntaxEquality(text) }
func ExpressionProperty(text string) (string, string, bool)       { return parseFabricVarProp(text) }
func ExpressionNotNullGuard(text, name string) bool               { return syntaxNotNullGuard(text, name) }
func ExpressionHasVariable(text, name string) bool                { return syntaxHasVariable(text, name) }

func ProjectionBodyAndTail(text, kind string) (string, string, bool) {
	q := ReadQueryStructure(text)
	if !q.Starts(kind) || q.Clauses[0].Projection == nil {
		return "", "", false
	}
	p := q.Clauses[0].Projection
	return p.Text, p.Tail, true
}

func ClauseBody(text, kind string) (string, bool) {
	q := ReadQueryStructure(text)
	if q.Err != nil || len(q.Clauses) == 0 {
		return "", false
	}
	c := q.Clauses[0]
	if c.Kind != kind && !(kind == "DELETE" && c.Kind == "DETACH DELETE") {
		return "", false
	}
	return c.Body, true
}

func ResultModifierSyntax(text string) *QueryProjection {
	q := ReadQueryStructure(text)
	if q.Err == nil {
		for i := len(q.Clauses) - 1; i >= 0; i-- {
			if q.Clauses[i].Projection != nil {
				return q.Clauses[i].Projection
			}
		}
	}
	tail := ReadQueryStructure("WITH __fabric_modifier_input " + text)
	if tail.Err == nil && len(tail.Clauses) == 1 {
		return tail.Clauses[0].Projection
	}
	return readProjection(text)
}

func OrderItemsSyntax(text string) []QueryOrderItem {
	q := ReadQueryStructure("WITH __fabric_order_input ORDER BY " + text)
	if q.Err != nil || len(q.Clauses) != 1 || q.Clauses[0].Projection == nil {
		return nil
	}
	return q.Clauses[0].Projection.Order
}

func ExpressionNullGuard(text string) (string, bool, bool) {
	_, tree := readExpression(text)
	for {
		paren, ok := syntaxOnly[grammar.IParenthesizedExpressionContext](tree)
		if !ok {
			break
		}
		tree = paren.Expression()
	}
	atom, ok := syntaxOnly[grammar.IAtomicExpressionContext](tree)
	if !ok || len(atom.AllNullExpression()) != 1 || atom.GetChildCount() != 2 {
		return "", false, false
	}
	prop, ok := syntaxOnly[grammar.IPropertyExpressionContext](atom.PropertyOrLabelExpression())
	if !ok || len(prop.AllName()) != 0 || prop.Atom().Symbol() == nil {
		return "", false, false
	}
	return unquoteSyntaxName(prop.Atom().Symbol().GetText()), atom.NullExpression(0).NOT() != nil, true
}

func LeadingWhereAndTail(text string) (string, string, bool) {
	q := ReadQueryStructure(text)
	if !q.Starts("WITH") {
		q = ReadQueryStructure("WITH * " + text)
	}
	if !q.Starts("WITH") || len(q.Clauses) < 2 || q.Clauses[0].Predicate == "" {
		return "", "", false
	}
	return q.Clauses[0].Predicate, q.Clauses[0].Remaining, true
}

// LocalQuery lowers the grammar-owned syntax to the local execution parser.
// Keyword-spelled names are quoted by their Symbol rule, never by spelling or
// preceding-token guesses. Trivia between grammar keywords is rendered once
// here, so the execution parser receives complete operators even when general
// query normalization is disabled. The planner always reads the original text.
func LocalQuery(text string) string {
	q := ReadQueryStructure(text)
	if q.Err != nil {
		return text
	}
	return q.LocalText
}

// StripEmbeddingOption consumes only a grammar-owned option on an updating
// clause. A projection of a variable named EMBEDDING is a different production.
func StripEmbeddingOption(text string) (string, bool) {
	q := ReadQueryStructure(text)
	if q.Err == nil {
		for _, clause := range q.Clauses {
			if clause.embeddingEnd > clause.embeddingStart {
				before, after := strings.TrimSpace(text[:clause.embeddingStart]), strings.TrimSpace(text[clause.embeddingEnd:])
				return strings.TrimSpace(before + " " + after), true
			}
		}
	}
	return text, false
}

func ReadOnlyQueryPrefix(text string) string {
	q := ReadQueryStructure(text)
	if q.Err != nil || q.Last("RETURN") == nil {
		return ""
	}
	var clauses []string
	started := false
	for _, c := range q.Clauses {
		if c.Kind == "MATCH" {
			started = true
		}
		if !started {
			continue
		}
		switch c.Kind {
		case "SET", "CREATE":
			return strings.Join(clauses, " ") + " RETURN 1"
		case "DELETE", "DETACH DELETE", "MERGE", "REMOVE", "FOREACH", "RETURN":
			return ""
		}
		clauses = append(clauses, c.Text)
	}
	return ""
}

func QueryWithBatchWindow(text string, limit, skip int) string {
	q := ReadQueryStructure(text)
	if q.Err != nil {
		return text
	}
	window := fmt.Sprintf(" LIMIT %d", limit)
	if skip > 0 {
		window = fmt.Sprintf(" SKIP %d LIMIT %d", skip, limit)
	}
	matchIndex := -1
	var parts []string
	for i, c := range q.Clauses {
		if c.Kind == "MATCH" && matchIndex < 0 {
			matchIndex = i
		}
		if matchIndex < 0 && c.Kind == "RETURN" && c.Projection != nil && c.Projection.Skip == "" && c.Projection.Limit == "" {
			parts = append(parts, strings.TrimSpace(window))
			for _, tail := range q.Clauses[i:] {
				parts = append(parts, tail.Text)
			}
			return strings.Join(parts, " ")
		}
		if matchIndex >= 0 && i > matchIndex {
			switch c.Kind {
			case "SET", "CREATE", "DELETE", "DETACH DELETE", "MERGE", "RETURN":
				name := "n"
				walkSyntax(q.Clauses[matchIndex].tree, func(node antlr4.Tree) {
					if n, ok := node.(grammar.INodePatternContext); ok && n.Symbol() != nil && name == "n" {
						name = n.Symbol().GetText()
					}
				})
				parts = append(parts, "WITH "+name+window)
				for _, tail := range q.Clauses[i:] {
					parts = append(parts, tail.Text)
				}
				return strings.Join(parts, " ")
			}
		}
		parts = append(parts, c.Text)
	}
	return strings.Join(parts, " ") + window
}

func (r *structureReader) lower(q *QueryStructure) string {
	type edit struct {
		start, end int
		text       string
	}
	candidateNames := q.EmbeddingCandidateNames()
	var names []edit
	keywords := make(map[int]bool)
	var syntaxEdits []edit
	var visit func(antlr4.Tree, bool, bool, bool)
	visit = func(tree antlr4.Tree, inName, inParameter, inInvocation bool) {
		if tree == nil {
			return
		}
		switch node := tree.(type) {
		case grammar.IFabricInsertStContext:
			token := node.INSERT().GetSymbol()
			syntaxEdits = append(syntaxEdits, edit{r.start(token), r.end(token), "CREATE"})
		case grammar.IFabricNoDetachDeleteStContext:
			token := node.NODETACH().GetSymbol()
			syntaxEdits = append(syntaxEdits, edit{r.start(token), r.end(token), ""})
		case grammar.IFabricOffsetStContext:
			token := node.OFFSET().GetSymbol()
			syntaxEdits = append(syntaxEdits, edit{r.start(token), r.end(token), "SKIP"})
		}
		if _, ok := tree.(grammar.IParameterContext); ok {
			inParameter = true
		}
		if _, ok := tree.(grammar.IInvocationNameContext); ok {
			inInvocation = true
		}
		if symbol, ok := tree.(grammar.ISymbolContext); ok {
			inName = true
			kind := symbol.GetStart().GetTokenType()
			if !candidateNames[symbol.GetStart().GetStart()] && !inParameter && !inInvocation && kind != grammar.CypherParserID && kind != grammar.CypherParserESC_LITERAL {
				names = append(names, edit{r.start(symbol.GetStart()), r.end(symbol.GetStop()), "`" + strings.ReplaceAll(unquoteSyntaxName(symbol.GetText()), "`", "``") + "`"})
			}
		}
		if token, ok := tree.(antlr4.TerminalNode); ok && !inName && !inParameter {
			keywords[r.start(token.GetSymbol())] = true
		}
		for i := 0; i < tree.GetChildCount(); i++ {
			visit(tree.GetChild(i), inName, inParameter, inInvocation)
		}
	}
	visit(q.tree, false, false, false)
	names = append(names, syntaxEdits...)
	sort.Slice(names, func(i, j int) bool { return names[i].start < names[j].start })
	r.stream.Fill()
	var out strings.Builder
	previousEnd, previousStart, nameIndex := 0, -1, 0
	for _, token := range r.stream.GetAllTokens() {
		if token.GetTokenType() == antlr4.TokenEOF || token.GetChannel() != antlr4.TokenDefaultChannel {
			continue
		}
		start, end := r.start(token), r.end(token)
		if start < previousEnd {
			continue
		}
		gap := r.text[previousEnd:start]
		if len(gap) > 0 && (keywords[previousStart] && keywords[start] || strings.Contains(gap, "/*") || strings.Contains(gap, "//")) {
			gap = " "
		}
		if gap != r.text[previousEnd:start] {
			q.LocalEdits = append(q.LocalEdits, QueryTextEdit{previousEnd, start, gap})
		}
		out.WriteString(gap)
		if nameIndex < len(names) && names[nameIndex].start == start {
			out.WriteString(names[nameIndex].text)
			end = names[nameIndex].end
			if names[nameIndex].text != r.text[start:end] {
				q.LocalEdits = append(q.LocalEdits, QueryTextEdit{start, end, names[nameIndex].text})
			}
			nameIndex++
		} else {
			out.WriteString(r.text[start:end])
		}
		previousEnd, previousStart = end, start
	}
	if previousEnd < len(r.text) {
		q.LocalEdits = append(q.LocalEdits, QueryTextEdit{previousEnd, len(r.text), ""})
	}
	return out.String()
}

// EmbeddingCandidateNames exposes grammar-owned candidate spans, without
// predicting any variable scope. Callers keep them unresolved until execution.
func (q *QueryStructure) EmbeddingCandidateNames() map[int]bool {
	var names map[int]bool
	var visit func(*QueryStructure)
	visit = func(body *QueryStructure) {
		for _, c := range body.Clauses {
			if c.EmbeddingCandidate != "" && c.Projection != nil {
				item := c.Projection.Items[0]
				if names == nil {
					names = make(map[int]bool)
				}
				names[item.tree.GetStart().GetStart()] = true
			}
			if c.Nested != nil {
				visit(c.Nested)
			}
		}
	}
	visit(q)
	return names
}

func (q *QueryStructure) HasEmbeddingCandidate() bool {
	for _, clause := range q.Clauses {
		if clause.EmbeddingCandidate != "" || clause.Nested != nil && clause.Nested.HasEmbeddingCandidate() {
			return true
		}
	}
	return false
}
