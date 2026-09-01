// Lucene-classic fulltext query parser used by
// db.index.fulltext.queryNodes / db.index.fulltext.queryRelationships.
//
// The Neo4j fulltext procedures accept the Lucene classic query grammar
// (see org.neo4j.kernel.api.impl.fulltext.FulltextIndexReader — a
// MultiFieldQueryParser with setAllowLeadingWildcard(true)). Prior to
// this parser NornicDB tokenized the query string with strings.Fields
// and had no notion of parenthesized groups, field-scoped clauses, or
// escape sequences. That meant any composite query — most notably the
// Graphiti shape `group_id:"<g>" AND (<terms>)` — silently discarded
// the parenthesized default-field clause, making the lexical arm of
// hybrid search term-blind. This file replaces that ad-hoc tokenizer
// with a proper recursive-descent parser plus a per-document evaluator
// that intersects/unions clause matches as the boolean operators dictate.
//
// Grammar (Lucene classic subset that the reference parser accepts):
//
//	Query     := OrExpr
//	OrExpr    := AndExpr ( ("OR"|"||") AndExpr )*
//	AndExpr   := NotExpr ( ("AND"|"&&") NotExpr )*
//	NotExpr   := "NOT" NotExpr | Clause
//	Clause    := ("+"|"-")? ( FieldExpr | Group | Atom ) ("^" number)?
//	Group     := "(" OrExpr ")"
//	FieldExpr := TERM ":" ( Group | Range | Atom )
//	Range     := ("["|"{") RangeTerm "TO" RangeTerm ("]"|"}")
//	Atom      := QUOTED ("~" digits?)?      // proximity when after phrase
//	           | TERM ("*"|"?")*             // wildcards
//	           | TERM "~" digits?            // fuzzy
//	           | REGEX
//
// Escape rule: `\X` in a term or phrase decodes to a literal X for any
// character X (matches Lucene's QueryParserBase.discardEscapeChar).
package cypher

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

type ftTokenKind int

const (
	tkEOF ftTokenKind = iota
	tkTerm
	tkQuoted
	tkRegex
	tkLParen
	tkRParen
	tkLBrack // [
	tkRBrack // ]
	tkLBrace // {
	tkRBrace // }
	tkColon
	tkPlus
	tkMinus
	tkNot // bare NOT keyword
	tkAnd // bare AND / && keyword
	tkOr  // bare OR / || keyword
	tkTo  // bare TO keyword (only meaningful inside ranges)
	tkCaret
	tkTilde
	tkStar
	tkQuestion
)

type ftToken struct {
	kind ftTokenKind
	// text is the decoded literal text for TERM/QUOTED/REGEX and empty for
	// punctuation. Escapes have been resolved.
	text string
	// hadPrecedingSpace tracks whether whitespace came before this token in
	// the source; used by the parser to decide when a TERM has been used up
	// (e.g. `foo :bar` is not a field expression).
	hadPrecedingSpace bool
}

// ftTokenize converts a Lucene classic query string into a token stream.
// It never returns an error — malformed input (unterminated quotes, stray
// escapes) is best-effort: whatever text was seen is emitted as a term or
// phrase and the caller/parser will reject structurally-broken input.
func ftTokenize(input string) []ftToken {
	tokens := make([]ftToken, 0, 16)
	i := 0
	precedingSpace := true

	emit := func(kind ftTokenKind, text string) {
		tokens = append(tokens, ftToken{kind: kind, text: text, hadPrecedingSpace: precedingSpace})
		precedingSpace = false
	}

	// isTermChar returns true for characters that are allowed unescaped inside
	// a bare term. Lucene forbids the following unescaped: + - ! ( ) { } [ ] ^
	// " ~ * ? : \ / and whitespace. We treat `!` as bare-word (Neo4j does not
	// use it as a keyword). Wildcards ? and * are handled as their own tokens
	// only when they appear outside a term.
	isTermChar := func(r rune) bool {
		if unicode.IsSpace(r) {
			return false
		}
		switch r {
		case '+', '-', '(', ')', '{', '}', '[', ']', '^', '"', '~', ':', '\\', '/', '*', '?', '|', '&':
			return false
		}
		return true
	}

	for i < len(input) {
		r := input[i]
		if unicode.IsSpace(rune(r)) {
			precedingSpace = true
			i++
			continue
		}

		switch r {
		case '(':
			emit(tkLParen, "")
			i++
			continue
		case ')':
			emit(tkRParen, "")
			i++
			continue
		case '[':
			emit(tkLBrack, "")
			i++
			continue
		case ']':
			emit(tkRBrack, "")
			i++
			continue
		case '{':
			emit(tkLBrace, "")
			i++
			continue
		case '}':
			emit(tkRBrace, "")
			i++
			continue
		case ':':
			emit(tkColon, "")
			i++
			continue
		case '+':
			emit(tkPlus, "")
			i++
			continue
		case '-':
			emit(tkMinus, "")
			i++
			continue
		case '^':
			emit(tkCaret, "")
			i++
			continue
		case '~':
			emit(tkTilde, "")
			i++
			continue
		case '*':
			emit(tkStar, "")
			i++
			continue
		case '?':
			emit(tkQuestion, "")
			i++
			continue
		case '|':
			if i+1 < len(input) && input[i+1] == '|' {
				emit(tkOr, "||")
				i += 2
				continue
			}
		case '&':
			if i+1 < len(input) && input[i+1] == '&' {
				emit(tkAnd, "&&")
				i += 2
				continue
			}
		case '"':
			// Quoted phrase: scan until the matching unescaped double quote.
			// Escapes inside a phrase decode `\X` to `X` (any X, matches Lucene).
			var b strings.Builder
			i++
			for i < len(input) {
				c := input[i]
				if c == '\\' && i+1 < len(input) {
					b.WriteByte(input[i+1])
					i += 2
					continue
				}
				if c == '"' {
					i++
					break
				}
				b.WriteByte(c)
				i++
			}
			emit(tkQuoted, b.String())
			continue
		case '/':
			// Regex literal: /re/ — scan until the closing /, honoring \/.
			// We only recognize this as a regex when the token position looks
			// like the start of an atom (either the very first token or after
			// whitespace/operator/COLON). Otherwise treat `/` as a plain
			// term character.
			//
			// The Lucene reference treats a bare `/` as a term char in the
			// classic parser; we only take the regex path when it clearly
			// opens a delimited expression.
			if canStartRegex(tokens) {
				var b strings.Builder
				i++
				closed := false
				for i < len(input) {
					c := input[i]
					if c == '\\' && i+1 < len(input) {
						// Preserve the escape so the compiled regex is faithful.
						b.WriteByte('\\')
						b.WriteByte(input[i+1])
						i += 2
						continue
					}
					if c == '/' {
						i++
						closed = true
						break
					}
					b.WriteByte(c)
					i++
				}
				_ = closed
				emit(tkRegex, b.String())
				continue
			}
			// Fallthrough to term-scan below.
		}

		// Bare term: consume run of term chars, decoding `\X` escapes.
		var b strings.Builder
		for i < len(input) {
			c := input[i]
			if c == '\\' && i+1 < len(input) {
				b.WriteByte(input[i+1])
				i += 2
				continue
			}
			if !isTermChar(rune(c)) {
				break
			}
			b.WriteByte(c)
			i++
		}
		if b.Len() == 0 {
			// Unrecognized single char. Skip to avoid an infinite loop.
			i++
			continue
		}
		text := b.String()
		switch text {
		case "AND":
			emit(tkAnd, text)
		case "OR":
			emit(tkOr, text)
		case "NOT":
			emit(tkNot, text)
		case "TO":
			emit(tkTo, text)
		default:
			emit(tkTerm, text)
		}
	}

	emit(tkEOF, "")
	return tokens
}

// canStartRegex reports whether a `/` at the current tokenizer position
// should begin a /re/ literal. Lucene only allows this after operator
// tokens or at the start of input.
func canStartRegex(prev []ftToken) bool {
	if len(prev) == 0 {
		return true
	}
	last := prev[len(prev)-1]
	switch last.kind {
	case tkLParen, tkColon, tkAnd, tkOr, tkNot, tkPlus, tkMinus, tkLBrack, tkLBrace:
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// AST
// ---------------------------------------------------------------------------

type ftNode interface {
	// eval reports whether the doc matches this subtree and the score
	// contribution of the matched leaves (0 when !match). Field-scoped
	// leaves do not contribute to score; only default-field term leaves do,
	// so BM25 ordering stays comparable to the pre-parser behavior for the
	// common bag-of-terms case.
	eval(ctx *ftEvalCtx, doc *ftDoc) (matched bool, score float64)
}

// booleanOccur mirrors Lucene's BooleanClause.Occur.
type booleanOccur int

const (
	occurShould booleanOccur = iota
	occurMust
	occurMustNot
)

type ftBooleanNode struct {
	clauses []ftBooleanClause
	// defaultAnd tells the evaluator that unmarked clauses at this level
	// should behave as MUST (set by ANDs) rather than SHOULD (the Lucene
	// default). This matches how MultiFieldQueryParser rewrites
	// `a AND b` — both clauses become mandatory.
	defaultAnd bool
}

type ftBooleanClause struct {
	occur booleanOccur
	node  ftNode
}

func (n *ftBooleanNode) eval(ctx *ftEvalCtx, doc *ftDoc) (bool, float64) {
	var (
		total       float64
		anyShould   bool
		anyMustNot  bool
		hitShould   bool
		mustCount   int
		positiveHit bool // any MUST or SHOULD matched
	)
	for _, c := range n.clauses {
		occur := c.occur
		if occur == occurShould && n.defaultAnd {
			occur = occurMust
		}
		matched, sc := c.node.eval(ctx, doc)
		switch occur {
		case occurMust:
			mustCount++
			if !matched {
				return false, 0
			}
			positiveHit = true
			total += sc
		case occurMustNot:
			anyMustNot = true
			if matched {
				return false, 0
			}
		case occurShould:
			anyShould = true
			if matched {
				hitShould = true
				positiveHit = true
				total += sc
			}
		}
	}
	if mustCount == 0 && anyShould && !hitShould {
		// Pure SHOULD list requires at least one match (Lucene's
		// minimumNumberShouldMatch = 1 when there are no MUSTs).
		return false, 0
	}
	if mustCount == 0 && !anyShould && anyMustNot {
		// A boolean of only MUST_NOT clauses would match every doc that
		// doesn't hit the prohibitions — Lucene rejects that as
		// "TooManyClauses / no positive clause". We mirror the safer
		// behavior: require at least one positive clause somewhere.
		return false, 0
	}
	_ = positiveHit
	return true, total
}

type ftBoostNode struct {
	inner ftNode
	boost float64
}

func (n *ftBoostNode) eval(ctx *ftEvalCtx, doc *ftDoc) (bool, float64) {
	matched, sc := n.inner.eval(ctx, doc)
	if !matched {
		return false, 0
	}
	return true, sc * n.boost
}

// ftLeafNode is a text-matching leaf. `field` is empty for default-field
// atoms; those match if any of ctx.DefaultFields contains the pattern.
type ftLeafNode struct {
	field   string
	pattern ftPattern
}

func (n *ftLeafNode) eval(ctx *ftEvalCtx, doc *ftDoc) (bool, float64) {
	if n.field != "" {
		return n.pattern.matchField(ctx, doc, n.field)
	}
	// Default-field atom. When ctx.DefaultFields is non-empty (a
	// declared fulltext index) we probe each declared field. When it's
	// empty (legacy/no-index path) we probe the pseudo-field "*" which
	// matches against the doc's concatenated contentLower.
	if len(ctx.DefaultFields) == 0 {
		if hit, _ := n.pattern.matchField(ctx, doc, "*"); hit {
			return true, n.pattern.score(ctx, doc)
		}
		return false, 0
	}
	for _, f := range ctx.DefaultFields {
		if hit, _ := n.pattern.matchField(ctx, doc, f); hit {
			return true, n.pattern.score(ctx, doc)
		}
	}
	return false, 0
}

// ftPattern is the shape-specific matcher for a leaf.
type ftPattern interface {
	matchField(ctx *ftEvalCtx, doc *ftDoc, field string) (bool, float64)
	score(ctx *ftEvalCtx, doc *ftDoc) float64
	// primaryTerm returns the lower-cased term this pattern scores against
	// in the aggregated default-field content, if any. Wildcards/ranges/
	// regexes return "" — they still gate matching but don't feed BM25.
	primaryTerm() string
}

// ---- concrete patterns ----

type patTerm struct{ term string } // exact substring, case-insensitive

func (p patTerm) matchField(ctx *ftEvalCtx, doc *ftDoc, field string) (bool, float64) {
	v := doc.fieldLower(field)
	if v == "" || p.term == "" {
		return false, 0
	}
	return strings.Contains(v, p.term), 0
}
func (p patTerm) score(ctx *ftEvalCtx, doc *ftDoc) float64 {
	return ctx.bm25Term(doc, p.term)
}
func (p patTerm) primaryTerm() string { return p.term }

type patPhrase struct{ phrase string }

func (p patPhrase) matchField(ctx *ftEvalCtx, doc *ftDoc, field string) (bool, float64) {
	v := doc.fieldLower(field)
	if v == "" || p.phrase == "" {
		return false, 0
	}
	return strings.Contains(v, p.phrase), 0
}
func (p patPhrase) score(ctx *ftEvalCtx, doc *ftDoc) float64 {
	// A phrase counts as a single term for scoring; we boost its
	// contribution slightly so exact phrase matches outrank single terms.
	return ctx.bm25Term(doc, p.phrase) + 1.0
}
func (p patPhrase) primaryTerm() string { return p.phrase }

// patPresence implements `field:*`.
type patPresence struct{}

func (p patPresence) matchField(ctx *ftEvalCtx, doc *ftDoc, field string) (bool, float64) {
	if doc.fieldPresent(field) {
		return true, 0
	}
	return false, 0
}
func (p patPresence) score(ctx *ftEvalCtx, doc *ftDoc) float64 { return 1.0 }
func (p patPresence) primaryTerm() string                      { return "" }

// patWildcard matches Lucene wildcards: `*` (any run), `?` (single char).
// The Lucene reference operates on whole tokens; because our value store is
// unstructured strings we approximate token boundaries by whitespace.
type patWildcard struct {
	re *regexp.Regexp
}

func (p patWildcard) matchField(ctx *ftEvalCtx, doc *ftDoc, field string) (bool, float64) {
	v := doc.fieldLower(field)
	if v == "" || p.re == nil {
		return false, 0
	}
	for _, tok := range strings.Fields(v) {
		if p.re.MatchString(tok) {
			return true, 0
		}
	}
	return false, 0
}
func (p patWildcard) score(ctx *ftEvalCtx, doc *ftDoc) float64 { return 1.0 }
func (p patWildcard) primaryTerm() string                      { return "" }

// patRange matches `field:[a TO b]` / `field:{a TO b}` on the string form of
// the property value.
type patRange struct {
	lo, hi         string
	loIncl, hiIncl bool
}

func (p patRange) matchField(ctx *ftEvalCtx, doc *ftDoc, field string) (bool, float64) {
	v, ok := doc.fieldRaw(field)
	if !ok {
		return false, 0
	}
	s := strings.ToLower(v)
	if p.lo != "" && p.lo != "*" {
		if p.loIncl {
			if s < p.lo {
				return false, 0
			}
		} else if s <= p.lo {
			return false, 0
		}
	}
	if p.hi != "" && p.hi != "*" {
		if p.hiIncl {
			if s > p.hi {
				return false, 0
			}
		} else if s >= p.hi {
			return false, 0
		}
	}
	return true, 0
}
func (p patRange) score(ctx *ftEvalCtx, doc *ftDoc) float64 { return 1.0 }
func (p patRange) primaryTerm() string                      { return "" }

// patRegex matches `field:/re/`.
type patRegex struct{ re *regexp.Regexp }

func (p patRegex) matchField(ctx *ftEvalCtx, doc *ftDoc, field string) (bool, float64) {
	if p.re == nil {
		return false, 0
	}
	v := doc.fieldLower(field)
	if v == "" {
		return false, 0
	}
	return p.re.MatchString(v), 0
}
func (p patRegex) score(ctx *ftEvalCtx, doc *ftDoc) float64 { return 1.0 }
func (p patRegex) primaryTerm() string                      { return "" }

// patFuzzy matches `term~n` — max Levenshtein distance across whitespace-
// separated tokens in the field.
type patFuzzy struct {
	term    string
	maxEdit int
}

func (p patFuzzy) matchField(ctx *ftEvalCtx, doc *ftDoc, field string) (bool, float64) {
	if p.term == "" {
		return false, 0
	}
	v := doc.fieldLower(field)
	if v == "" {
		return false, 0
	}
	for _, tok := range strings.Fields(v) {
		if levenshtein(tok, p.term) <= p.maxEdit {
			return true, 0
		}
	}
	return false, 0
}
func (p patFuzzy) score(ctx *ftEvalCtx, doc *ftDoc) float64 {
	return ctx.bm25Term(doc, p.term)
}
func (p patFuzzy) primaryTerm() string { return p.term }

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

// FulltextQuery is a parsed Lucene-classic query, ready for per-document
// evaluation.
type FulltextQuery struct {
	root ftNode
	// primaryTerms is the list of scoring terms extracted for BM25 IDF
	// bookkeeping; it deduplicates lower-cased terms across all leaves.
	primaryTerms []string
}

// ParseFulltextQuery parses input, returning a Neo4j-style parse error for
// structurally malformed queries. It never panics; the tokenizer accepts any
// input and the parser rejects only unbalanced groups or dangling operators.
func ParseFulltextQuery(input string) (*FulltextQuery, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return &FulltextQuery{}, nil
	}
	tokens := ftTokenize(input)
	p := &ftParser{tokens: tokens}
	node, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tkEOF {
		return nil, localizedError(localization.CypherCoreFulltextUnexpectedToken(p.peek().text), nil)
	}
	// If the top-level node is a lone MustNot (e.g. bare `NOT term` or
	// `-term`), wrap it in a boolean node with an implicit MatchAll so
	// the negation has something to negate against. Mirrors Lucene's
	// behavior for pure-negative queries.
	if m, ok := node.(*occurMarker); ok && m.occur == occurMustNot {
		node = &ftBooleanNode{
			clauses: []ftBooleanClause{
				{occur: occurMust, node: &ftLeafNode{pattern: patMatchAll{}}},
				{occur: occurMustNot, node: m.inner},
			},
		}
	}
	q := &FulltextQuery{root: node}
	q.primaryTerms = collectPrimaryTerms(node)
	return q, nil
}

type ftParser struct {
	tokens []ftToken
	pos    int
}

func (p *ftParser) peek() ftToken { return p.tokens[p.pos] }
func (p *ftParser) peekAt(off int) ftToken {
	if p.pos+off >= len(p.tokens) {
		return ftToken{kind: tkEOF}
	}
	return p.tokens[p.pos+off]
}
func (p *ftParser) advance() ftToken {
	t := p.tokens[p.pos]
	if t.kind != tkEOF {
		p.pos++
	}
	return t
}

// parseOr := parseAnd ( OR parseAnd )*
func (p *ftParser) parseOr() (ftNode, error) {
	first, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tkOr {
		return first, nil
	}
	// Collect a SHOULD-list.
	clauses := []ftBooleanClause{{occur: extractOccur(first), node: unwrapOccur(first)}}
	for p.peek().kind == tkOr {
		p.advance()
		next, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, ftBooleanClause{occur: extractOccur(next), node: unwrapOccur(next)})
	}
	return &ftBooleanNode{clauses: clauses, defaultAnd: false}, nil
}

// parseAnd := parseNot ( AND parseNot )*
func (p *ftParser) parseAnd() (ftNode, error) {
	first, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	// A run of implicit-adjacency operands (no AND/OR between them) is also
	// collected here: Lucene classic parses `a b c` as three SHOULD clauses.
	// We split behavior by whether the next connector is AND. If any AND
	// appears, all clauses become MUST (defaultAnd=true).
	if p.peek().kind != tkAnd && !isClauseStart(p.peek().kind) {
		return first, nil
	}

	clauses := []ftBooleanClause{{occur: extractOccur(first), node: unwrapOccur(first)}}
	sawAnd := false
	for {
		if p.peek().kind == tkAnd {
			sawAnd = true
			p.advance()
			next, err := p.parseNot()
			if err != nil {
				return nil, err
			}
			clauses = append(clauses, ftBooleanClause{occur: extractOccur(next), node: unwrapOccur(next)})
			continue
		}
		if isClauseStart(p.peek().kind) {
			next, err := p.parseNot()
			if err != nil {
				return nil, err
			}
			clauses = append(clauses, ftBooleanClause{occur: extractOccur(next), node: unwrapOccur(next)})
			continue
		}
		break
	}
	if len(clauses) == 1 {
		return first, nil
	}
	if sawAnd {
		// Promote any bare SHOULDs to MUST; explicit +/- prefixes stay.
		for i := range clauses {
			if clauses[i].occur == occurShould {
				clauses[i].occur = occurMust
			}
		}
	}
	return &ftBooleanNode{clauses: clauses}, nil
}

// parseNot := NOT parseNot | parseClause
//
// NOT is prefix-only in Lucene; the resulting clause carries a MustNot
// occur so the enclosing boolean node treats it as a prohibition rather
// than a nested SHOULD. We tag the node with an occurMarker(mustNot) so
// parseAnd/parseOr can propagate the occur when it collects the clause.
func (p *ftParser) parseNot() (ftNode, error) {
	if p.peek().kind == tkNot {
		p.advance()
		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		// Flatten repeated NOTs by toggling occur.
		existing := extractOccur(inner)
		var newOccur booleanOccur
		if existing == occurMustNot {
			newOccur = occurMust
		} else {
			newOccur = occurMustNot
		}
		return &occurMarker{occur: newOccur, inner: unwrapOccur(inner)}, nil
	}
	return p.parseClause()
}

// occurMarker wraps a node with an explicit occur to survive the +/- prefix
// through boolean-node assembly.
type occurMarker struct {
	occur booleanOccur
	inner ftNode
}

func (m *occurMarker) eval(ctx *ftEvalCtx, doc *ftDoc) (bool, float64) {
	return m.inner.eval(ctx, doc)
}

func extractOccur(n ftNode) booleanOccur {
	if m, ok := n.(*occurMarker); ok {
		return m.occur
	}
	return occurShould
}
func unwrapOccur(n ftNode) ftNode {
	if m, ok := n.(*occurMarker); ok {
		return m.inner
	}
	return n
}

// parseClause := (+|-)? ( FieldExpr | Group | Atom ) (^ number)?
func (p *ftParser) parseClause() (ftNode, error) {
	occur := occurShould
	if p.peek().kind == tkPlus {
		p.advance()
		occur = occurMust
	} else if p.peek().kind == tkMinus {
		p.advance()
		occur = occurMustNot
	}

	node, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	// Optional boost.
	if p.peek().kind == tkCaret {
		p.advance()
		if p.peek().kind != tkTerm {
			return nil, localizedError(localization.CypherCoreFulltextNumberAfterBoostExpected(), nil)
		}
		nTok := p.advance()
		b, err := strconv.ParseFloat(nTok.text, 64)
		if err != nil {
			return nil, localizedError(localization.CypherCoreFulltextBadBoost(nTok.text, err), err)
		}
		node = &ftBoostNode{inner: node, boost: b}
	}
	if occur == occurShould {
		return node, nil
	}
	return &occurMarker{occur: occur, inner: node}, nil
}

// parsePrimary := FieldExpr | Group | Atom
func (p *ftParser) parsePrimary() (ftNode, error) {
	tk := p.peek()
	switch tk.kind {
	case tkLParen:
		p.advance()
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.peek().kind != tkRParen {
			return nil, localizedError(localization.CypherCoreFulltextClosingParenthesisRequired(), nil)
		}
		p.advance()
		return inner, nil
	case tkTerm:
		// Look ahead for `TERM :` — field expression.
		if p.peekAt(1).kind == tkColon && !p.peekAt(1).hadPrecedingSpace {
			field := p.advance().text
			p.advance() // consume ':'
			return p.parseFieldValue(field)
		}
		return p.parseAtom("")
	case tkQuoted, tkRegex, tkStar, tkQuestion, tkLBrack, tkLBrace:
		return p.parseAtom("")
	}
	return nil, localizedError(localization.CypherCoreFulltextUnexpectedToken(tk.text), nil)
}

// parseFieldValue is the RHS of `field:` — a group, range, atom, or a
// standalone `*` (presence).
func (p *ftParser) parseFieldValue(field string) (ftNode, error) {
	switch p.peek().kind {
	case tkLParen:
		p.advance()
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.peek().kind != tkRParen {
			return nil, localizedError(localization.CypherCoreFulltextClosingParenthesisRequired(), nil)
		}
		p.advance()
		return rebindField(inner, field), nil
	case tkLBrack, tkLBrace:
		return p.parseRange(field)
	case tkStar:
		// `field:*` — presence. But `field:*foo` is a leading-wildcard match,
		// not a presence; distinguish by looking at the following token.
		if p.peekAt(1).kind == tkTerm && !p.peekAt(1).hadPrecedingSpace {
			return p.parseAtom(field)
		}
		p.advance()
		return &ftLeafNode{field: field, pattern: patPresence{}}, nil
	}
	return p.parseAtom(field)
}

// parseRange handles `[a TO b]` / `{a TO b]` / mixed inclusivity.
func (p *ftParser) parseRange(field string) (ftNode, error) {
	openTok := p.advance()
	loIncl := openTok.kind == tkLBrack

	lo, err := p.rangeTerm()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tkTo {
		return nil, localizedError(localization.CypherCoreFulltextRangeTORequired(), nil)
	}
	p.advance()
	hi, err := p.rangeTerm()
	if err != nil {
		return nil, err
	}
	closeTok := p.advance()
	if closeTok.kind != tkRBrack && closeTok.kind != tkRBrace {
		return nil, localizedError(localization.CypherCoreFulltextRangeCloseRequired(), nil)
	}
	hiIncl := closeTok.kind == tkRBrack
	return &ftLeafNode{
		field:   field,
		pattern: patRange{lo: strings.ToLower(lo), hi: strings.ToLower(hi), loIncl: loIncl, hiIncl: hiIncl},
	}, nil
}

func (p *ftParser) rangeTerm() (string, error) {
	switch p.peek().kind {
	case tkTerm:
		return p.advance().text, nil
	case tkQuoted:
		return p.advance().text, nil
	case tkStar:
		p.advance()
		return "*", nil
	}
	return "", localizedError(localization.CypherCoreFulltextRangeEndpointRequired(), nil)
}

// parseAtom parses a single leaf value (term/phrase/wildcard/regex/fuzzy)
// bound to `field` (empty = default field set).
func (p *ftParser) parseAtom(field string) (ftNode, error) {
	tk := p.peek()
	switch tk.kind {
	case tkQuoted:
		p.advance()
		// Optional proximity `~n` after a phrase; we accept but ignore n and
		// fall back to substring matching (Lucene proximity requires token
		// positions, which we don't index).
		if p.peek().kind == tkTilde {
			p.advance()
			if p.peek().kind == tkTerm {
				if _, err := strconv.Atoi(p.peek().text); err == nil {
					p.advance()
				}
			}
		}
		return &ftLeafNode{field: field, pattern: patPhrase{phrase: strings.ToLower(tk.text)}}, nil
	case tkRegex:
		p.advance()
		re, err := regexp.Compile("(?i)" + tk.text)
		if err != nil {
			return nil, localizedError(localization.CypherCoreFulltextBadRegex(tk.text, err), err)
		}
		return &ftLeafNode{field: field, pattern: patRegex{re: re}}, nil
	case tkStar, tkQuestion:
		// Bare wildcard atom (leading wildcard is allowed for Neo4j).
		return p.parseWildcard(field, "")
	case tkTerm:
		p.advance()
		text := tk.text
		// Wildcard run may follow: `foo*`, `foo?bar`, `foo*bar`, etc.
		if p.peek().kind == tkStar || p.peek().kind == tkQuestion || followsWildcardChar(p) {
			return p.parseWildcard(field, text)
		}
		// Fuzzy: `term~` or `term~2`.
		if p.peek().kind == tkTilde {
			p.advance()
			maxEdit := 2
			if p.peek().kind == tkTerm {
				if n, err := strconv.Atoi(p.peek().text); err == nil {
					maxEdit = n
					p.advance()
				}
			}
			return &ftLeafNode{field: field, pattern: patFuzzy{term: strings.ToLower(text), maxEdit: maxEdit}}, nil
		}
		return &ftLeafNode{field: field, pattern: patTerm{term: strings.ToLower(text)}}, nil
	}
	return nil, localizedError(localization.CypherCoreFulltextUnexpectedToken(tk.text), nil)
}

// followsWildcardChar peeks for a wildcard char immediately following the
// current token — used to catch `foo*bar` shapes where the star did not
// receive whitespace on either side. We treat consecutive TERM/wildcard
// tokens with no preceding space as one wildcard pattern.
func followsWildcardChar(p *ftParser) bool {
	// Currently unused because tokenizer already breaks on * / ?; kept as an
	// extension point should the tokenizer merge adjacent tokens.
	return false
}

func (p *ftParser) parseWildcard(field, prefix string) (ftNode, error) {
	var b strings.Builder
	b.WriteString(regexp.QuoteMeta(strings.ToLower(prefix)))
	for {
		tk := p.peek()
		if tk.kind == tkStar {
			p.advance()
			b.WriteString(".*")
			continue
		}
		if tk.kind == tkQuestion {
			p.advance()
			b.WriteByte('.')
			continue
		}
		if tk.kind == tkTerm && !tk.hadPrecedingSpace {
			p.advance()
			b.WriteString(regexp.QuoteMeta(strings.ToLower(tk.text)))
			continue
		}
		break
	}
	re, err := regexp.Compile("(?i)^" + b.String() + "$")
	if err != nil {
		return nil, localizedError(localization.CypherCoreFulltextBadWildcard(err), err)
	}
	return &ftLeafNode{field: field, pattern: patWildcard{re: re}}, nil
}

// isClauseStart reports whether the token can begin a new clause in an
// AND-chain / implicit-adjacency list.
func isClauseStart(k ftTokenKind) bool {
	switch k {
	case tkTerm, tkQuoted, tkRegex, tkLParen, tkPlus, tkMinus, tkNot, tkStar, tkQuestion:
		return true
	}
	return false
}

// rebindField descends into a subtree parsed under `field:( ... )` and
// binds all default-field atoms inside to the given field. Nested field
// expressions inside the group keep their own field.
func rebindField(n ftNode, field string) ftNode {
	switch x := n.(type) {
	case *ftLeafNode:
		if x.field == "" {
			x.field = field
		}
		return x
	case *ftBooleanNode:
		for i := range x.clauses {
			x.clauses[i].node = rebindField(x.clauses[i].node, field)
		}
		return x
	case *ftBoostNode:
		x.inner = rebindField(x.inner, field)
		return x
	case *occurMarker:
		x.inner = rebindField(x.inner, field)
		return x
	}
	return n
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

// ftDoc adapts a single node/edge to the evaluator. `properties` maps
// case-insensitive field names to their stringified values (already
// non-nil-checked). `contentLower` is the concatenation of every default
// field's value used for BM25 scoring.
type ftDoc struct {
	properties     map[string]string // canonicalized: lower-case keys, lower-case values
	rawProperties  map[string]string // lower-case keys, original-case values (for range)
	contentLower   string
	contentTokenN  int
	presenceLookup func(field string) bool
}

func (d *ftDoc) fieldLower(field string) string {
	if d == nil {
		return ""
	}
	if field == "*" {
		return d.contentLower
	}
	return d.properties[strings.ToLower(field)]
}
func (d *ftDoc) fieldRaw(field string) (string, bool) {
	if d == nil {
		return "", false
	}
	v, ok := d.rawProperties[strings.ToLower(field)]
	return v, ok
}
func (d *ftDoc) fieldPresent(field string) bool {
	if d == nil {
		return false
	}
	if d.presenceLookup != nil {
		return d.presenceLookup(field)
	}
	v, ok := d.rawProperties[strings.ToLower(field)]
	return ok && v != ""
}

// ftEvalCtx carries per-query bookkeeping the leaves need to compute BM25.
type ftEvalCtx struct {
	DefaultFields []string
	// avgDocLen is set once by the caller before evaluation.
	avgDocLen float64
	// totalDocs and docFreq feed IDF; docFreq is keyed on lower-case term.
	totalDocs int
	docFreq   map[string]int
}

// bm25Term computes a BM25-like contribution for `term` in doc, mirroring
// the pre-parser `calculateBM25Score` math so scoring stays comparable.
func (ctx *ftEvalCtx) bm25Term(doc *ftDoc, term string) float64 {
	if term == "" || doc == nil {
		return 0
	}
	tf := float64(strings.Count(doc.contentLower, term))
	if tf == 0 {
		return 0
	}
	df := float64(ctx.docFreq[term])
	if df == 0 {
		df = 0.5
	}
	idf := math.Log((float64(ctx.totalDocs) + 1) / df)
	if idf < 0.1 {
		idf = 0.1
	}
	const k1 = 1.2
	const b = 0.75
	avg := ctx.avgDocLen
	if avg <= 0 {
		avg = 100.0
	}
	docLen := float64(doc.contentTokenN)
	tfNorm := (tf * (k1 + 1)) / (tf + k1*(1-b+b*(docLen/avg)))
	return idf * tfNorm
}

// patMatchAll is used internally to satisfy a lone NOT expression.
type patMatchAll struct{}

func (patMatchAll) matchField(*ftEvalCtx, *ftDoc, string) (bool, float64) { return true, 0 }
func (patMatchAll) score(*ftEvalCtx, *ftDoc) float64                      { return 0 }
func (patMatchAll) primaryTerm() string                                   { return "" }

// PrimaryTerms exposes the list of lower-cased scoring terms so callers can
// build docFreq maps before evaluating each doc.
func (q *FulltextQuery) PrimaryTerms() []string {
	if q == nil {
		return nil
	}
	return q.primaryTerms
}

// IsEmpty reports whether the query has no evaluable content (empty input).
func (q *FulltextQuery) IsEmpty() bool { return q == nil || q.root == nil }

// Match returns (matched, score) for a single doc. Callers build the doc
// once per candidate and pass shared bookkeeping via ctx.
func (q *FulltextQuery) Match(ctx *ftEvalCtx, doc *ftDoc) (bool, float64) {
	if q.IsEmpty() {
		return false, 0
	}
	return q.root.eval(ctx, doc)
}

func collectPrimaryTerms(n ftNode) []string {
	seen := map[string]struct{}{}
	var out []string
	var walk func(x ftNode)
	walk = func(x ftNode) {
		switch v := x.(type) {
		case *ftLeafNode:
			t := v.pattern.primaryTerm()
			if t == "" {
				return
			}
			if _, ok := seen[t]; ok {
				return
			}
			seen[t] = struct{}{}
			out = append(out, t)
		case *ftBooleanNode:
			for _, c := range v.clauses {
				walk(c.node)
			}
		case *ftBoostNode:
			walk(v.inner)
		case *occurMarker:
			walk(v.inner)
		}
	}
	walk(n)
	return out
}

// ---------------------------------------------------------------------------
// Levenshtein (small helper — corpus values are short so an O(mn) DP is fine)
// ---------------------------------------------------------------------------

func levenshtein(a, b string) int {
	if a == b {
		return 0
	}
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}
	prev := make([]int, len(b)+1)
	curr := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		curr[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			del := prev[j] + 1
			ins := curr[j-1] + 1
			sub := prev[j-1] + cost
			m := del
			if ins < m {
				m = ins
			}
			if sub < m {
				m = sub
			}
			curr[j] = m
		}
		prev, curr = curr, prev
	}
	return prev[len(b)]
}
