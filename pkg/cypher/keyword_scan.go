package cypher

// Clause and keyword grammar consumes the canonical internal/cyphertext
// tokenizer. Quotation, comments, names and parameters have no private lexer here.

import (
	"hash/maphash"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// cutDistinct reports whether text (a projection body, or an aggregate's
// argument list) starts with the DISTINCT keyword, and returns the text after
// it, trimmed. The keyword ends at any character that can't continue an
// identifier, so DISTINCT{a: 1}, DISTINCT(x) and DISTINCT followed by a
// newline are DISTINCT, and a name that only starts with it (distinctName) is
// not. A variable named distinct is a name, not the keyword, when what
// follows it can't start an expression (distinctFollowsAsName, #894).
// Without the keyword, text is returned trimmed.
func cutDistinct(text string) (string, bool) {
	text = strings.TrimSpace(text)
	const keyword = "DISTINCT"
	if len(text) <= len(keyword) || !strings.EqualFold(text[:len(keyword)], keyword) || isIdentByte(text[len(keyword)]) {
		return text, false
	}
	rest := strings.TrimSpace(text[len(keyword):])
	if rest == "" || distinctFollowsAsName(rest) {
		return text, false
	}
	return rest, true
}

// distinctFollowsAsName reports whether rest, the text after a projection's
// or aggregate's leading word distinct, makes that word a variable. Neo4j
// reads distinct as the keyword whenever rest can be an expression, so it is
// a variable only when rest can't start one: a property access, a label
// (distinct:Q), a separator, a comparison, / % ^ * (not RETURN DISTINCT *, a
// star projection) or ::, a map projection (distinct{.a}; DISTINCT {a: 1} is
// a map), AND, OR, XOR, CONTAINS, AS, IS (not IS ::), or a clause
// (startsWithClauseAfterDistinct: RETURN distinct ORDER BY distinct). Before
// + - [ ( a number, IN, STARTS, ENDS or a name, distinct is the keyword.
func distinctFollowsAsName(rest string) bool {
	switch rest[0] {
	case '.':
		return len(rest) == 1 || !isDigitByte(rest[1]) // RETURN DISTINCT .5
	case ',', ':', '/', '%', '^', '=', '<', '>':
		return true
	case '*':
		after := strings.TrimSpace(rest[1:])
		return after != "" && after[0] != ',' && !startsWithClauseAfterDistinct(after)
	case '{':
		after := strings.TrimSpace(rest[1:])
		return after != "" && after[0] == '.'
	}
	for _, word := range [...]string{"AS", "AND", "OR", "XOR", "CONTAINS"} {
		if matchKeywordAt(rest, 0, word) {
			return true
		}
	}
	if matchKeywordAt(rest, 0, "IS") {
		return !strings.HasPrefix(strings.TrimSpace(rest[len("IS"):]), "::")
	}
	return startsWithClauseAfterDistinct(rest)
}

// startsWithClauseAfterDistinct reports whether text, which follows the word
// distinct, starts with a clause rather than a name spelled as a clause
// keyword. As Neo4j reads it, the word is a name when it ends the text or
// what follows it continues an expression (RETURN DISTINCT order, RETURN
// DISTINCT skip - 1, RETURN DISTINCT where.x), and a clause otherwise
// (RETURN distinct SKIP 1, WITH distinct WHERE where = 3). UNION is a clause
// only as UNION ALL.
func startsWithClauseAfterDistinct(text string) bool {
	end := 0
	for end < len(text) && isIdentByte(text[end]) {
		end++
	}
	if end == 0 || !isNameableClauseKeyword(text[:end]) {
		return false
	}
	rest := strings.TrimSpace(text[end:])
	if strings.EqualFold(text[:end], "UNION") {
		return matchKeywordAt(rest, 0, "ALL")
	}
	return startsWithClauseAfterName(text)
}

// startsWithClauseAfterName reports whether text, which follows a name,
// starts with a clause: a clause keyword that isn't itself a name there. The
// word is a name when it ends the text, what follows it continues an
// expression, or a clause follows it (no clause keyword but OPTIONAL and
// UNION is followed directly by a clause: WITH where WHERE where = 3); ORDER
// is a clause only as ORDER BY and OPTIONAL only as OPTIONAL MATCH (WITH
// order ORDER BY order).
func startsWithClauseAfterName(text string) bool {
	end := 0
	for end < len(text) && isIdentByte(text[end]) {
		end++
	}
	if end == 0 || !isNameableClauseKeyword(text[:end]) {
		return false
	}
	rest := strings.TrimSpace(text[end:])
	switch {
	case strings.EqualFold(text[:end], "ORDER"):
		return matchKeywordAt(rest, 0, "BY")
	case strings.EqualFold(text[:end], "OPTIONAL"):
		return matchKeywordAt(rest, 0, "MATCH")
	}
	if rest == "" || nameContinuesExpression(rest) {
		return false
	}
	return strings.EqualFold(text[:end], "UNION") || !startsWithClauseAfterName(rest)
}

// nameContinuesExpression reports whether rest, the non-empty text after a
// name, continues an expression from that name: an operator, a separator, a
// property access, a subscript, a closing parenthesis, AS or a word
// operator.
func nameContinuesExpression(rest string) bool {
	if strings.IndexByte(".,:/%^=<>*+-[)", rest[0]) >= 0 {
		return true
	}
	if matchKeywordAt(rest, 0, "AS") {
		// AS introduces an alias only before a name (WHERE as = 3: as is a
		// variable).
		alias := strings.TrimSpace(rest[len("AS"):])
		return alias != "" && (isIdentByte(alias[0]) || alias[0] == '`')
	}
	for _, word := range [...]string{"AND", "OR", "XOR", "CONTAINS", "IS", "IN", "STARTS", "ENDS"} {
		if matchKeywordAt(rest, 0, word) {
			return true
		}
	}
	return false
}

// startsWithDistinct reports whether text starts with the DISTINCT keyword
// (cutDistinct).
func startsWithDistinct(text string) bool {
	_, distinct := cutDistinct(text)
	return distinct
}

// cutDistinctArgument is cutDistinct for a function's argument list, where
// a lone distinct is the keyword with no argument: count(distinct) has no
// argument, as Neo4j reads it, while RETURN distinct returns the variable.
func cutDistinctArgument(text string) (string, bool) {
	if strings.EqualFold(strings.TrimSpace(text), "DISTINCT") {
		return "", true
	}
	return cutDistinct(text)
}

// startsWithDistinctArgument reports whether a function's argument list
// starts with the DISTINCT keyword (cutDistinctArgument).
func startsWithDistinctArgument(text string) bool {
	_, distinct := cutDistinctArgument(text)
	return distinct
}

// keywordScan provides high-performance, allocation-free keyword searching with:
//   - case-insensitive matching
//   - flexible whitespace between keyword tokens
//   - skipping over string literals / backtick identifiers / comments
//   - optional skipping over nested (), [], {} regions
//
// It is used to harden clause routing against "keywords inside data" (e.g. string literals)
// and to make keyword detection lenient about whitespace without falling back to regex.

type keywordBoundaryMode uint8

const (
	keywordBoundaryWord keywordBoundaryMode = iota
	keywordBoundaryWhitespace
)

type keywordScanOpts struct {
	SkipParens   bool
	SkipBrackets bool
	SkipBraces   bool

	SkipStrings   bool
	SkipBackticks bool
	SkipComments  bool

	Boundary keywordBoundaryMode
}

type keywordIndexCacheKey struct {
	s       string
	keyword string
	from    int
}

// defaultKeywordIndexCache caches keyword positions found with the default
// scan options.
var defaultKeywordIndexCache = newBoundedCache[keywordIndexCacheKey, int](4096, keywordIndexCacheKeyShardIndex)

func defaultKeywordScanOpts() keywordScanOpts {
	return keywordScanOpts{
		SkipParens:    true,
		SkipBrackets:  true,
		SkipBraces:    false,
		SkipStrings:   true,
		SkipBackticks: true,
		SkipComments:  true,
		Boundary:      keywordBoundaryWord,
	}
}

func keywordIndex(s, keyword string) int {
	return keywordIndexFrom(s, keyword, 0, defaultKeywordScanOpts())
}

// topLevelKeywordIndex finds a keyword only at the "top level" of the query,
// skipping over nested (), [], and {} regions as well as string literals and comments.
//
// This is intended for clause splitting/routing (e.g., finding the RETURN after a CALL { ... }
// subquery), where keywords inside nested structures must not be treated as delimiters.
//
// NOTE: General-purpose findKeywordIndex intentionally does NOT skip {} by default, because
// braces are used for write operations (CREATE {...}) and CALL { ... } subqueries, and other
// analyzers need to see keywords within those bodies.
func topLevelKeywordIndex(s, keyword string) int {
	opts := defaultKeywordScanOpts()
	opts.SkipBraces = true
	return keywordIndexFrom(s, keyword, 0, opts)
}

// isWithKeyword reports whether a keyword search is for the WITH clause.
func isWithKeyword(keyword string) bool {
	ks, ke := trimKeywordWSBounds(keyword)
	if ke-ks != len("WITH") {
		return false
	}
	for j := 0; j < len("WITH"); j++ {
		if asciiUpper(keyword[ks+j]) != "WITH"[j] {
			return false
		}
	}
	return true
}

// isOperatorWith reports whether the WITH at pos belongs to the STARTS WITH /
// ENDS WITH string operator rather than starting a WITH clause. Every keyword
// scanner applies it when searching for WITH (keywordIndexFrom,
// findKeywordIndexInContext, findAllTopLevelPipelineKeywordPositions), so no
// caller splits a WHERE at the operator.
//
// The operator's STARTS / ENDS follows the end of an expression (an
// identifier, ')', ']', '}', or a closing quote). A variable named starts /
// ends before a WITH clause follows a keyword (AS, WITH, WHERE, AND, IN, ...),
// a comma, an operator symbol, or a '.' (property key), so that WITH is a
// clause: UNWIND [1] AS starts WITH starts RETURN starts.
func isOperatorWith(s string, pos int) bool {
	// The WITH of the type names TIME WITH TIME ZONE and TIMESTAMP WITH TIME
	// ZONE (type predicates, #838) is not a clause.
	if (prevWordEqualsIgnoreCase(s, pos, "TIME") || prevWordEqualsIgnoreCase(s, pos, "TIMESTAMP")) && followedByTimeZone(s, pos+len("WITH")) {
		return true
	}
	var wordStart int
	switch {
	case prevWordEqualsIgnoreCase(s, pos, "STARTS"):
		wordStart = prevWordStart(s, pos)
	case prevWordEqualsIgnoreCase(s, pos, "ENDS"):
		wordStart = prevWordStart(s, pos)
	default:
		return false
	}
	i := lastLiveByte(s, wordStart)
	if i < 0 {
		return false
	}
	switch c := s[i]; {
	case c == ')' || c == ']' || c == '}' || c == '\'' || c == '"' || c == '`':
		return true
	case isIdentByte(c):
		end := i + 1
		for i >= 0 && isIdentByte(s[i]) {
			i--
		}
		if i >= 0 && s[i] == '.' {
			return true // n.name STARTS WITH ...
		}
		return !isExpressionBoundaryWord(s[i+1 : end])
	default:
		return false
	}
}

// clauseKeywordUsedAsName reports whether the clause keyword matched at
// s[pos:end] is a name there, as Cypher's grammar reads it (#740). Only
// clause keywords are checked (keywords such as DISTINCT, NOT and CASE follow
// the words below in valid Cypher). Every keyword scanner applies it next to
// isOperatorWith. The keyword is a name when what precedes it is
//   - '.', ':' or '$' (a property key, label or map key, parameter);
//   - an operator or separator: + - / % ^ = < > , ( [ (a clause can't start
//     in an operand position; after | one does: FOREACH (x IN l | SET …),
//     and * reads alike as RETURN * and multiplication);
//   - a word that must be followed by a name or an expression (AS set,
//     RETURN set + 1, WHERE limit > 1, ORDER BY skip), unless that word is
//     itself a name (WITH 1 AS set RETURN …);
//
// or, when it starts the scanned text (a projection body such as
// "skip + 1 AS v"), when what follows it is an operator, a separator, AS or
// nothing. SKIP and LIMIT, which take an expression, are names before
// another clause keyword (RETURN skip ORDER BY skip). Inside a CASE … END
// block no clause can start, so a clause keyword there is a name wherever it
// stands (CASE match WHEN 1 THEN match END; #1162).
func clauseKeywordUsedAsName(s string, pos, end int, keyword string) bool {
	if !isNameableClauseKeyword(keyword) {
		return false
	}
	return clauseKeywordNamedByContext(s, pos, end, keyword) || insideCaseBlock(s, pos)
}

// clauseKeywordNamedByContext is clauseKeywordUsedAsName's reading of the
// keyword's neighbours.
func clauseKeywordNamedByContext(s string, pos, end int, keyword string) bool {
	i := lastLiveByte(s, pos)
	if i >= 0 {
		switch s[i] {
		case '.', ':':
			return true
		case '$':
			// $RETURN is a parameter; a$ RETURN ends the name a$.
			return isParameterStart(s, i)
		case '/':
			return true
		case '%':
			// n:% (any label, #860) ends an operand; a remainder operator
			// follows one.
			return !labelWildcardAt(s, i)
		case '+':
			// ((a)-->(b))+ RETURN …: a quantified path pattern's + ends the
			// pattern.
			return !quantifiedGroupQuantifierAt(s, i)
		case '-', '^', '=', '<', '>', ',', '(', '[':
			return true
		}
	}
	j := end
	for j < len(s) && isASCIISpace(s[j]) {
		j++
	}
	if j < len(s) && s[j] == ':' {
		return true // a map key: {set: 1}
	}
	if strings.EqualFold(keyword, "SKIP") || strings.EqualFold(keyword, "LIMIT") {
		wordEnd := j
		for wordEnd < len(s) && isIdentByte(s[wordEnd]) {
			wordEnd++
		}
		for _, next := range [...]string{"ORDER", "SKIP", "LIMIT", "WHERE", "UNION", "RETURN", "WITH"} {
			if strings.EqualFold(s[j:wordEnd], next) {
				return true
			}
		}
	}
	if i < 0 {
		// The keyword starts the text: read what follows.
		if j >= len(s) {
			return true
		}
		// A sign after it is a unary operator (RETURN +3, RETURN -3), as
		// a statement can only start with a clause.
		switch s[j] {
		case ',', '/', '%', '^', '=', '<', '>', ')':
			return true
		case '.':
			return j+1 < len(s) && !isDigitByte(s[j+1]) // RETURN .5 is a number
		}
		return j+2 <= len(s) && strings.EqualFold(s[j:j+2], "AS") && (j+2 == len(s) || !isIdentByte(s[j+2]))
	}
	if !isIdentByte(s[i]) {
		return false
	}
	wordEnd := i + 1
	for i >= 0 && isIdentByte(s[i]) {
		i--
	}
	if i >= 0 && s[i] == '.' {
		return false // n.as SET …: the word is a property key
	}
	if !isExpressionBoundaryWord(s[i+1 : wordEnd]) {
		return false
	}
	// The word before is a keyword only when it isn't itself a name
	// (WITH 1 AS set RETURN …: set is an alias, so RETURN is a clause).
	if wordIsName(s, i+1) {
		return false
	}
	if strings.EqualFold(s[i+1:wordEnd], "DISTINCT") {
		// distinct may be a variable: RETURN distinct ORDER BY distinct (#894).
		return !startsWithClauseAfterDistinct(s[pos:])
	}
	return true
}

// isDigitByte reports whether b is an ASCII digit.
func isDigitByte(b byte) bool { return b >= '0' && b <= '9' }

// wordIsName reports whether the word starting at wordStart is in a name
// position: after AS, or after '.', ':' or '$'. After a separator, an
// operator or a keyword an expression follows (WITH, RETURN, BY, WHERE, AND,
// OR, XOR, NOT, IN), a word other than NOT, CASE and DISTINCT is a name too
// (WITH x, where RETURN where, WITH where WHERE where = 3, RETURN by ORDER BY
// by, WHERE x AND not RETURN …; #894).
func wordIsName(s string, wordStart int) bool {
	i := lastLiveByte(s, wordStart)
	if i < 0 {
		return false
	}
	switch s[i] {
	case '.', ':', '$':
		return true
	case ',', '(', '[', '+', '-', '*', '/', '%', '^', '=', '<', '>':
		return !wordStartsExpression(s[wordStart:])
	}
	end := i + 1
	for i >= 0 && isIdentByte(s[i]) {
		i--
	}
	// The word before must be the keyword, not a name spelled the same
	// (WITH 3 AS as WITH as RETURN as, WITH 3 AS with WITH with WHERE …).
	switch word := s[i+1 : end]; {
	case strings.EqualFold(word, "AS"):
		return !wordIsName(s, i+1)
	case expressionFollowsKeyword(word):
		return !wordStartsExpression(s[wordStart:]) && !wordIsName(s, i+1)
	}
	return false
}

// expressionFollowsKeyword reports whether word is a keyword an expression
// follows: a projection (WITH, RETURN, ORDER BY), a predicate (WHERE) or an
// operand of a boolean or membership operator (AND, OR, XOR, NOT, IN).
func expressionFollowsKeyword(word string) bool {
	for _, keyword := range [...]string{"WITH", "RETURN", "BY", "WHERE", "AND", "OR", "XOR", "NOT", "IN"} {
		if strings.EqualFold(word, keyword) {
			return true
		}
	}
	return false
}

// wordStartsExpression reports whether text starts with a keyword that
// starts an expression or a projection where a name could stand: DISTINCT,
// or NOT or CASE unless a clause follows it (WITH case WHERE case = 1: case
// is a variable).
func wordStartsExpression(text string) bool {
	if matchKeywordAt(text, 0, "DISTINCT") {
		return true
	}
	for _, keyword := range [...]string{"NOT", "CASE"} {
		if matchKeywordAt(text, 0, keyword) {
			return !startsWithClauseAfterName(strings.TrimSpace(text[len(keyword):]))
		}
	}
	return false
}

// isNameableClauseKeyword reports whether keyword (its first word) starts a
// clause that a name spelled the same can be mistaken for.
func isNameableClauseKeyword(keyword string) bool {
	start, end := trimKeywordWSBounds(keyword)
	word := keyword[start:end]
	if space := strings.IndexAny(word, " \t"); space >= 0 {
		word = word[:space]
	}
	var candidates []string
	switch len(word) {
	case 3:
		candidates = nameableClauseKeywords3[:]
	case 4:
		candidates = nameableClauseKeywords4[:]
	case 5:
		candidates = nameableClauseKeywords5[:]
	case 6:
		candidates = nameableClauseKeywords6[:]
	case 7:
		candidates = nameableClauseKeywords7[:]
	case 8:
		candidates = nameableClauseKeywords8[:]
	default:
		return false
	}
	first := asciiUpper(word[0])
	for _, clause := range candidates {
		if clause[0] == first && equalFoldASCII(word, clause) {
			return true
		}
	}
	return false
}

// The clause keywords a name can be spelled as (isNameableClauseKeyword),
// by length.
var (
	nameableClauseKeywords3 = [...]string{"SET", "LET", "FOR"}
	nameableClauseKeywords4 = [...]string{"WITH", "SKIP", "CALL"}
	nameableClauseKeywords5 = [...]string{"MATCH", "MERGE", "UNION", "LIMIT", "ORDER", "WHERE"}
	nameableClauseKeywords6 = [...]string{"RETURN", "DELETE", "CREATE", "REMOVE", "UNWIND", "FINISH", "FILTER"}
	nameableClauseKeywords7 = [...]string{"FOREACH"}
	nameableClauseKeywords8 = [...]string{"OPTIONAL"}
)

// prevWordStart returns the start of the word that precedes pos (after
// skipping whitespace), for a pos where prevWordEqualsIgnoreCase matched.
func prevWordStart(s string, pos int) int {
	i := lastLiveByte(s, pos)
	for i >= 0 && isIdentByte(s[i]) {
		i--
	}
	return i + 1
}

// lastLiveByte returns the index of the last byte before pos that is live
// statement text — not whitespace and not inside a line comment or a block
// comment — or -1 when none is. Quoted spans are tracked so a comment-like
// sequence inside a string is not a comment; their quote bytes are live
// (they end expressions, so a clause keyword after a string literal follows
// a quote, not the operator the string belongs to). Backward word lookups
// (prevWordStart, prevWordEqualsIgnoreCase, clauseKeywordUsedAsName,
// wordIsName, isOperatorWith) must start from it: a plain whitespace skip
// would land inside a preceding comment and read the comment's last word as
// code (RETURN 1 // WITH\nWITH … read the comment's WITH as the previous
// word).
// labelWildcardAt reports whether the % at s[i] is the label expression
// wildcard (n:%, n:A|%, n:!%, n:(%)) rather than the remainder operator, which
// always follows an operand.
func labelWildcardAt(s string, i int) bool {
	j := lastLiveByte(s, i)
	return j >= 0 && strings.IndexByte(":|&!(", s[j]) >= 0
}

func lastLiveByte(s string, pos int) int {
	if pos <= 0 {
		return -1
	}
	reader := cyphertext.NewReader(s[:pos], 0)
	last := -1
	for span, ok := reader.Next(); ok; span, ok = reader.Next() {
		last = span.End - 1
	}
	return last
}

// isExpressionBoundaryWord reports whether word is a keyword after which an
// expression starts (so a following starts / ends is a variable, not the end
// of an expression).
func isExpressionBoundaryWord(word string) bool {
	switch len(word) {
	case 2, 3, 4, 5, 6, 8:
	default:
		return false
	}
	for _, keyword := range [...]string{"AS", "WITH", "RETURN", "WHERE", "BY", "DISTINCT", "AND", "OR", "XOR", "NOT", "IN", "CASE", "WHEN", "THEN", "ELSE", "UNWIND", "SET", "YIELD", "DELETE", "LET", "FOR", "FILTER"} {
		if len(keyword) != len(word) {
			continue
		}
		match := true
		for j := 0; j < len(word); j++ {
			if asciiUpper(word[j]) != keyword[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// keywordIndexFrom finds keyword from `from` on. When the keyword is WITH, the
// WITH of STARTS WITH / ENDS WITH is skipped (isOperatorWith, checked at each
// match in both scan loops, so cached results already exclude it).
func keywordIndexFrom(s, keyword string, from int, opts keywordScanOpts) int {
	if isDefaultKeywordScanOpts(opts) {
		return cachedKeywordIndexFromDefault(s, keyword, from)
	}
	return scanKeywordTokens(s, from, opts, keyword)
}
func cachedKeywordIndexFromDefault(s, keyword string, from int) int {
	key := keywordIndexCacheKey{s: s, keyword: keyword, from: from}
	if idx, ok := defaultKeywordIndexCache.get(key); ok {
		return idx
	}
	idx := keywordIndexFromDefault(s, keyword, from)
	defaultKeywordIndexCache.put(key, idx)
	return idx
}

func keywordIndexFromDefault(s, keyword string, from int) int {
	return firstKeywordIndexFromDefault(s, from, keyword)
}

// maxKeywordSet is the most keywords firstKeywordIndexFromDefault takes.
const maxKeywordSet = 16

// firstKeywordIndexFromDefault is the position of the earliest of keywords
// in s at or after from, with the default scan options (keywordIndex: the
// same strings, comments, nesting and name rules), or -1. A set of clause
// keywords is found in one scan rather than one scan per keyword. It takes
// at most maxKeywordSet keywords.
func firstKeywordIndexFromDefault(s string, from int, keywords ...string) int {
	return scanKeywordTokens(s, from, defaultKeywordScanOpts(), keywords...)
}

// scanKeywordTokens owns keyword grammar, never token boundaries. Visibility
// options use the same tokenizer as the normal parser, including names with $.
func scanKeywordTokens(s string, from int, opts keywordScanOpts, keywords ...string) int {
	if len(keywords) > maxKeywordSet {
		panic("firstKeywordIndexFromDefault: more than maxKeywordSet keywords")
	}
	if from < 0 {
		from = 0
	}
	if from >= len(s) {
		return -1
	}
	type bound struct {
		start, end int
		first      byte
	}
	var bounds [maxKeywordSet]bound
	for index, keyword := range keywords {
		start, end := trimKeywordWSBounds(keyword)
		if start < end {
			bounds[index] = bound{start, end, asciiUpper(keyword[start])}
		}
	}
	reader := cyphertext.NewReaderPolicy(s, from, cyphertext.LexicalPolicy{Strings: opts.SkipStrings, Backticks: opts.SkipBackticks, Comments: opts.SkipComments})
	parens, brackets, braces := 0, 0, 0
	for span, ok := reader.Next(); ok; span, ok = reader.Next() {
		if span.Kind == cyphertext.Symbol {
			switch span.Text(s) {
			case "(":
				parens++
			case ")":
				if parens > 0 {
					parens--
				}
			case "[":
				brackets++
			case "]":
				if brackets > 0 {
					brackets--
				}
			case "{":
				braces++
			case "}":
				if braces > 0 {
					braces--
				}
			}
		}
		if opts.SkipParens && parens > 0 || opts.SkipBrackets && brackets > 0 || opts.SkipBraces && braces > 0 || span.Kind != cyphertext.Word {
			continue
		}
		at := span.Start
		for index, keyword := range keywords {
			b := bounds[index]
			if b.start >= b.end || b.first != asciiUpper(s[at]) || !keywordLeftBoundaryOK(s, at, opts.Boundary) {
				continue
			}
			end, matches := keywordMatchAt(s, at, keyword, b.start, b.end)
			if !matches || !keywordRightBoundaryOK(s, end, opts.Boundary) {
				continue
			}
			if b.end-b.start == len("WITH") && isWithKeyword(keyword) && isOperatorWith(s, at) {
				continue
			}
			if clauseKeywordUsedAsName(s, at, end, keyword) {
				continue
			}
			return at
		}
	}
	return -1
}
func isDefaultKeywordScanOpts(opts keywordScanOpts) bool {
	return opts.SkipParens &&
		opts.SkipBrackets &&
		!opts.SkipBraces &&
		opts.SkipStrings &&
		opts.SkipBackticks &&
		opts.SkipComments &&
		opts.Boundary == keywordBoundaryWord
}

func keywordMatchAt(s string, pos int, keyword string, ks, ke int) (endPos int, ok bool) {
	query := cyphertext.NewReader(s, pos)
	words := cyphertext.NewReader(keyword[ks:ke], 0)
	endPos = pos
	for expected, more := words.Next(); more; expected, more = words.Next() {
		actual, exists := query.Next()
		if endPos == pos && (!exists || actual.Start != pos) {
			return 0, false
		}
		if !exists || !equalFoldASCII(actual.Text(s), expected.Text(keyword[ks:ke])) {
			return 0, false
		}
		endPos = actual.End
	}
	return endPos, endPos > pos
}
func trimKeywordWSBounds(s string) (start, end int) {
	start = 0
	end = len(s)
	for start < end && isASCIISpace(s[start]) {
		start++
	}
	for end > start && isASCIISpace(s[end-1]) {
		end--
	}
	return start, end
}

func keywordLeftBoundaryOK(s string, pos int, boundary keywordBoundaryMode) bool {
	if pos == 0 {
		return true
	}
	prev := s[pos-1]
	if boundary == keywordBoundaryWhitespace {
		return isASCIISpace(prev)
	}
	if prev == ':' || prev == '$' {
		return false
	}
	return !isIdentByte(prev)
}

func keywordRightBoundaryOK(s string, endPos int, boundary keywordBoundaryMode) bool {
	if endPos >= len(s) {
		return true
	}
	next := s[endPos]
	if boundary == keywordBoundaryWhitespace {
		return isASCIISpace(next)
	}
	if next == ':' {
		return false
	}
	return !isIdentByte(next)
}

func isCypherWhitespace(c byte) bool { return cyphertext.IsWhitespaceByte(c) }

func isASCIISpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

func asciiUpper(b byte) byte {
	if b >= 'a' && b <= 'z' {
		return b - ('a' - 'A')
	}
	return b
}

// upperASCII upper-cases the ASCII letters of statement text and leaves every
// other byte as it is, so the result has the same length as the text and a
// position found in it (a keyword's offset) indexes the text itself. Cypher
// keywords are ASCII, so it is the upper-casing for finding them.
// strings.ToUpper is not: a non-ASCII letter can change length ('ı' is two
// bytes, its 'I' one), which shifted every later position and cut the text
// in the wrong place (#748). Cypher's toUpper() on data keeps
// strings.ToUpper.
func upperASCII(text string) string {
	for i := 0; i < len(text); i++ {
		if c := text[i]; c >= 'a' && c <= 'z' {
			// One allocation, as strings.ToUpper's ASCII path.
			var out strings.Builder
			out.Grow(len(text))
			out.WriteString(text[:i])
			for j := i; j < len(text); j++ {
				out.WriteByte(asciiUpper(text[j]))
			}
			return out.String()
		}
	}
	return text
}

// lowerASCII is upperASCII's lower-case counterpart: ASCII letters only, same
// length and positions as the text ('İ' lower-cases to three bytes with
// strings.ToLower). Cypher's toLower() on data keeps strings.ToLower.
func lowerASCII(text string) string {
	for i := 0; i < len(text); i++ {
		if c := text[i]; c >= 'A' && c <= 'Z' {
			var out strings.Builder
			out.Grow(len(text))
			out.WriteString(text[:i])
			for j := i; j < len(text); j++ {
				c := text[j]
				if c >= 'A' && c <= 'Z' {
					c += 'a' - 'A'
				}
				out.WriteByte(c)
			}
			return out.String()
		}
	}
	return text
}

// indexASCIIFold returns the first index at which the lower-case ASCII
// substring occurs in text, compared case-insensitively, or -1. Zero
// allocations on the miss path; shares equalFoldASCII with the other ASCII
// keyword comparisons.
func indexASCIIFold(text, lowerSub string) int {
	if lowerSub == "" {
		return 0
	}
	first := lowerSub[0]
	for i := 0; i+len(lowerSub) <= len(text); i++ {
		if asciiLowerByte(text[i]) != first {
			continue
		}
		if equalFoldASCII(text[i:i+len(lowerSub)], lowerSub) {
			return i
		}
	}
	return -1
}

// isIdentStartByte reports whether b can start an unquoted name: an ASCII
// letter, '_', or any byte of a non-ASCII character. The scanners read the
// UTF-8 bytes of a non-ASCII character as part of a name, as the ANTLR
// lexer's Letter does, so ñRETURN is one name and RETURN in it is no
// keyword. Which non-ASCII characters Neo4j takes in a name is checked once,
// per character, by validateUnicodeOperators (isIdentifierStartRune,
// isIdentifierPartRune). It and isIdentByte are the scanners' one rule.
func isIdentStartByte(b byte) bool { return cyphertext.IdentifierStartByte(b) }

// isIdentStartRune is isIdentStartByte for a decoded character.
func isIdentStartRune(r rune) bool {
	return r >= utf8.RuneSelf || isIdentStartByte(byte(r))
}

// isIdentRune is isIdentByte for a decoded character.
func isIdentRune(r rune) bool {
	return r >= utf8.RuneSelf || isIdentByte(byte(r))
}

// isIdentByte reports whether b can continue an unquoted name: what
// isIdentStartByte takes, a digit, or '$': as in Neo4j, a$b and L$M are
// names, and $ starts a parameter only where no name goes on
// (isParameterStart).
func isIdentByte(b byte) bool { return cyphertext.IdentifierPartByte(b) }

// isParameterStart reports whether the '$' at query[index] starts a
// parameter ($name, $0, $`name`): it doesn't continue a name (a$b).
func isParameterStart(query string, index int) bool { return cyphertext.ParameterStart(query, index) }

func startsWithKeywordFold(s string, keywordUpper string) bool {
	if len(s) < len(keywordUpper) {
		return false
	}
	for i := 0; i < len(keywordUpper); i++ {
		if asciiUpper(s[i]) != keywordUpper[i] {
			return false
		}
	}
	if len(s) == len(keywordUpper) {
		return true
	}
	// Require a boundary so "MATCHX" doesn't count as "MATCH".
	return !isIdentByte(s[len(keywordUpper)])
}

// ---- query text canonicalization ----
//
// Cypher gives whitespace no meaning outside string literals and quoted
// names: ORDER  BY, ORDER /* c */ BY and ORDER<newline>BY are ORDER BY, and
// n.p<newline>= 1 is n.p = 1. The executor's clause routing and parsing read
// the statement as text, so canonicalizeQueryText rewrites it once, where it
// enters the executor, into one form: every run of whitespace and comments
// outside string literals and quoted names becomes one space. Whitespace is
// anything Neo4j accepts: space, tab, CR, LF, form feed, vertical tab and
// the Unicode spaces. Inside a relationship arrow of a MATCH, MERGE or
// CREATE pattern, (b) <- [r] - (a) and (a)-[r]- >(b), a gap is removed
// instead: (b)<-[r]-(a), (a)-[r]->(b) (arrowGap, inPatternClause).
//
// NornicDB's statements with a syntax of its own (hasOwnStatementSyntax), and
// the shell command lines that can start a statement (:USE db), keep their
// text; only their comments go, as they always have.
//
// The rewrite records its edits, so what a client sees is the text it sent
// (queryRewrite.restore): unaliased column names, error messages and
// EXPLAIN / PROFILE text. Routing and the query caches use the canonical
// text, so the formatting of a statement doesn't change how it runs (#740).

// queryTextEdit is one replacement: original[origStart:origEnd] became
// canonical[canonStart:canonEnd].
type queryTextEdit struct {
	origStart, origEnd   int
	canonStart, canonEnd int
}

// queryRewrite maps a canonical statement back to the text the client sent.
type queryRewrite struct {
	original  string
	canonical string
	edits     []queryTextEdit
	// verbatimColumns: a column whose text the client wrote is kept as is.
	// The label-expression rewrite inserts predicates, in which a short
	// column name (r) can occur last (#860).
	verbatimColumns bool
}

// queryWhitespaceAt returns the length of the whitespace character at
// query[index] (ASCII or a Unicode space), 0 when there is none.
func queryWhitespaceAt(query string, index int) int { return cyphertext.WhitespaceAt(query, index) }

// queryCommentEnd returns the end of the comment that starts at
// query[index], or -1 when none starts there. A line comment ends before
// its line break.
func queryCommentEnd(query string, index int) int {
	return cyphertext.CommentEnd(query, index)
}

// queryGapEnd returns the end of the run of whitespace and comments that
// starts at index, index itself when none does.
func queryGapEnd(query string, index int) int { return cyphertext.SkipTrivia(query, index) }

// hasOwnStatementSyntax reports whether query is one of NornicDB's
// statements with a syntax of its own, which canonicalizeQueryText leaves as
// written but for its comments: the knowledge-policy DDL (CREATE / ALTER /
// DROP / SHOW DECAY, PROMOTION or RETENTION …), whose bodies separate
// settings and statements by lines; constraint contracts (CREATE CONSTRAINT
// … REQUIRE { … }, one predicate per line); and CREATE [OR REPLACE]
// PROCEDURE, which stores its body's text.
func hasOwnStatementSyntax(query string) bool {
	var words [3]string
	count := 0
	for index := queryGapEnd(query, 0); index < len(query) && count < len(words); index = queryGapEnd(query, index) {
		end := index
		for end < len(query) && ((query[end] >= 'A' && query[end] <= 'Z') || (query[end] >= 'a' && query[end] <= 'z')) {
			end++
		}
		if end == index {
			break
		}
		words[count] = query[index:end]
		count++
		index = end
	}
	switch {
	case strings.EqualFold(words[0], "CREATE") && (strings.EqualFold(words[1], "PROCEDURE") ||
		strings.EqualFold(words[1], "OR") && strings.EqualFold(words[2], "REPLACE")):
		return true
	case strings.EqualFold(words[0], "CREATE") && strings.EqualFold(words[1], "CONSTRAINT"):
		// A constraint contract: REQUIRE { one predicate per line }.
		if require := keywordIndex(query, "REQUIRE"); require >= 0 {
			if open := queryGapEnd(query, require+len("REQUIRE")); open < len(query) && query[open] == '{' {
				return true
			}
		}
		return false
	case strings.EqualFold(words[0], "CREATE"), strings.EqualFold(words[0], "ALTER"),
		strings.EqualFold(words[0], "DROP"), strings.EqualFold(words[0], "SHOW"):
		return strings.EqualFold(words[1], "DECAY") || strings.EqualFold(words[1], "PROMOTION") ||
			strings.EqualFold(words[1], "RETENTION")
	}
	return false
}

// leadingShellCommandsEnd returns the end of the shell command lines
// (:USE db, :param x => 1, …) that start query: the start of the line after
// the last one, 0 when query starts with none.
func leadingShellCommandsEnd(query string) int {
	end := 0
	for index := 0; ; {
		for index < len(query) {
			if commentEnd := queryCommentEnd(query, index); commentEnd >= 0 {
				index = commentEnd
				continue
			}
			if size := queryWhitespaceAt(query, index); size > 0 {
				index += size
				continue
			}
			break
		}
		if index >= len(query) || query[index] != ':' {
			return end
		}
		lineEnd := strings.IndexAny(query[index:], "\r\n")
		if lineEnd < 0 {
			return len(query)
		}
		index += lineEnd + 1
		if query[index-1] == '\r' && index < len(query) && query[index] == '\n' {
			index++
		}
		end = index
	}
}

// canonicalRewrites memoizes the rewrites of formatted statements, which
// are immutable: a statement sent again with the same text gets the same
// rewrite without building it again (no allocation). Each slot holds the
// last statement whose text hashed to it, so a collision only replaces an
// entry; statements longer than canonicalRewriteMemoMaxLen (one-off batches
// with inline data) are not kept.
var canonicalRewrites = &canonicalRewriteMemo{seed: maphash.MakeSeed()}

const canonicalRewriteMemoMaxLen = 64 << 10

type canonicalRewriteMemo struct {
	seed  maphash.Seed
	slots [256]atomic.Pointer[queryRewrite]
}

func (m *canonicalRewriteMemo) slot(query string) *atomic.Pointer[queryRewrite] {
	return &m.slots[maphash.String(m.seed, query)%uint64(len(m.slots))]
}

// classificationText is the text a statement's permission and routing
// decisions read (read / write / schema / admin, cacheability, write
// detection): its canonical form, whatever NORNICDB_CYPHER_QUERY_NORMALIZATION
// says, so a comment or unusual spacing never changes them. A statement that
// is canonical already is returned as is, without allocating.
func classificationText(query string) string {
	canonical, _ := canonicalizeQueryText(query)
	return canonical
}

// queryMayNeedCanonicalRewrite reports whether query holds anything the
// canonical pass could rewrite: an ASCII whitespace other than a single
// space, two spaces in a row, the start of a comment, or a non-ASCII byte (a
// possible Unicode space). It doesn't read quotes, so it may answer true for
// text that needs no rewrite (two spaces in a string literal), never false
// for text that does. Most statements are canonical and leave the pass here,
// in one pass over their bytes.
func queryMayNeedCanonicalRewrite(query string) bool {
	return cyphertext.MayNeedNormalization(query, arrowGap, gluedAliasKeywordAt, aliasKeywordEndsAt)
}

// gluedAliasKeywordAt reports whether the keyword AS starts at query[at],
// right after a closing bracket or quote: AS, then a space, a backtick or
// the end.
func gluedAliasKeywordAt(query string, at int) bool {
	if at+2 > len(query) || !equalFoldASCII(query[at:at+2], "AS") {
		return false
	}
	return at+2 == len(query) || isASCIISpace(query[at+2]) || query[at+2] == '`'
}

// aliasKeywordEndsAt reports whether the keyword AS ends right before the
// backtick at query[at]: RETURN 1 AS`n` names the column n.
func aliasKeywordEndsAt(query string, at int) bool {
	return at >= 2 && equalFoldASCII(query[at-2:at], "AS") && (at == 2 || !isIdentByte(query[at-3]))
}

// canonicalizeQueryText returns the canonical form of query (see above) and
// the rewrite that maps it back, or query and nil when it is canonical
// already; it doesn't allocate then, nor for a formatted statement whose
// rewrite is memoized (canonicalRewrites).
func canonicalizeQueryText(query string) (string, *queryRewrite) {
	if !queryMayNeedCanonicalRewrite(query) {
		return query, nil
	}
	return scanCanonicalQueryText(query)
}

// scanCanonicalQueryText is canonicalizeQueryText's full scan, for a query
// the quick check couldn't clear.
func scanCanonicalQueryText(query string) (string, *queryRewrite) {
	var (
		rewrite  *queryRewrite
		memoized *queryRewrite
		out      strings.Builder
		last     int
	)
	replace := func(start, end int, replacement string) {
		if rewrite == nil {
			if len(query) <= canonicalRewriteMemoMaxLen {
				if cached := canonicalRewrites.slot(query).Load(); cached != nil && cached.original == query {
					memoized = cached
					return
				}
			}
			rewrite = &queryRewrite{original: query}
			out.Grow(len(query))
		}
		out.WriteString(query[last:start])
		canonStart := out.Len()
		out.WriteString(replacement)
		rewrite.edits = append(rewrite.edits, queryTextEdit{origStart: start, origEnd: end, canonStart: canonStart, canonEnd: out.Len()})
		last = end
	}
	// query[:verbatimEnd] keeps its text but for its comments: the whole of
	// a statement with its own syntax, or the leading shell command lines
	// (:USE db, :param …), which end at their line breaks.
	verbatimEnd := leadingShellCommandsEnd(query)
	if hasOwnStatementSyntax(query[verbatimEnd:]) {
		verbatimEnd = len(query)
	}
	reader := cyphertext.NewReader(query, 0)
	for span, ok := reader.NextRaw(); ok; span, ok = reader.NextRaw() {
		if span.Kind == cyphertext.QuotedName && span.Start >= verbatimEnd && aliasKeywordEndsAt(query, span.Start) {
			replace(span.Start, span.Start, " ")
			if memoized != nil {
				return memoized.canonical, memoized
			}
		}
		if span.Kind == cyphertext.String || span.Kind == cyphertext.QuotedName || span.Is(query, ")") || span.Is(query, "]") || span.Is(query, "}") {
			if span.End >= verbatimEnd && gluedAliasKeywordAt(query, span.End) {
				replace(span.End, span.End, " ")
				if memoized != nil {
					return memoized.canonical, memoized
				}
			}
			continue
		}
		if span.Kind != cyphertext.Whitespace && span.Kind != cyphertext.Comment {
			continue
		}
		if span.Unterminated {
			continue
		}
		if span.Start < verbatimEnd {
			if span.Kind == cyphertext.Comment {
				replace(span.Start, span.End, commentReplacement(span.Text(query)))
				if memoized != nil {
					return memoized.canonical, memoized
				}
			}
			continue
		}
		end := span.End
		for {
			lookahead := reader
			next, exists := lookahead.NextRaw()
			if !exists || next.Unterminated || next.Kind != cyphertext.Whitespace && next.Kind != cyphertext.Comment {
				break
			}
			end = next.End
			reader = lookahead
		}
		replacement := " "
		if span.Start > 0 && end < len(query) && arrowGap(query[span.Start-1], query[end]) && inPatternClause(query, span.Start) {
			replacement = ""
		}
		if end-span.Start != 1 || query[span.Start:end] != replacement {
			replace(span.Start, end, replacement)
			if memoized != nil {
				return memoized.canonical, memoized
			}
		}
	}
	if rewrite == nil {
		return query, nil
	}
	out.WriteString(query[last:])
	rewrite.canonical = out.String()
	if len(query) <= canonicalRewriteMemoMaxLen {
		canonicalRewrites.slot(query).Store(rewrite)
	}
	return rewrite.canonical, rewrite
}

// arrowGap reports whether a gap between before and after can be inside a
// relationship arrow: one side is a dash or an arrowhead, the other a node or
// relationship bracket, a dash or a head ((b) <-, - [, ] -, - >, -> ().
func arrowGap(before, after byte) bool {
	b, a := arrowGapClass[before], arrowGapClass[after]
	return b&arrowGapBefore != 0 && a&arrowGapAfter != 0 && (b|a)&arrowGapPart != 0
}

// arrowGapClass classifies the bytes arrowGap reads: one that can end the
// text before a gap in an arrow, one that can start the text after it, and
// an arrow's own dash or head. A table, so the quick check that runs on
// every space of a statement (queryMayNeedCanonicalRewrite) costs two loads.
var arrowGapClass = [256]uint8{
	')': arrowGapBefore, ']': arrowGapBefore, '(': arrowGapAfter, '[': arrowGapAfter,
	'-': arrowGapBefore | arrowGapAfter | arrowGapPart,
	'<': arrowGapBefore | arrowGapAfter | arrowGapPart,
	'>': arrowGapBefore | arrowGapAfter | arrowGapPart,
}

const (
	arrowGapBefore uint8 = 1 << iota
	arrowGapAfter
	arrowGapPart
)

// inPatternClause reports whether query[index] is between the elements of a
// MATCH, MERGE or CREATE pattern: the nearest clause keyword before it, in
// its subquery or braces, is one of those (not ON CREATE or ON MATCH), and it
// is inside no parenthesis or bracket of that clause. There a dash or an
// arrowhead is part of an arrow; in an expression, x < - -1 and (2) - [3][0]
// keep their spaces.
func inPatternClause(query string, index int) bool {
	type frame struct {
		keyword string
		depth   int
	}
	frames := []frame{{}}
	previousWord := ""
	var previous cyphertext.Token
	reader := cyphertext.NewReader(query[:index], 0)
	for span, ok := reader.Next(); ok; span, ok = reader.Next() {
		if span.Kind == cyphertext.Symbol {
			switch span.Text(query) {
			case "{":
				frames = append(frames, frame{})
			case "}":
				if len(frames) > 1 {
					frames = frames[:len(frames)-1]
				}
			case "(", "[":
				frames[len(frames)-1].depth++
			case ")", "]":
				frames[len(frames)-1].depth--
			}
		} else if span.Kind == cyphertext.Word && isIdentStartByte(query[span.Start]) && !previous.Is(query, ".") {
			word := upperASCII(span.Text(query))
			switch word {
			case "MATCH", "MERGE", "CREATE":
				if previousWord == "ON" {
					word = "ON"
				}
				frames[len(frames)-1].keyword = word
			case "ON", "WHERE", "RETURN", "WITH", "UNWIND", "SET", "DELETE", "REMOVE", "FOREACH", "CALL", "YIELD", "ORDER", "SKIP", "LIMIT", "UNION", "FINISH", "LET", "FILTER", "USE", "LOAD", "OFFSET":
				frames[len(frames)-1].keyword = word
			}
			previousWord = word
		}
		previous = span
	}
	top := frames[len(frames)-1]
	return top.depth == 0 && (top.keyword == "MATCH" || top.keyword == "MERGE" || top.keyword == "CREATE")
}

// StripComments returns query without its Cypher comments: // to the end of
// the line, and /* … */ (replaced by a space, keeping its line breaks), as
// commentReplacement. // and /* inside string literals and quoted names are
// text. A query without comments is returned as is, without allocating. It
// is the comment rule the HTTP server's statement checks use (#683).
func StripComments(query string) string {
	var out strings.Builder
	last := 0
	reader := cyphertext.NewReader(query, 0)
	for span, ok := reader.NextRaw(); ok; span, ok = reader.NextRaw() {
		if span.Kind != cyphertext.Comment || span.Unterminated {
			continue
		}
		out.WriteString(query[last:span.Start])
		out.WriteString(commentReplacement(span.Text(query)))
		last = span.End
	}
	if last == 0 {
		return query
	}
	out.WriteString(query[last:])
	return out.String()
}

// commentReplacement is what a comment in a statement with its own syntax
// becomes: nothing for a line comment (its line break stays), one space plus
// its line breaks for a block comment.
func commentReplacement(comment string) string {
	if strings.HasPrefix(comment, "//") {
		return ""
	}
	if !strings.ContainsAny(comment, "\r\n") {
		return " "
	}
	var replacement strings.Builder
	replacement.WriteByte(' ')
	for i := 0; i < len(comment); i++ {
		if comment[i] == '\n' || comment[i] == '\r' {
			replacement.WriteByte(comment[i])
		}
	}
	return replacement.String()
}

// originalOffset maps a canonical offset to the original text. An offset
// inside a replacement maps to the start of the replaced text when it starts
// a span (start), to its end when it ends one.
func (r *queryRewrite) originalOffset(offset int, start bool) int {
	shift := 0
	for _, edit := range r.edits {
		if start {
			if offset < edit.canonStart {
				break
			}
			if offset < edit.canonEnd {
				return edit.origStart
			}
		} else {
			// A span that ends where a replacement starts ends before it.
			if offset <= edit.canonStart {
				break
			}
			if offset <= edit.canonEnd {
				return edit.origEnd
			}
		}
		shift = edit.origEnd - edit.canonEnd
	}
	return offset + shift
}

// touches reports whether canonical[start:end] holds a replacement.
func (r *queryRewrite) touches(start, end int) bool {
	for _, edit := range r.edits {
		if edit.canonStart >= end {
			return false
		}
		if edit.canonEnd > start {
			return true
		}
	}
	return false
}

// originalText returns the text the client wrote for text, a part of the
// canonical statement (its last occurrence: the final RETURN names the
// columns); text itself when it isn't one or holds no replacement.
func (r *queryRewrite) originalText(text string) string {
	if text == "" || r.verbatimColumns && strings.Contains(r.original, text) {
		return text
	}
	// The last whole-token occurrence: a column c is not the c inside
	// percentileCont, which a rewrite may have written.
	start := strings.LastIndex(r.canonical, text)
	for start >= 0 && !wholeTokenAt(r.canonical, start, start+len(text)) {
		start = strings.LastIndex(r.canonical[:start], text)
	}
	if start < 0 || !r.touches(start, start+len(text)) {
		return text
	}
	return r.original[r.originalOffset(start, true):r.originalOffset(start+len(text), false)]
}

// wholeTokenAt reports whether text[start:end] doesn't continue a name on
// either side: the bytes around it aren't both name characters.
func wholeTokenAt(text string, start, end int) bool {
	return (start == 0 || !isIdentByte(text[start-1]) || !isIdentByte(text[start])) &&
		(end == len(text) || !isIdentByte(text[end]) || !isIdentByte(text[end-1]))
}

// restoreMessage puts the client's text back into a message: the canonical
// statement, a quoted part of it, and a parser position ("line 1:42") are
// those of the text it sent.
func (r *queryRewrite) restoreMessage(message string) string {
	if strings.Contains(message, r.canonical) {
		message = strings.ReplaceAll(message, r.canonical, r.original)
	}
	var out strings.Builder
	last := 0
	reader := cyphertext.NewReader(message, 0)
	for span, ok := reader.Next(); ok; span, ok = reader.Next() {
		if (span.Kind == cyphertext.String || span.Kind == cyphertext.QuotedName) && !span.Unterminated && span.End-span.Start > 2 {
			inner := message[span.Start+1 : span.End-1]
			if restored := r.originalText(inner); restored != inner {
				out.WriteString(message[last : span.Start+1])
				out.WriteString(restored)
				last = span.End - 1
			}
			continue
		}
		if span.Kind != cyphertext.Word || span.Text(message) != "line" || span.End >= len(message) || message[span.End] != ' ' {
			continue
		}
		line, column, end, valid := parseLineColumn(message, span.End+1)
		if !valid {
			continue
		}
		offset, found := offsetAtLineColumn(r.canonical, line, column)
		if !found {
			continue
		}
		origLine, origColumn := lineColumnAtOffset(r.original, r.originalOffset(offset, true))
		out.WriteString(message[last : span.End+1])
		out.WriteString(strconv.Itoa(origLine) + ":" + strconv.Itoa(origColumn))
		last = end
		reader.Seek(end)
	}
	if last == 0 {
		return message
	}
	out.WriteString(message[last:])
	return out.String()
}

// parseLineColumn reads "<line>:<column>" at message[start:], as the parser
// reports a position (line from 1, column from 0).
func parseLineColumn(message string, start int) (line, column, end int, ok bool) {
	number := func(index int) (int, int, bool) {
		value, next := 0, index
		for next < len(message) && message[next] >= '0' && message[next] <= '9' {
			value = value*10 + int(message[next]-'0')
			next++
		}
		return value, next, next > index
	}
	line, next, ok := number(start)
	if !ok || next >= len(message) || message[next] != ':' {
		return 0, 0, 0, false
	}
	column, end, ok = number(next + 1)
	return line, column, end, ok
}

// offsetAtLineColumn returns the byte offset of a parser position in text
// (line from 1, column in characters from 0).
func offsetAtLineColumn(text string, line, column int) (int, bool) {
	offset := 0
	for current := 1; current < line; current++ {
		next := strings.IndexByte(text[offset:], '\n')
		if next < 0 {
			return 0, false
		}
		offset += next + 1
	}
	for ; column > 0 && offset < len(text) && text[offset] != '\n'; column-- {
		_, width := utf8.DecodeRuneInString(text[offset:])
		offset += width
	}
	return offset, column == 0
}

// lineColumnAtOffset is the parser position (line from 1, column in
// characters from 0) of a byte offset in text.
func lineColumnAtOffset(text string, offset int) (line, column int) {
	if offset > len(text) {
		offset = len(text)
	}
	line, lineStart := 1, 0
	for index := 0; index < offset; index++ {
		if text[index] == '\r' || text[index] == '\n' && (index == 0 || text[index-1] != '\r') {
			line++
			lineStart = index + 1
		} else if text[index] == '\n' && index > 0 && text[index-1] == '\r' {
			lineStart = index + 1
		}
	}
	return line, utf8.RuneCountInString(text[lineStart:offset])
}

// restoredQueryError is err with a message that quotes the client's text.
type restoredQueryError struct {
	cause   error
	message string
}

func (e *restoredQueryError) Error() string { return e.message }
func (e *restoredQueryError) Unwrap() error { return e.cause }

// restore returns result and err as the client sees them: unaliased column
// names, error messages, and the text of an EXPLAIN / PROFILE plan use the
// text it sent. result is copied only when something differs, never
// changed, so a cached result stays canonical. Row values are data and are
// kept, except in a plan.
func (r *queryRewrite) restore(result *ExecuteResult, err error) (*ExecuteResult, error) {
	if r == nil {
		return result, err
	}
	if result != nil {
		var columns []string
		for i, column := range result.Columns {
			if original := r.originalText(column); original != column {
				if columns == nil {
					columns = append([]string(nil), result.Columns...)
				}
				columns[i] = original
			}
		}
		metadata, metadataRestored := r.restoreValue(result.Metadata)
		var rows interface{}
		rowsRestored := false
		if len(result.Rows) > 0 && isExplainOrProfileText(r.canonical) {
			rows, rowsRestored = r.restoreValue(result.Rows)
		}
		if columns != nil || metadataRestored || rowsRestored {
			restored := *result
			if columns != nil {
				restored.Columns = columns
			}
			if metadataRestored {
				restored.Metadata = metadata.(map[string]interface{})
			}
			if rowsRestored {
				restored.Rows = rows.([][]interface{})
			}
			result = &restored
		}
	}
	if err != nil {
		if message := err.Error(); r.restoreMessage(message) != message {
			err = &restoredQueryError{cause: err, message: r.restoreMessage(message)}
		}
	}
	return result, err
}

// isExplainOrProfileText reports whether a canonical statement starts with
// EXPLAIN or PROFILE.
func isExplainOrProfileText(query string) bool {
	query = strings.TrimLeft(query, " ")
	return startsWithKeywordFold(query, "EXPLAIN") || startsWithKeywordFold(query, "PROFILE")
}

// restoreValue restores the statement text in the strings of value (a plan
// or metadata), copying what it changes.
func (r *queryRewrite) restoreValue(value interface{}) (interface{}, bool) {
	switch typed := value.(type) {
	case string:
		restored := r.originalText(typed)
		if restored == typed {
			restored = r.restoreMessage(typed)
		}
		return restored, restored != typed
	case map[string]interface{}:
		var copied map[string]interface{}
		for key, item := range typed {
			if restored, changed := r.restoreValue(item); changed {
				if copied == nil {
					copied = make(map[string]interface{}, len(typed))
					for k, v := range typed {
						copied[k] = v
					}
				}
				copied[key] = restored
			}
		}
		if copied == nil {
			return value, false
		}
		return copied, true
	case []interface{}:
		var copied []interface{}
		for i, item := range typed {
			if restored, changed := r.restoreValue(item); changed {
				if copied == nil {
					copied = append([]interface{}(nil), typed...)
				}
				copied[i] = restored
			}
		}
		if copied == nil {
			return value, false
		}
		return copied, true
	case [][]interface{}:
		var copied [][]interface{}
		for i, row := range typed {
			if restored, changed := r.restoreValue(row); changed {
				if copied == nil {
					copied = append([][]interface{}(nil), typed...)
				}
				copied[i] = restored.([]interface{})
			}
		}
		if copied == nil {
			return value, false
		}
		return copied, true
	}
	return value, false
}

// skipCypherQuotedText returns the end of the string literal or quoted name
// that starts at query[start] (quote): a doubled quote, or a backslash escape
// in a string, doesn't end it.
func skipCypherQuotedText(query string, start int, quote byte) int {
	end, _ := scanCypherQuotedText(query, start, quote)
	return end
}

func scanCypherQuotedText(query string, start int, quote byte) (int, bool) {
	if start < 0 || start >= len(query) || query[start] != quote {
		return start, false
	}
	span, ok := cyphertext.TokenAt(query, start)
	if !ok || (span.Kind != cyphertext.String && span.Kind != cyphertext.QuotedName) {
		return start, false
	}
	return span.End, !span.Unterminated
}
func skipCypherLexicalToken(text string, index int) (int, bool) {
	return cyphertext.SkipToken(text, index)
}

func isCypherQuoteByte(character byte) bool       { return cyphertext.IsQuote(character) }
func isCypherStringQuoteByte(character byte) bool { return cyphertext.IsStringQuote(character) }
func isCypherSingleQuoteByte(character byte) bool { return cyphertext.IsSingleQuote(character) }
func isCypherDoubleQuoteByte(character byte) bool { return cyphertext.IsDoubleQuote(character) }
func isCypherBacktickByte(character byte) bool    { return cyphertext.IsBacktick(character) }

const (
	cypherQuoteClass       = -1
	cypherStringQuoteClass = -2
)

// cypherLexicalByte lets grammar-specific switch statements classify every
// quote kind through the canonical owner while retaining their local handling
// of parentheses, brackets and operators.
func cypherLexicalByte(character byte) int {
	if isCypherQuoteByte(character) {
		return cypherQuoteClass
	}
	return int(character)
}

func cypherStringLexicalByte(character byte) int {
	if isCypherStringQuoteByte(character) {
		return cypherStringQuoteClass
	}
	return int(character)
}

func scanIdentifierToken(text string, start int) (string, int, bool) {
	return cyphertext.ScanIdentifier(text, start)
}

func parseIdentifierToken(text string) (string, string, bool) {
	text = strings.TrimSpace(text)
	name, end, ok := scanIdentifierToken(text, 0)
	if !ok {
		return "", "", false
	}
	return name, text[end:], true
}

// findMatchingDelimiter skips Cypher quoted text and comments while matching
// nested delimiters, returning -1 for invalid offsets or unclosed sections.
func findMatchingDelimiter(text string, start int, opener, closer rune) int {
	return cyphertext.FindMatchingDelimiter(text, start, opener, closer)
}

func extractDelimitedSection(text string, opener, closer rune) (string, string, bool) {
	end := findMatchingDelimiter(text, 0, opener, closer)
	if end < 0 {
		return "", "", false
	}
	return text[1:end], text[end+1:], true
}

func extractParenSection(text string) (string, string, bool) {
	return extractDelimitedSection(text, '(', ')')
}

func extractBracketSection(text string) (string, string, bool) {
	return extractDelimitedSection(text, '[', ']')
}

// followedByTimeZone reports whether the words TIME ZONE follow position pos.
func followedByTimeZone(s string, pos int) bool {
	for _, word := range []string{"TIME", "ZONE"} {
		pos = skipSpaces(s, pos)
		if pos+len(word) > len(s) || !strings.EqualFold(s[pos:pos+len(word)], word) ||
			(pos+len(word) < len(s) && isIdentByte(s[pos+len(word)])) {
			return false
		}
		pos += len(word)
	}
	return true
}
