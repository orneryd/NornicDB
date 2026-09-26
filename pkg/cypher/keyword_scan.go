package cypher

import (
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// cutDistinct reports whether text (a projection body, or an aggregate's
// argument list) starts with the DISTINCT keyword, and returns the text after
// it, trimmed. The keyword ends at any character that can't continue an
// identifier, so DISTINCT{a: 1}, DISTINCT(x) and DISTINCT followed by a
// newline are DISTINCT, and a name that only starts with it (distinctName) is
// not. Without the keyword, text is returned trimmed.
func cutDistinct(text string) (string, bool) {
	text = strings.TrimSpace(text)
	const keyword = "DISTINCT"
	if len(text) <= len(keyword) || !strings.EqualFold(text[:len(keyword)], keyword) || isAlphaNumericByte(text[len(keyword)]) {
		return text, false
	}
	return strings.TrimSpace(text[len(keyword):]), true
}

// startsWithDistinct reports whether text starts with the DISTINCT keyword
// (cutDistinct).
func startsWithDistinct(text string) bool {
	_, distinct := cutDistinct(text)
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
var defaultKeywordIndexCache = newBoundedCache[keywordIndexCacheKey, int](4096)

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
	var wordStart int
	switch {
	case prevWordEqualsIgnoreCase(s, pos, "STARTS"):
		wordStart = prevWordStart(s, pos)
	case prevWordEqualsIgnoreCase(s, pos, "ENDS"):
		wordStart = prevWordStart(s, pos)
	default:
		return false
	}
	i := wordStart - 1
	for i >= 0 && isASCIISpace(s[i]) {
		i--
	}
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

// prevWordStart returns the start of the word that precedes pos (after
// skipping whitespace), for a pos where prevWordEqualsIgnoreCase matched.
func prevWordStart(s string, pos int) int {
	i := pos - 1
	for i >= 0 && isASCIISpace(s[i]) {
		i--
	}
	for i >= 0 && isIdentByte(s[i]) {
		i--
	}
	return i + 1
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
	for _, keyword := range [...]string{"AS", "WITH", "RETURN", "WHERE", "BY", "DISTINCT", "AND", "OR", "XOR", "NOT", "IN", "CASE", "WHEN", "THEN", "ELSE", "UNWIND", "SET", "YIELD"} {
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

	ks, ke := trimKeywordWSBounds(keyword)
	if ks >= ke {
		return -1
	}
	if from < 0 {
		from = 0
	}
	if from >= len(s) {
		return -1
	}

	first := asciiUpper(keyword[ks])

	var (
		parenDepth   int
		bracketDepth int
		braceDepth   int

		inSingleQuote  bool
		inDoubleQuote  bool
		inBacktick     bool
		inLineComment  bool
		inBlockComment bool
	)

	for i := from; i < len(s); i++ {
		c := s[i]

		if opts.SkipComments {
			if inLineComment {
				if c == '\n' {
					inLineComment = false
				}
				continue
			}
			if inBlockComment {
				if c == '*' && i+1 < len(s) && s[i+1] == '/' {
					inBlockComment = false
					i++
				}
				continue
			}
		}

		if opts.SkipStrings {
			if inSingleQuote {
				if c == '\\' && i+1 < len(s) {
					i++
					continue
				}
				if c == '\'' {
					if i+1 < len(s) && s[i+1] == '\'' {
						i++
						continue
					}
					inSingleQuote = false
				}
				continue
			}
			if inDoubleQuote {
				if c == '\\' && i+1 < len(s) {
					i++
					continue
				}
				if c == '"' {
					if i+1 < len(s) && s[i+1] == '"' {
						i++
						continue
					}
					inDoubleQuote = false
				}
				continue
			}
		}

		if opts.SkipBackticks && inBacktick {
			if c == '`' {
				if i+1 < len(s) && s[i+1] == '`' {
					i++
					continue
				}
				inBacktick = false
			}
			continue
		}

		if opts.SkipComments && c == '/' && i+1 < len(s) {
			if s[i+1] == '/' {
				inLineComment = true
				i++
				continue
			}
			if s[i+1] == '*' {
				inBlockComment = true
				i++
				continue
			}
		}

		if opts.SkipStrings {
			if c == '\'' {
				inSingleQuote = true
				continue
			}
			if c == '"' {
				inDoubleQuote = true
				continue
			}
		}
		if opts.SkipBackticks && c == '`' {
			inBacktick = true
			continue
		}

		switch c {
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		case '{':
			braceDepth++
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
		}

		if (opts.SkipParens && parenDepth > 0) ||
			(opts.SkipBrackets && bracketDepth > 0) ||
			(opts.SkipBraces && braceDepth > 0) {
			continue
		}

		if asciiUpper(c) != first {
			continue
		}

		if !keywordLeftBoundaryOK(s, i, opts.Boundary) {
			continue
		}

		endPos, ok := keywordMatchAt(s, i, keyword, ks, ke)
		if !ok {
			continue
		}
		if !keywordRightBoundaryOK(s, endPos, opts.Boundary) {
			continue
		}
		if ke-ks == len("WITH") && isWithKeyword(keyword) && isOperatorWith(s, i) {
			continue
		}
		return i
	}

	return -1
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
	ks, ke := trimKeywordWSBounds(keyword)
	if ks >= ke {
		return -1
	}
	if from < 0 {
		from = 0
	}
	if from >= len(s) {
		return -1
	}

	first := asciiUpper(keyword[ks])

	var (
		parenDepth   int
		bracketDepth int
		braceDepth   int

		inSingleQuote  bool
		inDoubleQuote  bool
		inBacktick     bool
		inLineComment  bool
		inBlockComment bool
	)

	for i := from; i < len(s); i++ {
		c := s[i]

		if inLineComment {
			if c == '\n' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			if c == '*' && i+1 < len(s) && s[i+1] == '/' {
				inBlockComment = false
				i++
			}
			continue
		}
		if inSingleQuote {
			if c == '\\' && i+1 < len(s) {
				i++
				continue
			}
			if c == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inSingleQuote = false
			}
			continue
		}
		if inDoubleQuote {
			if c == '\\' && i+1 < len(s) {
				i++
				continue
			}
			if c == '"' {
				if i+1 < len(s) && s[i+1] == '"' {
					i++
					continue
				}
				inDoubleQuote = false
			}
			continue
		}
		if inBacktick {
			if c == '`' {
				if i+1 < len(s) && s[i+1] == '`' {
					i++
					continue
				}
				inBacktick = false
			}
			continue
		}
		if c == '/' && i+1 < len(s) {
			if s[i+1] == '/' {
				inLineComment = true
				i++
				continue
			}
			if s[i+1] == '*' {
				inBlockComment = true
				i++
				continue
			}
		}
		if c == '\'' {
			inSingleQuote = true
			continue
		}
		if c == '"' {
			inDoubleQuote = true
			continue
		}
		if c == '`' {
			inBacktick = true
			continue
		}

		switch c {
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		case '{':
			braceDepth++
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
		}

		if parenDepth > 0 || bracketDepth > 0 {
			continue
		}

		if asciiUpper(c) != first {
			continue
		}

		if !keywordLeftBoundaryOK(s, i, keywordBoundaryWord) {
			continue
		}

		endPos, ok := keywordMatchAt(s, i, keyword, ks, ke)
		if !ok {
			continue
		}
		if !keywordRightBoundaryOK(s, endPos, keywordBoundaryWord) {
			continue
		}
		if ke-ks == len("WITH") && isWithKeyword(keyword) && isOperatorWith(s, i) {
			continue
		}
		return i
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
	j := pos
	k := ks

	for k < ke {
		ck := keyword[k]
		if isASCIISpace(ck) {
			for k < ke && isASCIISpace(keyword[k]) {
				k++
			}
			if j >= len(s) || !isASCIISpace(s[j]) {
				return 0, false
			}
			for j < len(s) && isASCIISpace(s[j]) {
				j++
			}
			continue
		}
		if j >= len(s) {
			return 0, false
		}
		if asciiUpper(s[j]) != asciiUpper(ck) {
			return 0, false
		}
		j++
		k++
	}

	return j, true
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
	if prev == ':' {
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

func isASCIISpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

func asciiUpper(b byte) byte {
	if b >= 'a' && b <= 'z' {
		return b - ('a' - 'A')
	}
	return b
}

func isIdentByte(b byte) bool {
	if b >= 0x80 {
		return true
	}
	if b >= 'A' && b <= 'Z' {
		return true
	}
	if b >= 'a' && b <= 'z' {
		return true
	}
	if b >= '0' && b <= '9' {
		return true
	}
	return b == '_'
}

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
// the Unicode spaces.
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
}

// queryWhitespaceAt returns the length of the whitespace character at
// query[index] (ASCII or a Unicode space), 0 when there is none.
func queryWhitespaceAt(query string, index int) int {
	switch c := query[index]; {
	case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
		return 1
	case c >= utf8.RuneSelf:
		r, width := utf8.DecodeRuneInString(query[index:])
		if unicode.IsSpace(r) {
			return width
		}
	}
	return 0
}

// queryCommentEnd returns the end of the comment that starts at
// query[index], or -1 when none starts there. A line comment ends before
// its line break.
func queryCommentEnd(query string, index int) int {
	if index+1 >= len(query) || query[index] != '/' {
		return -1
	}
	switch query[index+1] {
	case '/':
		end := index + 2
		for end < len(query) && query[end] != '\n' && query[end] != '\r' {
			end++
		}
		return end
	case '*':
		if close := strings.Index(query[index+2:], "*/"); close >= 0 {
			return index + 2 + close + 2
		}
		return len(query)
	}
	return -1
}

// queryGapEnd returns the end of the run of whitespace and comments that
// starts at index, index itself when none does.
func queryGapEnd(query string, index int) int {
	end := index
	for end < len(query) {
		if commentEnd := queryCommentEnd(query, end); commentEnd >= 0 {
			end = commentEnd
			continue
		}
		size := queryWhitespaceAt(query, end)
		if size == 0 {
			break
		}
		end += size
	}
	return end
}

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
		lineEnd := strings.IndexByte(query[index:], '\n')
		if lineEnd < 0 {
			return len(query)
		}
		index += lineEnd + 1
		end = index
	}
}

// canonicalizeQueryText returns the canonical form of query (see above) and
// the rewrite that maps it back, or query and nil when it is canonical
// already; it doesn't allocate then.
func canonicalizeQueryText(query string) (string, *queryRewrite) {
	var (
		rewrite *queryRewrite
		out     strings.Builder
		last    int
	)
	replace := func(start, end int, replacement string) {
		if rewrite == nil {
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
	for index := 0; index < len(query); {
		c := query[index]
		if c == '\'' || c == '"' || c == '`' {
			index = skipCypherQuotedText(query, index, c)
			continue
		}
		if index < verbatimEnd {
			if end := queryCommentEnd(query, index); end >= 0 {
				replace(index, end, commentReplacement(query[index:end]))
				index = end
				continue
			}
			index++
			continue
		}
		if end := queryGapEnd(query, index); end > index {
			if end != index+1 || c != ' ' {
				replace(index, end, " ")
			}
			index = end
			continue
		}
		index++
	}
	if rewrite == nil {
		return query, nil
	}
	out.WriteString(query[last:])
	rewrite.canonical = out.String()
	return rewrite.canonical, rewrite
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
	if text == "" {
		return text
	}
	start := strings.LastIndex(r.canonical, text)
	if start < 0 || !r.touches(start, start+len(text)) {
		return text
	}
	return r.original[r.originalOffset(start, true):r.originalOffset(start+len(text), false)]
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
	for index := 0; index < len(message); index++ {
		switch quote := message[index]; {
		case quote == '\'' || quote == '"' || quote == '`':
			close := strings.IndexByte(message[index+1:], quote)
			if close <= 0 {
				continue
			}
			inner := message[index+1 : index+1+close]
			if restored := r.originalText(inner); restored != inner {
				out.WriteString(message[last : index+1])
				out.WriteString(restored)
				last = index + 1 + close
			}
			index += close + 1
		case strings.HasPrefix(message[index:], "line "):
			line, column, end, ok := parseLineColumn(message, index+len("line "))
			if !ok {
				continue
			}
			offset, found := offsetAtLineColumn(r.canonical, line, column)
			if !found {
				continue
			}
			origLine, origColumn := lineColumnAtOffset(r.original, r.originalOffset(offset, true))
			out.WriteString(message[last : index+len("line ")])
			out.WriteString(strconv.Itoa(origLine) + ":" + strconv.Itoa(origColumn))
			last = end
			index = end - 1
		}
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
	lineStart := strings.LastIndexByte(text[:offset], '\n') + 1
	return strings.Count(text[:offset], "\n") + 1, utf8.RuneCountInString(text[lineStart:offset])
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
	for index := start + 1; index < len(query); index++ {
		if query[index] == '\\' && quote != '`' && index+1 < len(query) {
			index++
			continue
		}
		if query[index] != quote {
			continue
		}
		if index+1 < len(query) && query[index+1] == quote {
			index++
			continue
		}
		return index + 1
	}
	return len(query)
}
