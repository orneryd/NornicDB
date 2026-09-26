package cypher

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
