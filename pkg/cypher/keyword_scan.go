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

func keywordIndexFrom(s, keyword string, from int, opts keywordScanOpts) int {
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
		return i
	}

	return -1
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
