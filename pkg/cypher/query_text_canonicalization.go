package cypher

import (
	"strings"
	"unicode/utf8"
)

// hasOwnStatementSyntax reports whether query is one of NornicDB's
// statements with a syntax of its own, which canonicalizeQueryText leaves as
// written but for its comments: the knowledge-policy DDL (CREATE / ALTER /
// DROP / SHOW DECAY, PROMOTION or RETENTION …), whose bodies separate
// settings and statements by lines; constraint contracts (CREATE CONSTRAINT
// … REQUIRE { … }, one predicate per line); and CREATE [OR REPLACE]
// PROCEDURE, which stores its body's text.
func hasOwnStatementSyntax(query string) bool {
	return hasOwnStatementSyntaxWithCancellation(query, nil)
}

func hasOwnStatementSyntaxWithCancellation(query string, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}

	var words [3]string
	count := 0
	for index := queryGapEndWithCancellation(query, 0, probe); index < len(query) && count < len(words); index = queryGapEndWithCancellation(query, index, probe) {
		if probe != nil && !probe.check() {
			return false
		}

		end := index
		for end < len(query) && ((query[end] >= 'A' && query[end] <= 'Z') || (query[end] >= 'a' && query[end] <= 'z')) {
			if probe != nil && !probe.check() {
				return false
			}

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
		if require := keywordIndexFrom(query, "REQUIRE", 0, defaultKeywordScanOpts()); require >= 0 {
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
	return leadingShellCommandsEndWithCancellation(query, nil)
}

func leadingShellCommandsEndWithCancellation(query string, probe *pipelineCancellation) int {
	if probe != nil && !probe.check() {
		return len(query)
	}

	end := 0
	for index := 0; ; {
		if probe != nil && !probe.check() {
			return len(query)
		}

		for index < len(query) {
			if probe != nil && !probe.check() {
				return len(query)
			}

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
		lineEnd := indexAnyWithCancellation(query[index:], "\r\n", probe)
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

// classificationText is the text a statement's permission and routing
// decisions read (read / write / schema / admin, cacheability, write
// detection): its canonical form, whatever NORNICDB_CYPHER_QUERY_NORMALIZATION
// says, so a comment or unusual spacing never changes them. A statement that
// is canonical already is returned as is, without allocating.
func classificationText(query string) string { return classificationTextWithCancellation(query, nil) }

func classificationTextWithCancellation(query string, probe *pipelineCancellation) string {
	if probe != nil && !probe.check() {
		return ""
	}

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
	return queryMayNeedCanonicalRewriteWithCancellation(query, nil)
}

func queryMayNeedCanonicalRewriteWithCancellation(query string, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}

	for i := 0; i < len(query); i++ {
		if probe != nil && !probe.check() {
			return false
		}

		switch c := query[i]; {
		case c == ')' || c == ']' || c == '}' || c == '\'' || c == '"' || c == '`':
			if gluedAliasKeywordAt(query, i+1) || c == '`' && aliasKeywordEndsAt(query, i) {
				return true
			}
		case c == ' ':
			if i+1 < len(query) && (query[i+1] == ' ' || arrowGapClass[query[i+1]]&arrowGapAfter != 0 && i > 0 && arrowGap(query[i-1], query[i+1])) {
				return true
			}
		case c == '/':
			if i+1 < len(query) && (query[i+1] == '/' || query[i+1] == '*') {
				return true
			}
		case c < ' ' || c >= utf8.RuneSelf:
			return true
		}
	}
	return false
}

// canonicalizeQueryText returns the canonical form of query (see above) and
// the rewrite that maps it back, or query and nil when it is canonical
// already; it doesn't allocate then, nor for a formatted statement whose
// rewrite is memoized (canonicalRewrites).
func canonicalizeQueryText(query string) (string, *queryRewrite) {
	return canonicalizeQueryTextWithCancellation(query, nil)
}

func canonicalizeQueryTextWithCancellation(query string, probe *pipelineCancellation) (canonical string, result *queryRewrite) {
	defer func() {
		if probe != nil && probe.err != nil {
			canonical, result = "", nil
		}
	}()
	if probe != nil && !probe.check() {
		return "", nil
	}

	if !queryMayNeedCanonicalRewrite(query) {
		return query, nil
	}
	return scanCanonicalQueryTextWithCancellation(query, probe)
}

// scanCanonicalQueryText is canonicalizeQueryText's full scan, for a query
// the quick check couldn't clear.
func scanCanonicalQueryText(query string) (string, *queryRewrite) {
	return scanCanonicalQueryTextWithCancellation(query, nil)
}

func scanCanonicalQueryTextWithCancellation(query string, probe *pipelineCancellation) (canonical string, result *queryRewrite) {
	defer func() {
		if probe != nil && probe.err != nil {
			canonical, result = "", nil
		}
	}()
	if probe != nil && !probe.check() {
		return "", nil
	}

	var (
		rewrite  *queryRewrite
		memoized *queryRewrite
		out      strings.Builder
		last     int
	)
	replace := func(start, end int, replacement string) {
		if probe != nil && !probe.check() {
			return
		}
		if rewrite == nil {
			if len(query) <= canonicalRewriteMemoMaxLen {
				if cached := canonicalRewrites.loadWithCancellation(query, probe); cached != nil {
					memoized = cached
					return
				}
				if probe != nil && probe.err != nil {
					return
				}
			}
			rewrite = &queryRewrite{original: query}
			out.Grow(pipelineInitialCapacity(len(query), probe))
		}
		if !appendPipelineString(&out, query[last:start], probe) {
			return
		}
		canonStart := out.Len()
		if !appendPipelineString(&out, replacement, probe) {
			return
		}
		rewrite.edits = appendPipelineItem(rewrite.edits, queryTextEdit{origStart: start, origEnd: end, canonStart: canonStart, canonEnd: out.Len()}, probe)
		if probe != nil && probe.err != nil {
			return
		}

		last = end
	}
	// query[:verbatimEnd] keeps its text but for its comments: the whole of
	// a statement with its own syntax, or the leading shell command lines
	// (:USE db, :param …), which end at their line breaks.
	verbatimEnd := leadingShellCommandsEnd(query)
	if hasOwnStatementSyntaxWithCancellation(query[verbatimEnd:], probe) {
		verbatimEnd = len(query)
	}
	for index := 0; index < len(query); {
		if probe != nil && !probe.check() {
			return "", nil
		}

		c := query[index]
		if c == '\'' || c == '"' || c == '`' {
			if c == '`' && index >= verbatimEnd && aliasKeywordEndsAt(query, index) {
				replace(index, index, " ")
				if memoized != nil {
					return memoized.canonical, memoized
				}
			}
			index = skipCypherQuotedText(query, index, c)
			if index >= verbatimEnd && gluedAliasKeywordAt(query, index) {
				replace(index, index, " ")
				if memoized != nil {
					return memoized.canonical, memoized
				}
			}
			continue
		}
		if (c == ')' || c == ']' || c == '}') && index >= verbatimEnd && gluedAliasKeywordAt(query, index+1) {
			// [1, 2][0]AS x: AS is the alias keyword, spaced as every
			// clause reader expects.
			replace(index+1, index+1, " ")
			if memoized != nil {
				return memoized.canonical, memoized
			}
			index++
			continue
		}
		if c > ' ' && c != '/' && c < utf8.RuneSelf {
			// Can't start a comment or a run of whitespace.
			index++
			continue
		}
		if index < verbatimEnd {
			if end := queryCommentEnd(query, index); end >= 0 {
				replace(index, end, commentReplacement(query[index:end]))
				if memoized != nil {
					return memoized.canonical, memoized
				}
				index = end
				continue
			}
			index++
			continue
		}
		if end := queryGapEnd(query, index); end > index {
			if index > 0 && end < len(query) && arrowGap(query[index-1], query[end]) && inPatternClause(query, index) {
				replace(index, end, "")
				if memoized != nil {
					return memoized.canonical, memoized
				}
			} else if end != index+1 || c != ' ' {
				replace(index, end, " ")
				if memoized != nil {
					return memoized.canonical, memoized
				}
			}
			index = end
			continue
		}
		index++
	}
	if rewrite == nil {
		return query, nil
	}
	if !appendPipelineString(&out, query[last:], probe) {
		return "", nil
	}
	rewrite.canonical = out.String()
	if len(query) <= canonicalRewriteMemoMaxLen {
		canonicalRewrites.storeWithCancellation(query, rewrite, probe)
	}
	return rewrite.canonical, rewrite
}

// inPatternClause reports whether query[index] is between the elements of a
// MATCH, MERGE or CREATE pattern: the nearest clause keyword before it, in
// its subquery or braces, is one of those (not ON CREATE or ON MATCH), and it
// is inside no parenthesis or bracket of that clause. There a dash or an
// arrowhead is part of an arrow; in an expression, x < - -1 and (2) - [3][0]
// keep their spaces.
func inPatternClause(query string, index int) bool {
	return inPatternClauseWithCancellation(query, index, nil)
}

func inPatternClauseWithCancellation(query string, index int, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}

	type frame struct {
		keyword string
		depth   int
	}
	frames := []frame{{}}
	previous := ""
	for i := 0; i < index; {
		if probe != nil && !probe.check() {
			return false
		}

		c := query[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			i = skipCypherQuotedText(query, i, c)
			continue
		case c == '/':
			if end := queryCommentEnd(query, i); end >= 0 {
				i = end
				continue
			}
		case c == '{':
			frames = appendPipelineItem(frames, frame{}, probe)
			if probe != nil && probe.err != nil {
				return false
			}

		case c == '}':
			if len(frames) > 1 {
				frames = frames[:len(frames)-1]
			}
		case c == '(' || c == '[':
			frames[len(frames)-1].depth++
		case c == ')' || c == ']':
			frames[len(frames)-1].depth--
		case isIdentByte(c) && (c < '0' || c > '9') && (i == 0 || !isIdentByte(query[i-1]) && query[i-1] != '.' && query[i-1] != '$'):
			end := i + 1
			for end < len(query) && isIdentByte(query[end]) {
				if probe != nil && !probe.check() {
					return false
				}

				end++
			}
			word := upperASCII(query[i:end])
			switch word {
			case "MATCH", "MERGE", "CREATE":
				if previous == "ON" {
					word = "ON"
				}
				frames[len(frames)-1].keyword = word
			case "ON", "WHERE", "RETURN", "WITH", "UNWIND", "SET", "DELETE", "REMOVE", "FOREACH", "CALL", "YIELD",
				"ORDER", "SKIP", "LIMIT", "UNION", "FINISH", "LET", "FILTER", "USE", "LOAD", "OFFSET":
				frames[len(frames)-1].keyword = word
			}
			previous = word
			i = end
			continue
		}
		i++
	}
	top := frames[len(frames)-1]
	return top.depth == 0 && (top.keyword == "MATCH" || top.keyword == "MERGE" || top.keyword == "CREATE")
}

// StripComments returns query without its Cypher comments: // to the end of
// the line, and /* … */ (replaced by a space, keeping its line breaks), as
// commentReplacement. // and /* inside string literals and quoted names are
// text. A query without comments is returned as is, without allocating. It
// is the comment rule the HTTP server's statement checks use (#683).
func StripComments(query string) string { return StripCommentsWithCancellation(query, nil) }

func StripCommentsWithCancellation(query string, probe *pipelineCancellation) string {
	if probe != nil && !probe.check() {
		return ""
	}

	var out strings.Builder
	last := 0
	for index := 0; index < len(query); index++ {
		if probe != nil && !probe.check() {
			return ""
		}

		switch c := query[index]; c {
		case '\'', '"', '`':
			index = skipCypherQuotedText(query, index, c) - 1
		case '/':
			if end := queryCommentEnd(query, index); end >= 0 {
				if !appendPipelineString(&out, query[last:index], probe) || !appendPipelineString(&out, commentReplacement(query[index:end]), probe) {
					return ""
				}
				last = end
				index = end - 1
			}
		}
	}
	if last == 0 {
		if probe != nil && probe.err != nil {
			return ""
		}
		return query
	}
	if !appendPipelineString(&out, query[last:], probe) {
		return ""
	}
	return out.String()
}

// commentReplacement is what a comment in a statement with its own syntax
// becomes: nothing for a line comment (its line break stays), one space plus
// its line breaks for a block comment.
func commentReplacement(comment string) string {
	return commentReplacementWithCancellation(comment, nil)
}

func commentReplacementWithCancellation(comment string, probe *pipelineCancellation) string {
	if probe != nil && !probe.check() {
		return ""
	}

	if strings.HasPrefix(comment, "//") {
		return ""
	}
	if !containsAnyWithCancellation(comment, "\r\n", probe) {
		return " "
	}
	var replacement strings.Builder
	if !appendPipelineByte(&replacement, ' ', probe) {
		return ""
	}

	for i := 0; i < len(comment); i++ {
		if probe != nil && !probe.check() {
			return ""
		}

		if comment[i] == '\n' || comment[i] == '\r' {
			if !appendPipelineByte(&replacement, comment[i], probe) {
				return ""
			}

		}
	}
	return replacement.String()
}
