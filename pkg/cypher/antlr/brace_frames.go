package antlr

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
	"strings"
)

// Author: #1663 implementation lane. These framing adapters consume the
// existing lexical primitives. They do not classify a consumer's rewritten text.
func braceGapEnd(text string, at int) int     { end, _ := cyphertext.GapEnd(text, at); return end }
func queryGapEnd(text string, at int) int     { return braceGapEnd(text, at) }
func queryCommentEnd(text string, at int) int { return cyphertext.CommentEnd(text, at) }
func braceSkipSpaces(text string, at, end int) int {
	for at < end && cyphertext.IsWhitespaceAt(text, at) {
		at++
	}
	return at
}
func braceSymbolicName(text string) (string, bool) {
	name, ok := cyphertext.ScanIdentifier(text, 0)
	return name.Value, ok && name.End == len(text)
}
func braceKeywordAt(text string, at int, word string) bool {
	return (cyphertext.KeywordEnd(text, at, word, cyphertext.CypherWord) == at+len(word))
}
func braceStartsWithKeywords(text, first, rest string) bool {
	at := 0
	for _, word := range append([]string{first}, strings.Fields(rest)...) {
		at = braceGapEnd(text, at)
		if !braceKeywordAt(text, at, word) {
			return false
		}
		at += len(word)
	}
	return true
}
func braceLocalizedError(message localization.Message, cause error) error {
	return localization.NewLocalizedError(string(message.ID), message, cause)
}
func bracePathSyntaxError(message localization.Message) error {
	return braceLocalizedError(message, nil)
}
func StripComments(query string) string {
	var out strings.Builder
	last := 0
	for index := 0; index < len(query); index++ {
		switch c := query[index]; c {
		case '\'', '"', '`':
			index = braceQuotedEnd(query, index) - 1
		case '/':
			if end := queryCommentEnd(query, index); end >= 0 {
				out.WriteString(query[last:index])
				out.WriteString(CommentReplacement(query[index:end]))
				last = end
				index = end - 1
			}
		}
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
func CommentReplacement(comment string) string {
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

func braceQuotedEnd(text string, at int) int {
	end, _ := cyphertext.ScanQuotedText(text, at)
	return end
}
func braceStripComments(text string) string { return StripComments(text) }

var subqueryExpressionKeywords = [...]string{"EXISTS", "COUNT", "COLLECT"}

func UpperASCII(text string) string {
	for i := 0; i < len(text); i++ {
		if c := text[i]; c >= 'a' && c <= 'z' {
			// One allocation, as strings.ToUpper's ASCII path.
			var out strings.Builder
			out.Grow(len(text))
			out.WriteString(text[:i])
			for j := i; j < len(text); j++ {
				c := text[j]
				if c >= 'a' && c <= 'z' {
					c -= 'a' - 'A'
				}
				out.WriteByte(c)
			}
			return out.String()
		}
	}
	return text
}

// lowerASCII is upperASCII's lower-case counterpart: ASCII letters only, same
// length and positions as the text ('İ' lower-cases to three bytes with
// strings.ToLower). Cypher's toLower() on data keeps strings.ToLower.
func LowerASCII(text string) string {
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

var LiteralLeadingKeywords = map[string]struct{}{
	"RETURN": {}, "WITH": {}, "WHERE": {}, "AND": {}, "OR": {}, "XOR": {}, "NOT": {},
	"IN": {}, "IS": {}, "CASE": {}, "WHEN": {}, "THEN": {}, "ELSE": {}, "CONTAINS": {},
	"SKIP": {}, "LIMIT": {}, "UNWIND": {}, "FROM": {}, "FIELDTERMINATOR": {}, "OF": {},
	"DISTINCT": {}, "BY": {}, "YIELD": {}, "SHORTEST": {}, "ANY": {}, "ALL": {},
	"COMMIT": {}, "USE": {}, "OFFSET": {}, "DELETE": {},
	// trim([LEADING | TRAILING | BOTH] 'x' FROM s)
	"BOTH": {}, "LEADING": {}, "TRAILING": {},
}
