package cyphertext

import (
	"unicode"
	"unicode/utf8"
)

// TokenKind distinguishes opaque quoted tokens from words and punctuation.
type TokenKind uint8

const (
	Word TokenKind = iota + 1
	String
	QuotedName
	Symbol
	Parameter
	Whitespace
	Comment
)

// Token is a half-open byte span in the original source; quoted content is never
// interpreted as punctuation. Unterminated reports an unfinished string, quoted
// name or block comment. A line comment ending at EOF is complete.
type Token struct {
	Start, End   int
	Kind         TokenKind
	Unterminated bool
}

// Text returns the token's original spelling.
func (token Token) Text(text string) string { return text[token.Start:token.End] }

// Is reports an exact significant-token spelling.
func (token Token) Is(text, spelling string) bool {
	return token.End > token.Start && token.Text(text) == spelling
}

// Reader reads significant tokens using the shared quotation and comment owner.
// Copying a reader provides lookahead without allocating a token list.
type Reader struct {
	text     string
	position int
	policy   LexicalPolicy
	badAt    int
}

// LexicalPolicy retains the keyword scanner's supported visibility options.
// All modes use this tokenizer; consumers never implement opaque-token scans.
type LexicalPolicy struct{ Strings, Backticks, Comments bool }

// NewReader starts a forward reader at a known source boundary.
func NewReader(text string, start int) Reader {
	return NewReaderPolicy(text, start, LexicalPolicy{Strings: true, Backticks: true, Comments: true})
}

// NewReaderPolicy configures which lexical spans a structural search treats
// as opaque. The standard reader always treats every quoted/comment span so.
func NewReaderPolicy(text string, start int, policy LexicalPolicy) Reader {
	return Reader{text: text, position: start, policy: policy, badAt: -1}
}

// Seek moves to a known token boundary, for example after a matched body.
func (reader *Reader) Seek(position int) { reader.position = position }

// PreviousToken returns the last significant span before a source boundary.
func PreviousToken(text string, before int) Token {
	reader := NewReader(text, 0)
	var previous Token
	for span, ok := reader.Next(); ok && span.Start < before; span, ok = reader.Next() {
		previous = span
		if span.End >= before {
			break
		}
	}
	return previous
}

// Next returns the next significant token, omitting comments and whitespace.
func (reader *Reader) Next() (Token, bool) {
	for token, ok := reader.NextRaw(); ok; token, ok = reader.NextRaw() {
		if token.Kind != Whitespace && token.Kind != Comment {
			return token, true
		}
	}
	return Token{}, false
}

// NextRaw returns every token, including trivia, for source-preserving rewrites.
func (reader *Reader) NextRaw() (Token, bool) {
	start := reader.position
	if start >= len(reader.text) {
		reader.position = len(reader.text)
		return Token{}, false
	}
	end, kind := start+1, Symbol
	unterminated := false
	if width := WhitespaceAt(reader.text, start); width > 0 {
		kind = Whitespace
		end = start + width
		for end < len(reader.text) {
			width = WhitespaceAt(reader.text, end)
			if width == 0 {
				break
			}
			end += width
		}
	} else if comment, ok := ScanComment(reader.text, start); reader.policy.Comments && ok {
		kind, end, unterminated = comment.Kind, comment.End, comment.Unterminated
	} else if reader.policy.Strings && IsStringQuote(reader.text[start]) || reader.policy.Backticks && IsBacktick(reader.text[start]) {
		var closed bool
		if IsBacktick(reader.text[start]) {
			end, closed = ScanQuotedText(reader.text, start)
		} else {
			end, _, closed = ScanStringToken(reader.text, start)
		}
		unterminated = !closed
		kind = String
		if IsBacktick(reader.text[start]) {
			kind = QuotedName
		}
	} else if reader.text[start] == '$' && ParameterStart(reader.text, start) && start+1 < len(reader.text) {
		if nameEnd, ok := IdentifierEnd(reader.text, start+1); ok {
			kind, end = Parameter, nameEnd
		} else if reader.text[start+1] >= '0' && reader.text[start+1] <= '9' {
			kind, end = Parameter, start+2
			for end < len(reader.text) && IdentifierPartByte(reader.text[end]) {
				end++
			}
		}
	} else if IdentifierStartByte(reader.text[start]) {
		kind = Word
		end, _ = IdentifierEnd(reader.text, start)
	} else if reader.text[start] >= '0' && reader.text[start] <= '9' {
		kind = Word
		for end < len(reader.text) && IdentifierPartByte(reader.text[end]) {
			if reader.text[end] >= utf8.RuneSelf && WhitespaceAt(reader.text, end) > 0 {
				break
			}
			end++
		}
	}
	reader.position = end
	if unterminated && reader.badAt < 0 {
		reader.badAt = start
	}
	return Token{Start: start, End: end, Kind: kind, Unterminated: unterminated}, true
}

// UnterminatedAt reports the first unclosed token encountered by this reader.
func (reader *Reader) UnterminatedAt() (int, bool) { return reader.badAt, reader.badAt >= 0 }

// TokenAt reads a raw token at a known boundary for grammar adapters.
func TokenAt(text string, at int) (Token, bool) {
	if at < 0 || at >= len(text) {
		return Token{}, false
	}
	reader := NewReader(text, at)
	return reader.NextRaw()
}

// WhitespaceAt returns the byte width of Cypher whitespace at position.
func WhitespaceAt(text string, position int) int {
	if position < 0 || position >= len(text) {
		return 0
	}
	switch c := text[position]; {
	case IsWhitespaceByte(c):
		return 1
	case c >= utf8.RuneSelf:
		r, width := utf8.DecodeRuneInString(text[position:])
		if IsWhitespaceRune(r) {
			return width
		}
	}
	return 0
}

// IsWhitespaceRune is the same Cypher whitespace alphabet for rune adapters.
func IsWhitespaceRune(r rune) bool { return r != '\u0085' && unicode.IsSpace(r) }

// IsWhitespaceByte reports ASCII Cypher whitespace without decoding a rune.
func IsWhitespaceByte(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}

// TriviaSpan describes leading whitespace and comments without rewriting source.
// UnterminatedComment is the start of an unfinished block comment, or -1. End can
// equal EOF in either case; admission must inspect UnterminatedComment, not End.
type TriviaSpan struct {
	Start, End          int
	UnterminatedComment int
}

// ScanTrivia reads trivia at a known source boundary, stopping before the next
// significant token. Quotes and backticks are significant, even when their data
// contains comment delimiters. Statement framing can use this result permissively
// while admission rejects an UnterminatedComment without scanning the text again.
func ScanTrivia(text string, position int) TriviaSpan {
	span := TriviaSpan{Start: position, End: position, UnterminatedComment: -1}
	for position < len(text) {
		if width := WhitespaceAt(text, position); width > 0 {
			position += width
			continue
		}
		if comment, ok := ScanComment(text, position); ok {
			position = comment.End
			if comment.Unterminated {
				span.UnterminatedComment = comment.Start
				break
			}
			continue
		}
		break
	}
	span.End = position
	return span
}

// SkipTrivia is the permissive endpoint-only adapter. Strict admission uses
// ScanTrivia so an unfinished block comment is not mistaken for valid trivia.
func SkipTrivia(text string, position int) int { return ScanTrivia(text, position).End }

// TrimTrivia removes leading and trailing trivia without rewriting the source.
func TrimTrivia(text string) string {
	reader := NewReader(text, 0)
	first, ok := reader.Next()
	if !ok {
		return ""
	}
	end := first.End
	for token, ok := reader.Next(); ok; token, ok = reader.Next() {
		end = token.End
	}
	return text[first.Start:end]
}

// Segments reads comma-separated entries outside nested delimiters and quotes.
// Empty entries are preserved; an empty source contains no entries.
type Segments struct {
	text   string
	reader Reader
	start  int
	done   bool
}

// CommaSegments starts an allocation-free comma-separated entry reader.
func CommaSegments(text string) Segments {
	return Segments{text: text, reader: NewReader(text, 0), done: SkipTrivia(text, 0) == len(text)}
}

// Next returns the next entry, including any original leading or trailing trivia.
func (segments *Segments) Next() (string, bool) {
	if segments.done {
		return "", false
	}
	depth := 0
	for token, ok := segments.reader.Next(); ok; token, ok = segments.reader.Next() {
		if token.Kind != Symbol {
			continue
		}
		switch token.Text(segments.text) {
		case "(", "[", "{":
			depth++
		case ")", "]", "}":
			depth--
		case ",":
			if depth == 0 {
				part := segments.text[segments.start:token.Start]
				segments.start = token.End
				return part, true
			}
		}
	}
	segments.done = true
	return segments.text[segments.start:], true
}
