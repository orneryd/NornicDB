package cyphertext

// TokenKind classifies one significant token of Cypher text.
type TokenKind uint8

const (
	// Word is an unquoted name, keyword or number.
	Word TokenKind = iota + 1
	// String is a string literal, quotes included.
	String
	// QuotedName is a backtick-quoted name, backticks included.
	QuotedName
	// Parameter is $name, $`name` or $0.
	Parameter
	// Symbol is one punctuation or operator byte.
	Symbol
)

// Token is a half-open byte span of the original text. A string or quoted
// name is one opaque token, so its contents are never read as punctuation.
type Token struct {
	Start, End int
	Kind       TokenKind
}

// Text returns the token as written.
func (token Token) Text(text string) string { return text[token.Start:token.End] }

// Is reports whether the token is spelled exactly spelling.
func (token Token) Is(text, spelling string) bool {
	return token.End-token.Start == len(spelling) && text[token.Start:token.End] == spelling
}

// Keyword consumes the credentials lexical owner's complete-span identity.
// Tokens retain their source extent; no separate keyword field is derived.
func (token Token) Keyword(text, keyword string) bool {
	return token.Kind == Word && KeywordEnd(text, token.Start, keyword, CypherWord) == token.End
}

// Reader reads the significant tokens of Cypher text, skipping whitespace and
// comments. It uses this package's quotation, comment, whitespace and name
// rules, so its token boundaries are the scanners' boundaries. A Reader is a
// value: copying one gives lookahead without allocating.
type Reader struct {
	text        string
	position    int
	embeddedEnd func(int) int
	opaque      func(Span)
	tokenClosed bool
}

// NewReader starts a reader at a token boundary of text.
func NewReader(text string, start int) Reader { return Reader{text: text, position: start} }

// Seek moves the reader to a known token boundary, such as past a matched body.
func (reader *Reader) Seek(position int) { reader.position = position }

// Next returns the next significant token, or false at the end of the text.
func (reader *Reader) Next() (Token, bool) {
	text, position := reader.text, reader.position
	for position < len(text) {
		if width := WhitespaceAt(text, position); width > 0 {
			position += width
			continue
		}
		if end, _ := CommentEndStatus(text, position); end >= 0 {
			if reader.opaque != nil {
				reader.opaque(Span{position, end})
			}
			position = end
			continue
		}
		break
	}
	if position >= len(text) {
		reader.position = len(text)
		return Token{}, false
	}
	character := text[position]
	reader.tokenClosed = true
	token := Token{Start: position, End: position + 1, Kind: Symbol}
	switch {
	case IsQuote(character):
		if reader.embeddedEnd != nil && InterpolationQuote(text, position) {
			token.End, reader.tokenClosed = ScanInterpolatedText(text, position, reader.embeddedEnd)
		} else {
			token.End, reader.tokenClosed = ScanQuotedText(text, position)
		}
		token.Kind = String
		if IsBacktick(character) {
			token.Kind = QuotedName
		}
		if reader.opaque != nil {
			reader.opaque(Span{token.Start, token.End})
		}
	case character == '$':
		// Bound the reference owner's input to this candidate and a following
		// '('. This retains its dynamic-label and quoted-name rules without
		// rescanning the rest of the statement for each parameter.
		end := NamePartEnd(text, position+1)
		if name, ok := ScanIdentifier(text, position+1); ok && name.Quoted {
			end = name.End
		}
		if end < len(text) && text[end] == '(' {
			end++
		}
		ParameterReferences(text[position:end], func(_ string, start, end int) {
			if start == 0 {
				token.End, token.Kind = position+end, Parameter
			}
		})
	case NamePartAt(text, position):
		token.End = NamePartEnd(text, position)
		token.Kind = Word
	}
	reader.position = token.End
	return token, true
}
