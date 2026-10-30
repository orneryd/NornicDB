package cyphertext

// QueryBodyContainerKeywords are the introducers of statement bodies.
var QueryBodyContainerKeywords = [...]string{"CALL", "EXISTS", "COUNT", "COLLECT"}

// QueryStructureClauseStarts are the clause starts admitted by a query body.
var QueryStructureClauseStarts = []string{
	"MATCH", "OPTIONAL", "WITH", "RETURN", "UNWIND", "CALL", "CREATE", "MERGE", "DELETE", "DETACH", "SET", "REMOVE",
	"FOREACH", "LOAD", "FILTER", "LET", "FOR", "FINISH", "USE", "WHEN", "INSERT", "ORDER", "SKIP", "LIMIT", "OFFSET",
}

// ValidMapLiteralKey checks one complete key token without decoding its value.

// VisitMapEntries visits a complete literal map through the shared entry
// grammar. The callback receives keys as written and unevaluated values.
// False means malformed syntax; a caller must discard any partial result.

// MapEntryIssue describes an entry failure without choosing a consumer's message.
type MapEntryIssue struct {
	Kind  MapEntryIssueKind
	Entry string
}

// MapEntryIssueKind identifies the grammar rule a literal map violates.
type MapEntryIssueKind uint8

const (
	MapEnclosureRequired MapEntryIssueKind = iota
	MapEntryEmpty
	MapEntryInvalid
	MapKeyInvalid
)

// VisitMapEntriesChecked is VisitMapEntries with the owner's failure reason.
// Materializers can retain their established diagnostics without parsing again.

// BraceToken is a significant token and what the scanner knew about it.
type BraceToken struct {
	Token
	// ParenOpen is a ')' token's '(' (-1 otherwise); CallParen reports that
	// the parentheses are CALL's import list.
	ParenOpen int
	CallParen bool
	// MapColon reports a ':' after a map key ({k: …}), not a label.
	MapColon bool
	// Body reports a '{' that opens a query body (subqueryBrace).
	Body bool
}

type braceParen struct {
	open int
	call bool
}

// originalBraceScanner reads a statement's tokens once, left to right, and
// classifies each brace it passes. At answers for increasing brace offsets
// without rereading the text.
type originalBraceScanner struct {
	Query                string
	Last, Prior, Earlier BraceToken
	parens               []braceParen
	tokens               []Token
	tokenIndex           int
	startsQuery          map[int]bool
}

// NeworiginalBraceScanner starts a scanner whose first answer is at or after
// start. The tokens before start are still read: a consumer that begins inside
// an expression keeps the brace's real predecessor (CALL, a map key's colon).

// Next reads the next significant token.
func (scanner *originalBraceScanner) next() (Token, bool) {
	if scanner.tokenIndex >= len(scanner.tokens) {
		return Token{}, false
	}
	token := scanner.tokens[scanner.tokenIndex]
	scanner.tokenIndex++
	query := scanner.Query
	info := BraceToken{Token: token, ParenOpen: -1}
	if token.Kind == Symbol {
		switch query[token.Start] {
		case '(':
			scanner.parens = append(scanner.parens, braceParen{open: token.Start, call: scanner.keyword("CALL")})
		case ')':
			if count := len(scanner.parens); count > 0 {
				info.ParenOpen, info.CallParen = scanner.parens[count-1].open, scanner.parens[count-1].call
				scanner.parens = scanner.parens[:count-1]
			}
		case ':':
			info.MapColon = (scanner.Prior.Is(query, "{") || scanner.Prior.Is(query, ",")) && BraceEntryName(query, scanner.Last.Token)
		case '{':
			info.Body = scanner.opensBody(token.Start)
		}
	}
	scanner.Earlier, scanner.Prior, scanner.Last = scanner.Prior, scanner.Last, info
	return token, true
}

// SkipBody continues after the query body that closes at close.

// At reports whether the '{' at brace opens a query body.

// keyword reports whether the Last token is word used as a keyword: not a
// property name (n.count) or a label (:Count), but possibly a map value
// ({k: COUNT { … }}).
func (scanner *originalBraceScanner) keyword(word string) bool {
	query := scanner.Query
	return scanner.Last.Keyword(query, word) &&
		!scanner.Prior.Is(query, ".") && (!scanner.Prior.Is(query, ":") || scanner.Prior.MapColon)
}

// opensBody decides, as the brace is read, whether it opens a query body.
func (scanner *originalBraceScanner) opensBody(brace int) bool {
	query := scanner.Query
	startsQuery := func() bool {
		return scanner.startsQuery[brace]
	}
	switch {
	case scanner.Last.Kind == 0:
		// The text starts with it: a braced query part, or a block nested
		// in the Body being validated.
		return startsQuery()
	case scanner.Last.Is(query, ")"):
		return scanner.Last.CallParen
	case scanner.Last.Is(query, "{"):
		return scanner.Last.Body && startsQuery()
	case scanner.Last.Kind != Word:
		return false
	}
	for _, word := range QueryBodyContainerKeywords {
		if scanner.keyword(word) {
			return startsQuery()
		}
	}
	for _, word := range [...]string{"UNION", "NEXT", "THEN", "ELSE"} {
		if scanner.keyword(word) {
			return startsQuery()
		}
	}
	if scanner.keyword("ALL") && scanner.Prior.Keyword(query, "UNION") {
		return startsQuery()
	}
	return false
}

// BraceBodyStartsQuery reports whether the body of the '{' at brace starts like
// a Query: a clause, a pattern, or a nested block that does. A clause keyword
// followed by ':', ',' or '}' is a map key or projection selector instead.

// QueryBodyClauseStart reports whether word starts a clause, an updating one
// too: EXISTS { CREATE () } is a Body Neo4j rejects for its update, not a map.
func QueryBodyClauseStart(text string, span Span) bool {
	if KeywordEnd(text, span.Start, "NODETACH", CypherWord) == span.End {
		return true
	}
	for _, keyword := range QueryStructureClauseStarts {
		if KeywordEnd(text, span.Start, keyword, CypherWord) == span.End {
			return true
		}
	}
	return false
}

// BraceEntryKind is the grammar of one comma-separated entry of a brace.
type BraceEntryKind uint8

const (
	InvalidBraceEntry  BraceEntryKind = iota
	KeyedBraceEntry                   // key: value
	PropertyBraceEntry                // .key
	WildcardBraceEntry                // .*
	VariableBraceEntry                // name
)

// BraceEntryAt reads the entry query[start:end].

// ReadBraceEntry reads the entry text[start:] and returns its grammar, its
// key, selected property or variable as written, and a keyed entry's value.

// BraceEntry holds the original accepted-view boundaries of one entry. Value
// includes its trivia; consumers may trim a view without rediscovering syntax.
// A colon records its physical name/value slots even when Kind is Invalid:
// declaration grammars can consume those slots without admitting a map key.
type BraceEntry struct {
	original          *braceEntryIdentity
	separators        EntrySeparators
	Kind              BraceEntryKind
	IssueKind         MapEntryIssueKind
	Span, Name, Value Span
}

type braceEntryIdentity struct {
	source     *Source
	braceOpen  int
	entry      BraceEntry
	separators EntrySeparators
	first      Span
}

// EntrySeparators retains the token identities chosen by the entry owner.
// Colon is empty for a selector/variable entry. Views never rediscover them.
type EntrySeparators struct{ Before, Colon, After Span }

func (entry BraceEntry) SeparatorSpans() EntrySeparators { return entry.separators }

// ReadBraceEntrySpans owns entry grammar and its unevaluated source boundaries.

func braceEntryFromTokens(text string, entry BraceEntry, first, second Token, hasSecond, hasThird bool) BraceEntry {
	end := entry.Span.End
	switch {
	case hasSecond && second.Is(text, ":"):
		entry.Name, entry.Value = Span{first.Start, first.End}, Span{second.End, end}
		if !BraceEntryName(text, first) {
			if hasThird {
				entry.IssueKind = MapKeyInvalid
			}
			return entry
		}
		if !hasThird {
			return entry
		}
		entry.Kind, entry.Name, entry.Value = KeyedBraceEntry, Span{first.Start, first.End}, Span{second.End, end}
		return entry
	case first.Is(text, ".") && hasSecond && !hasThird:
		if second.Is(text, "*") {
			entry.Kind = WildcardBraceEntry
			return entry
		}
		if BraceEntryName(text, second) {
			entry.Kind, entry.Name = PropertyBraceEntry, Span{second.Start, second.End}
			return entry
		}
	case !hasSecond && BraceEntryName(text, first):
		entry.Kind, entry.Name = VariableBraceEntry, Span{first.Start, first.End}
		return entry
	}
	return entry
}

// BraceEntryHasSeparator reports a ':' outside the entry's nested delimiters.

func BraceEntryName(query string, token Token) bool {
	if token.Kind != Word && token.Kind != QuotedName {
		return false
	}
	name, ok := ScanIdentifier(token.Text(query), 0)
	return ok && name.End == token.End-token.Start
}

// ForEachBraceEntry visits the comma-separated entries between a brace at
// open and its close. A brace holding only whitespace and comments has none.
