package cyphertext

import (
	"sort"
)

// BraceFacts is the immutable lexical part of a statement's brace product.
// It is produced once from the original accepted Source. Views project these
// records through Source; they never classify transformed text again.
type BraceFacts struct {
	source              *Source
	scopes              []BraceScope
	braces              []BraceRecord
	byOpen              map[int]int
	byOriginal          map[Span]int
	delimiterByOriginal map[Span]int
	closes              map[int]int
	unmatched           []Span
	closedStrings       map[int]bool
	opaque              []Span
}

// BraceScope is a token stream in original accepted coordinates. The root is
// one stream; each interpolation expression is another. String tokens remain
// opaque in their containing stream. Slice accessors return copies.
type BraceScope struct {
	Span   Span
	tokens []BraceToken
}

func (scope BraceScope) Tokens() []BraceToken { return append([]BraceToken(nil), scope.tokens...) }

// BraceRecord retains a live brace, its body role and entry grammar. Contextual
// pattern/projection roles are resolved by the statement grammar using this
// same record. An unmatched opening has Close == -1.
type BraceRecord struct {
	source      *Source
	Open, Close int
	Body        bool
	Scope       int
	entries     []BraceEntry
	Separator   bool
}

func (record BraceRecord) Entries() []BraceEntry { return append([]BraceEntry(nil), record.entries...) }

func (facts *BraceFacts) Source() *Source            { return facts.source }
func (facts *BraceFacts) Scopes() []BraceScope       { return append([]BraceScope(nil), facts.scopes...) }
func (facts *BraceFacts) Braces() []BraceRecord      { return append([]BraceRecord(nil), facts.braces...) }
func (facts *BraceFacts) Unmatched() []Span          { return append([]Span(nil), facts.unmatched...) }
func (facts *BraceFacts) OpaqueSpans() []Span        { return append([]Span(nil), facts.opaque...) }
func (facts *BraceFacts) StringClosed(open int) bool { return facts.closedStrings[open] }
func (facts *BraceFacts) Close(open int) (int, bool) {
	close, ok := facts.closes[open]
	return close, ok
}
func (facts *BraceFacts) At(open int) (BraceRecord, bool) {
	index, ok := facts.byOpen[open]
	if !ok {
		return BraceRecord{}, false
	}
	return facts.braces[index], true
}

// AtView selects the original record of this exact occurrence. Generated
// braces have no original record; their producer supplies construction facts.
func (facts *BraceFacts) AtView(view *Source, open int) (BraceRecord, bool) {
	if open < 0 || open >= len(view.Text) {
		return BraceRecord{}, false
	}
	origin := view.Origin(Span{open, open + 1})
	if origin.Generated {
		return BraceRecord{}, false
	}
	index, ok := facts.byOriginal[origin.Original]
	if !ok {
		return BraceRecord{}, false
	}
	return facts.braces[index], true
}

// EntryAtView selects an original entry by its exact name/selector token
// origin. Generated names require a producer declaration instead.
func (facts *BraceFacts) EntryAtView(view *Source, name Span) (BraceEntry, bool) {
	origin := view.Origin(name)
	if origin.Generated {
		return BraceEntry{}, false
	}
	for _, record := range facts.braces {
		for _, entry := range record.entries {
			original, valid := facts.source.OriginalSpan(entry.Name, SourceExtent)
			if entry.Name.End > entry.Name.Start && valid && original == origin.Original {
				return entry, true
			}
		}
	}
	return BraceEntry{}, false
}

// BraceFacts returns the original accepted source's one lexical record, even
// when called on a window or rewrite. ProjectFrom/Origin are the only mapping
// owner; callers use their explicit Source view to select an occurrence.
func (source *Source) BraceFacts() *BraceFacts {
	source = source.OriginalView()
	source.braces.lexicalOnce.Do(func() {
		facts := &BraceFacts{source: source, byOpen: make(map[int]int), byOriginal: make(map[Span]int), delimiterByOriginal: make(map[Span]int), closes: make(map[int]int), closedStrings: make(map[int]bool)}
		facts.readScope(0, false)
		source.braces.lexical = facts
	})
	return source.braces.lexical
}

// InterpolationQuote identifies the existing s"..." lexical extension. It is
// opt-in for brace fact production; ordinary Reader behaviour is unchanged.
func InterpolationQuote(text string, quote int) bool {
	return quote > 0 && IsStringQuote(text[quote]) && text[quote-1]|0x20 == 's' &&
		(quote == 1 || !NamePartAt(text, quote-2) && text[quote-2] != '.' && text[quote-2] != '$')
}

// readScope tokenizes through the existing Reader. The quotation owner calls
// back at embedded-expression boundaries; that expression is read here using
// the same Reader/alphabet, rather than being rediscovered after rewriting.
func (facts *BraceFacts) readScope(start int, embedded bool) int {
	text := facts.source.Text
	scopeIndex := len(facts.scopes)
	facts.scopes = append(facts.scopes, BraceScope{Span: Span{Start: start, End: len(text)}})
	reader := NewReader(text, start)
	reader.opaque = func(span Span) { facts.opaque = append(facts.opaque, span) }
	reader.embeddedEnd = func(open int) int { return facts.readScope(open+1, true) }
	var tokens []Token
	depth, end := 0, len(text)
	for token, ok := reader.Next(); ok; token, ok = reader.Next() {
		if token.Kind == String {
			facts.closedStrings[token.Start] = reader.tokenClosed
		}
		if token.Is(text, "}") && embedded && depth == 0 {
			end = token.Start
			break
		}
		tokens = append(tokens, token)
		if token.Is(text, "{") {
			depth++
		}
		if token.Is(text, "}") {
			depth--
		}
	}
	facts.scopes[scopeIndex].Span.End = end
	starts := make(map[int]bool)
	for index := len(tokens) - 1; index >= 0; index-- {
		if !tokens[index].Is(text, "{") || index+1 == len(tokens) {
			continue
		}
		first := tokens[index+1]
		switch {
		case first.Is(text, "("):
			starts[tokens[index].Start] = true
		case first.Is(text, "{"):
			starts[tokens[index].Start] = starts[first.Start]
		case first.Kind == Word:
			if index+2 >= len(tokens) {
				starts[tokens[index].Start] = QueryBodyClauseStart(text, Span{Start: first.Start, End: first.End})
				continue
			}
			next := tokens[index+2]
			if next.Is(text, "=") && BraceEntryName(text, first) && index+3 < len(tokens) {
				starts[tokens[index].Start] = tokens[index+3].Is(text, "(")
			} else if !next.Is(text, ":") && !next.Is(text, ",") && !next.Is(text, "}") {
				starts[tokens[index].Start] = QueryBodyClauseStart(text, Span{Start: first.Start, End: first.End})
			}
		}
	}
	scanner := originalBraceScanner{Query: text, tokens: tokens, startsQuery: starts}
	if !embedded {
		scanner.tokenIndex = braceStatementTokenStart(text, tokens, starts)
	}
	var stack []Token
	var records []int
	for token, ok := scanner.next(); ok; token, ok = scanner.next() {
		facts.scopes[scopeIndex].tokens = append(facts.scopes[scopeIndex].tokens, scanner.Last)
		if token.Kind != Symbol {
			continue
		}
		switch text[token.Start] {
		case '(', '[', '{':
			stack = append(stack, token)
			original, _ := facts.source.OriginalSpan(Span{token.Start, token.End}, SourceExtent)
			facts.delimiterByOriginal[original] = token.Start
			if token.Is(text, "{") {
				facts.byOpen[token.Start] = len(facts.braces)
				facts.byOriginal[original] = len(facts.braces)
				records = append(records, len(facts.braces))
				facts.braces = append(facts.braces, BraceRecord{source: facts.source, Open: token.Start, Close: -1, Body: scanner.Last.Body, Scope: scopeIndex})
			}
		case ')', ']', '}':
			if len(stack) == 0 {
				if token.Is(text, "}") {
					facts.unmatched = append(facts.unmatched, Span{token.Start, token.End})
				}
				continue
			}
			open := stack[len(stack)-1]
			if !matchingBraceDelimiter(text[open.Start], text[token.Start]) {
				if token.Is(text, "}") {
					facts.unmatched = append(facts.unmatched, Span{token.Start, token.End})
				}
				continue
			}
			stack = stack[:len(stack)-1]
			facts.closes[open.Start] = token.Start
			if open.Is(text, "{") {
				facts.braces[facts.byOpen[open.Start]].Close = token.Start
			}
		}
	}
	for _, token := range stack {
		if token.Is(text, "{") {
			facts.unmatched = append(facts.unmatched, Span{token.Start, token.End})
		}
	}
	for _, index := range records {
		facts.readEntries(index, tokens)
	}
	if embedded && end == len(text) {
		facts.unmatched = append(facts.unmatched, Span{start - 1, start})
		return -1
	}
	return end
}

// Prefix handling is part of the original statement grammar. Its token
// population is the same one already read above; no rewritten prefix view is
// admitted again. Expression scopes do not apply statement-prefix grammar.
func braceStatementTokenStart(text string, tokens []Token, starts map[int]bool) int {
	if len(tokens) == 0 || tokens[0].Kind != Word {
		return 0
	}
	first := tokens[0]
	if !first.Keyword(text, "CYPHER") && !first.Keyword(text, "EXPLAIN") && !first.Keyword(text, "PROFILE") {
		return 0
	}
	for index := 1; index < len(tokens); index++ {
		token := tokens[index]
		if token.Is(text, "{") {
			if starts[token.Start] {
				return index
			}
			return 0
		}
		if token.Kind == Word && QueryBodyClauseStart(text, Span{Start: token.Start, End: token.End}) {
			return index
		}
	}
	return 0
}

func matchingBraceDelimiter(open, close byte) bool {
	return open == '(' && close == ')' || open == '[' && close == ']' || open == '{' && close == '}'
}

func (facts *BraceFacts) readEntries(index int, tokens []Token) {
	record := &facts.braces[index]
	if record.Close < 0 {
		return
	}
	text := facts.source.Text
	first := sort.Search(len(tokens), func(index int) bool { return tokens[index].Start > record.Open })
	start, entryToken := record.Open+1, first
	appendEntry := func(end, tokenEnd int) {
		entry := BraceEntry{Span: Span{start, end}, IssueKind: MapEntryInvalid}
		if entryToken == tokenEnd {
			entry.IssueKind = MapEntryEmpty
		} else {
			second := Token{}
			if entryToken+1 < tokenEnd {
				second = tokens[entryToken+1]
			}
			entry = braceEntryFromTokens(text, entry, tokens[entryToken], second, entryToken+1 < tokenEnd, entryToken+2 < tokenEnd)
		}
		entry.separators = EntrySeparators{Before: Span{start - 1, start}, After: Span{end, end + 1}}
		if entry.Name.End > entry.Name.Start && entry.Value.Start > entry.Name.End {
			token := tokens[entryToken+1]
			entry.separators.Colon = Span{token.Start, token.End}
		}
		identity := &braceEntryIdentity{source: facts.source, braceOpen: record.Open, entry: entry, separators: entry.separators}
		if entryToken < tokenEnd {
			identity.first = Span{tokens[entryToken].Start, tokens[entryToken].End}
		}
		entry.original = identity
		record.entries = append(record.entries, entry)
	}
	for cursor := first; cursor < len(tokens) && tokens[cursor].Start < record.Close; cursor++ {
		token := tokens[cursor]
		if token.Kind != Symbol {
			continue
		}
		switch text[token.Start] {
		case '(', '[', '{':
			if close, ok := facts.closes[token.Start]; ok {
				cursor = sort.Search(len(tokens), func(index int) bool { return tokens[index].Start >= close })
			}
		case ':':
			record.Separator = true
		case ',':
			appendEntry(token.Start, cursor)
			start, entryToken = token.End, cursor+1
		}
	}
	last := sort.Search(len(tokens), func(index int) bool { return tokens[index].Start >= record.Close })
	if first < last {
		appendEntry(record.Close, last)
	}
}
