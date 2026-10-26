package cypher

import "github.com/orneryd/nornicdb/internal/cyphertext"

// braceFactScanner traverses lexical tokens and reads retained facts. It owns
// no body/role/entry grammar and never reconstructs context from fragment text.
type braceFactScanner struct {
	view                 braceView
	Query                string
	reader               cyphertext.Reader
	Last, Prior, Earlier cyphertext.BraceToken
}

func newBraceFactScanner(view braceView, start int) braceFactScanner {
	scanner := braceFactScanner{view: view, Query: view.text(), reader: cyphertext.NewReader(view.text(), 0)}
	for start > 0 {
		lookahead := scanner.reader
		if token, ok := lookahead.Next(); !ok || token.Start >= start {
			break
		}
		scanner.next()
	}
	return scanner
}

func (scanner *braceFactScanner) next() (cyphertext.Token, bool) {
	token, ok := scanner.reader.Next()
	if !ok {
		return token, false
	}
	info := cyphertext.BraceToken{Token: token, ParenOpen: -1}
	if token.Is(scanner.Query, "{") {
		fact, found := scanner.view.at(token.Start)
		if !found {
			panic("cypher: live brace has no retained original or construction fact")
		}
		info.Body = fact.kind == subqueryBrace
	}
	scanner.Earlier, scanner.Prior, scanner.Last = scanner.Prior, scanner.Last, info
	return token, true
}

func (scanner *braceFactScanner) at(opening int) bool {
	if opening < 0 || opening >= len(scanner.Query) || !cyphertext.IsOpenBrace(scanner.Query[opening]) {
		return false
	}
	if scanner.Last.Kind != 0 && scanner.Last.Start > opening {
		*scanner = newBraceFactScanner(scanner.view, 0)
	}
	if scanner.Last.Kind != 0 && scanner.Last.Start == opening {
		return scanner.Last.Body
	}
	if scanner.Last.End > opening {
		return false
	}
	for token, ok := scanner.next(); ok && token.Start <= opening; token, ok = scanner.next() {
		if token.Start == opening {
			return scanner.Last.Body
		}
	}
	return false
}

func (scanner *braceFactScanner) kind() mapBraceKind {
	fact, found := scanner.view.at(scanner.Last.Start)
	if !found {
		panic("cypher: brace role requested without a retained fact")
	}
	return fact.kind
}

func (scanner *braceFactScanner) skipBody(close int) {
	scanner.reader.Seek(close + 1)
	scanner.Earlier, scanner.Prior = scanner.Prior, scanner.Last
	scanner.Last = cyphertext.BraceToken{Token: cyphertext.Token{Start: close, End: close + 1, Kind: cyphertext.Symbol}, ParenOpen: -1}
}
