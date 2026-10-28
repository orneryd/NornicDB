package fabric

import (
	"context"
	"errors"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// UseClause is the graph reference of a USE clause: a database, alias or
// composite constituent name, or a dynamic reference, a function call such
// as graph.byName(…) or graph.byElementId(…) whose arguments are evaluated
// when the statement runs.
type UseClause struct {
	// Name is the static graph name ("db", "cmp.alias"), with backtick
	// quotes removed. Empty for a dynamic reference.
	Name string
	// Function is a dynamic reference's function name as written, with any
	// spaces around its dots removed ("graph.byName").
	Function string
	// Args are a dynamic reference's argument expressions, as written.
	Args []string
}

// IsDynamic reports whether the clause looks its graph up with a function.
func (u UseClause) IsDynamic() bool { return u.Function != "" }

// Text is the graph reference as Neo4j prints it in messages: the name, or
// the function call with its arguments separated by ", " and string
// literals in double quotes (graph.byName("neo4j")).
func (u UseClause) Text() string {
	if !u.IsDynamic() {
		return u.Name
	}
	args := make([]string, len(u.Args))
	for i, arg := range u.Args {
		args[i] = doubleQuoteStringLiterals(arg)
	}
	return u.Function + "(" + strings.Join(args, ", ") + ")"
}

// UseSyntaxError is a USE clause that Neo4j's grammar rejects
// (Neo.ClientError.Statement.SyntaxError), with Neo4j's message.
type UseSyntaxError struct {
	Message localization.Message
}

func (e *UseSyntaxError) Error() string { return e.Message.Fallback }

func useSyntaxError(message localization.Message) error {
	return &UseSyntaxError{Message: message}
}

// UseClauseReference retains source coordinates separately from a routed
// graph's semantic identity, which remains valid after fragment rewriting.
// End is a byte offset in the supplied query at the selected source stage.
type UseClauseReference struct {
	UseClause
	End int
}

// ParseUseClause reads a leading USE clause as Neo4j 5.26's grammar does
// (#738). It is the one USE parser: the Cypher executor, the Fabric planner
// and statement routing all use it. inSubquery is true for the body of a
// CALL { } subquery. It returns the graph reference, the query after it,
// and whether the query starts with USE.
//
//   - USE [GRAPH] <reference>: GRAPH is the optional keyword when a name
//     that is not a clause keyword follows it (USE graph RETURN 1 names the
//     graph "graph").
//   - The reference is a name (parts separated by '.', each a symbolic or
//     backtick-quoted name, spaces and comments allowed around '.'), or a
//     function call, a dynamic reference (graph.byName(…)).
//   - A query clause must follow. USE alone, or followed by EXPLAIN,
//     PROFILE, CYPHER, BEGIN, :USE or any other word, is a SyntaxError.
//   - A second USE is a SyntaxError, and so is an administration command
//     (SHOW USERS, CREATE DATABASE, GRANT, …) after a top-level USE.
//
// Errors are *UseSyntaxError with Neo4j's message. The syntax is checked
// before any graph is looked up, so a bad statement is a SyntaxError even
// when the graph doesn't exist.
func ParseUseClause(ctx context.Context, query string, inSubquery bool, stage SourceStage) (UseClause, string, bool, error) {
	reference, remaining, hasUse, err := ReadUseClause(ctx, query, inSubquery, stage)
	return reference.UseClause, remaining, hasUse, err
}

// ReadUseClause consumes the shared grammar product and applies routing policy.
// RawSource preserves original Args, End and remaining source while names are
// decoded. AcceptedSource retains compiler spellings without another projection.
// The original context governs admission and grammar work, including no-USE input.
func ReadUseClause(ctx context.Context, query string, inSubquery bool, stage SourceStage) (clause UseClauseReference, remaining string, hasUse bool, err error) {
	if err := ctx.Err(); err != nil {
		return UseClauseReference{}, "", false, err
	}
	source := cyphertext.NewSource(ctx, query, stage)
	if source.Err != nil {
		return UseClauseReference{}, "", false, source.Err
	}
	leading := len(source.Text) - len(strings.TrimLeftFunc(source.Text, cyphertext.IsWhitespace))
	trailing := max(leading, len(strings.TrimRightFunc(source.Text, cyphertext.IsWhitespace)))
	view := source.Window(leading, trailing)
	trimmed := view.Text
	if !startsWithWordFold(trimmed, "USE") {
		return UseClauseReference{}, query, false, nil
	}
	reference, readErr := cypherantlr.ReadUseReference(ctx, view)
	if readErr != nil {
		if ctx.Err() != nil {
			return UseClauseReference{}, "", true, ctx.Err()
		}
		var position *cypherantlr.UseReferenceError
		if errors.As(readErr, &position) {
			input := inputToken(trimmed, position.Offset)
			switch {
			case position.Function:
				return UseClauseReference{}, "", true, useSyntaxError(localization.CypherCommandRoutingUseInvalidGraphFunctionArgument(input))
			case position.NamePart:
				return UseClauseReference{}, "", true, useSyntaxError(localization.CypherCommandRoutingUseInvalidGraphNamePart(input))
			default:
				return UseClauseReference{}, "", true, useSyntaxError(localization.CypherCommandRoutingUseInvalidGraphReference(input))
			}
		}
		return UseClauseReference{}, "", true, useSyntaxError(localization.CypherCommandRoutingUseInvalidGraphReference(inputToken(trimmed, skipSpacesAndComments(trimmed, len("USE")))))
	}
	clause = UseClauseReference{UseClause: UseClause{Name: reference.Name, Function: reference.Function, Args: reference.Args}, End: reference.End}
	end := reference.AcceptedEnd

	next := skipSpacesAndComments(trimmed, end)
	if next < len(trimmed) && trimmed[next] == ';' {
		// A ';' ends the statement: after it only whitespace and comments
		// may follow (USE db; is then a USE with no clause); another
		// statement is Neo4j's "Expected exactly one statement per query
		// but got: <n>".
		if more := statementsAfter(trimmed[next:]); more > 0 {
			return UseClauseReference{}, "", true, useSyntaxError(localization.CypherCommandRoutingMultipleStatements(1 + more))
		}
		next = len(trimmed)
	}
	if next >= len(trimmed) {
		if inSubquery {
			return UseClauseReference{}, "", true, useSyntaxError(localization.CypherCommandRoutingUseSubqueryMustConclude())
		}
		return UseClauseReference{}, "", true, useSyntaxError(localization.CypherCommandRoutingUseQueryCannotConclude())
	}
	word, _, ok := scanWord(trimmed, next)
	if !ok || !cypherantlr.IsUseClauseFollower(word, inSubquery) {
		if inSubquery {
			return UseClauseReference{}, "", true, useSyntaxError(localization.CypherCommandRoutingUseInvalidSubqueryClauseAfterGraph(inputToken(trimmed, next)))
		}
		return UseClauseReference{}, "", true, useSyntaxError(localization.CypherCommandRoutingUseInvalidClauseAfterGraph(inputToken(trimmed, next)))
	}
	if strings.EqualFold(word, "USE") {
		return UseClauseReference{}, "", true, useSyntaxError(localization.CypherCommandRoutingUseNotFirstClause())
	}
	if !inSubquery && IsAdministrationCommand(trimmed[next:]) {
		return UseClauseReference{}, "", true, useSyntaxError(localization.CypherCommandRoutingUseAdministrationCommand())
	}
	restStart := end + len(trimmed[end:]) - len(strings.TrimLeftFunc(trimmed[end:], cyphertext.IsWhitespace))
	restEnd := max(restStart, len(strings.TrimRightFunc(trimmed, cyphertext.IsWhitespace)))
	restSpan, _ := view.OriginalSpan(cyphertext.Span{Start: restStart, End: restEnd}, cyphertext.SourceExtent)
	return clause, source.Original[restSpan.Start:restSpan.End], true, nil
}

// IsAdministrationCommand reports whether the statement is one of Neo4j's
// administration commands (database, alias, server, user, role and
// privilege management), which Neo4j routes to the system database and
// refuses after a USE clause. Classified as Neo4j 5.26's grammar does
// (verified per command): SHOW DATABASE[S] / DEFAULT DATABASE / HOME
// DATABASE / SERVER[S] / SUPPORTED PRIVILEGES / POPULATED ROLES / ALL
// ROLES|PRIVILEGES / USER[S] (not USER DEFINED FUNCTIONS) / CURRENT USER /
// ROLE[S] / PRIVILEGE[S] / ALIAS[ES]; CREATE / DROP / ALTER of a DATABASE,
// COMPOSITE DATABASE, ALIAS, USER, ROLE or SERVER (and CREATE OR REPLACE of
// one, ALTER CURRENT USER); GRANT, DENY, REVOKE, RENAME, START, STOP,
// ENABLE, DRYRUN, DEALLOCATE and REALLOCATE.
func IsAdministrationCommand(statement string) bool {
	words := leadingWords(statement, 4)
	if len(words) == 0 {
		return false
	}
	switch words[0] {
	case "GRANT", "DENY", "REVOKE", "RENAME", "START", "STOP", "ENABLE", "DRYRUN", "DEALLOCATE", "REALLOCATE":
		return true
	case "SHOW":
		if len(words) < 2 {
			return false
		}
		switch words[1] {
		case "DATABASE", "DATABASES", "DEFAULT", "HOME", "SERVER", "SERVERS", "SUPPORTED", "POPULATED",
			"USERS", "CURRENT", "ROLE", "ROLES", "PRIVILEGE", "PRIVILEGES", "ALIAS", "ALIASES":
			return true
		case "USER":
			return len(words) < 3 || words[2] != "DEFINED"
		case "ALL":
			return len(words) > 2 && (words[2] == "ROLE" || words[2] == "ROLES" || words[2] == "PRIVILEGE" || words[2] == "PRIVILEGES")
		}
		return false
	case "CREATE", "DROP", "ALTER":
		object := words[1:]
		if words[0] == "CREATE" && len(object) >= 2 && object[0] == "OR" && object[1] == "REPLACE" {
			object = object[2:]
		}
		if len(object) == 0 {
			return false
		}
		switch object[0] {
		case "DATABASE", "COMPOSITE", "ALIAS", "USER", "ROLE", "SERVER":
			return true
		case "CURRENT":
			return words[0] == "ALTER" && len(object) > 1 && object[1] == "USER"
		}
	}
	return false
}

// inputToken is the token Neo4j names in "Invalid input '…'" at i: the run
// of an identifier, an invalid digit/name run ('1abc'), or one rune, else ”
// at the end of the text.
func inputToken(s string, i int) string {
	if i >= len(s) {
		return ""
	}
	if s[i] == '$' {
		return s[i : i+1]
	}
	if name, ok := cyphertext.ScanIdentifier(s, i); ok {
		return s[i:name.End]
	}
	if cyphertext.IsDigit(s[i]) {
		return s[i:cyphertext.NamePartEnd(s, i)]
	}
	return string(cyphertext.RuneAt(s, i))
}

// leadingWords returns up to n leading words of statement, upper-cased,
// skipping spaces and comments between them; it stops at the first token
// that is not a word.
func leadingWords(statement string, n int) []string {
	words := make([]string, 0, n)
	i := skipSpacesAndComments(statement, 0)
	for len(words) < n {
		word, next, ok := scanWord(statement, i)
		if !ok {
			break
		}
		// These products classify reserved grammar words, not identifiers.
		// Unicode case folding must not turn a name into a reserved token.
		for j := 0; j < len(word); j++ {
			if word[j] >= 0x80 {
				return words
			}
		}
		words = append(words, strings.ToUpper(word))
		i = skipSpacesAndComments(statement, next)
	}
	return words
}

// startsWithWordFold reports whether s starts with word (case-insensitive)
// followed by a byte that can't continue a name.
func startsWithWordFold(s, word string) bool {
	return len(s) >= len(word) && strings.EqualFold(s[:len(word)], word) &&
		(len(s) == len(word) || !cyphertext.NamePartAt(s, len(word)))
}

// scanWord reads the unquoted name at i: a letter, '_' or non-ASCII byte,
// then name bytes.
func scanWord(s string, i int) (string, int, bool) {
	name, ok := cyphertext.ScanIdentifier(s, i)
	if !ok || name.Quoted {
		return "", i, false
	}
	return name.Value, name.End, true
}

// statementsAfter counts the statements in rest, text that starts at a
// statement-ending ';': each ';' outside quoted text and comments ends one,
// and a statement is counted when it holds anything but whitespace and
// comments.
func statementsAfter(rest string) int {
	count := 0
	content := false
	for i := 1; i < len(rest); {
		i = skipSpacesAndComments(rest, i)
		if i >= len(rest) {
			break
		}
		if end, quoted := cyphertext.SkipToken(rest, i); quoted {
			content = true
			i = end
			continue
		}
		switch rest[i] {
		case ';':
			if content {
				count++
			}
			content = false
			i++
			continue
		}
		content = true
		i++
	}
	if content {
		count++
	}
	return count
}

// skipSpacesAndComments returns the index of the first byte at or after i
// that is not whitespace or part of a // or /* */ comment.
func skipSpacesAndComments(s string, i int) int {
	end, _ := cyphertext.GapEnd(s, i)
	return end
}

// matchingParen returns the index of the ')' closing the '(' at open,
// skipping string literals (with backslash escapes) and backtick names.
func matchingParen(s string, open int) (int, bool) {
	depth := 0
	for i := open; i < len(s); i++ {
		if end, quoted := cyphertext.SkipToken(s, i); quoted {
			i = end - 1
			continue
		}
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i, true
			}
		}
	}
	return -1, false
}

// doubleQuoteStringLiterals writes an expression's single-quoted string
// literals in double quotes, as Neo4j prints expressions in messages.
func doubleQuoteStringLiterals(expression string) string {
	if !strings.Contains(expression, "'") {
		return expression
	}
	var b strings.Builder
	for i := 0; i < len(expression); i++ {
		c := expression[i]
		if !cyphertext.IsQuote(c) {
			b.WriteByte(c)
			continue
		}
		end, closed := cyphertext.ScanQuotedText(expression, i)
		if !closed {
			b.WriteString(expression[i:])
			break
		}
		if cyphertext.IsSingleQuote(c) {
			body := expression[i+1 : end-1]
			b.WriteByte('"')
			b.WriteString(strings.ReplaceAll(body, `"`, `\"`))
			b.WriteByte('"')
		} else {
			b.WriteString(expression[i:end])
		}
		i = end - 1
	}
	return b.String()
}

func useKeywordAt(query string, at int) bool { return keywordAt(query, at, "USE") }

// splitAtTopLevelUse splits query at the first top-level USE after any other
// text, with indexTopLevelKeyword as the one reader of quote, comment and
// nesting traversal. It returns the original source before and from USE; a
// leading USE stays with parseLeadingUse, and a query without such a USE
// returns empty strings and false.
func splitAtTopLevelUse(query string, isName ClauseKeywordNameReader) (string, string, bool) {
	start := 0
	for {
		idx := indexTopLevelKeyword(query[start:], "USE")
		if idx < 0 {
			return "", "", false
		}
		at := start + idx
		// Leading USE is already handled by parseLeadingUse.
		if strings.TrimSpace(query[:at]) == "" {
			start = at + len("USE")
			continue
		}
		if !clauseKeywordAt(query, at, "USE", isName) {
			start = at + len("USE")
			continue
		}
		return query[:at], query[at:], true
	}
}
