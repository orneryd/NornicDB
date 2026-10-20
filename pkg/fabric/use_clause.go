package fabric

import (
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

func (e *UseSyntaxError) Error() string         { return e.Message.Fallback }
func (e *UseSyntaxError) BoltErrorCode() string { return "Neo.ClientError.Statement.SyntaxError" }

func useSyntaxError(message localization.Message) error {
	return &UseSyntaxError{Message: message}
}

// useClauseFollowers are the words Neo4j's grammar accepts after the graph
// reference of a top-level USE clause, in the order of Neo4j's SyntaxError
// "expected …" list (Neo4j 5.26). START / STOP DATABASE and ENABLE SERVER
// are listed by their first word.
var useClauseFollowers = [...]string{"FOREACH", "ALTER", "ORDER", "CALL", "CREATE", "LOAD", "START", "STOP",
	"DEALLOCATE", "DELETE", "DENY", "DETACH", "DROP", "DRYRUN", "FINISH", "GRANT", "INSERT", "LIMIT", "MATCH",
	"MERGE", "NODETACH", "OFFSET", "OPTIONAL", "REALLOCATE", "REMOVE", "RENAME", "RETURN", "REVOKE", "ENABLE",
	"SET", "SHOW", "SKIP", "TERMINATE", "UNION", "UNWIND", "USE", "WITH"}

// subqueryUseClauseFollowers are the words Neo4j's grammar accepts after
// the graph reference of a USE clause that starts a subquery (CALL { USE …
// }): the query clauses, no administration commands.
var subqueryUseClauseFollowers = [...]string{"FOREACH", "ORDER", "CALL", "CREATE", "LOAD", "DELETE", "DETACH",
	"FINISH", "INSERT", "LIMIT", "MATCH", "MERGE", "NODETACH", "OFFSET", "OPTIONAL", "REMOVE", "RETURN", "SET",
	"SKIP", "UNION", "UNWIND", "USE", "WITH"}

// ParseUseClause reads a leading USE clause as Neo4j 5.26's grammar does
// (#738). It is the one USE parser: the Cypher executor, the Fabric planner
// and statement routing all use it. inSubquery is true for the body of a
// CALL { } subquery. It returns the graph reference, the query after it,
// and whether the query starts with USE.
//
//   - USE [GRAPH] <reference>: the enclosing grammar decides whether GRAPH
//     is the modifier or a graph name, including keyword-spelled names.
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
func ParseUseClause(query string, inSubquery bool) (UseClause, string, bool, error) {
	// This negative lexical filter avoids parsing unrelated ordinary queries.
	// Presence never assigns roles: the complete grammar decides GRAPH/name.
	start := -1
	for token := range queryTokens(query) {
		if !token.word || !strings.EqualFold(token.text, "USE") {
			return UseClause{}, query, false, nil
		}
		start = token.start
		break
	}
	if start < 0 {
		return UseClause{}, query, false, nil
	}
	q := ReadQueryStructure(query)
	if q.parsed && len(q.Clauses) > 0 && q.Clauses[0].Kind == "USE" {
		if err := q.validateUseAt(0, inSubquery); err != nil {
			return UseClause{}, "", true, err
		}
		return *q.Clauses[0].Use, q.Clauses[0].Remaining, true, nil
	}
	// Preserve diagnostics and ordinary extensions outside this grammar. A
	// completed graph reference still comes from the native syntax owner.
	var preset *QueryClause
	if clause := q.First("USE"); clause != nil && clause.Start == start && clause.End > start {
		preset = clause
	}
	return parseUseClauseFallback(query, inSubquery, preset)
}

func parseUseClauseFallback(query string, inSubquery bool, preset *QueryClause) (UseClause, string, bool, error) {
	var clause UseClause
	var end int
	trimmed := query
	if preset != nil {
		// Diagnose a malformed continuation of a grammar-established reference.
		// This cannot choose GRAPH versus name: its start is supplied by AST.
		if _, _, err := scanGraphReference(query, preset.graphStart); err != nil {
			return UseClause{}, "", true, err
		}
		clause = *preset.Use
		end = preset.End
	} else {
		trimmed = strings.TrimSpace(query)
		trimmed = trimmed[skipSpacesAndComments(trimmed, 0):]
		if !startsWithWordFold(trimmed, "USE") {
			return UseClause{}, query, false, nil
		}
		start := skipSpacesAndComments(trimmed, len("USE"))
		if word, next, ok := scanWord(trimmed, start); ok && strings.EqualFold(word, "GRAPH") {
			// GRAPH is the keyword when a name follows it that is not a clause
			// keyword; otherwise it is the graph's name.
			after := skipSpacesAndComments(trimmed, next)
			if after < len(trimmed) && (cyphertext.IsBacktick(trimmed[after]) || isNameStart(trimmed[after])) {
				if follow, _, ok := scanWord(trimmed, after); !ok || !isUseClauseFollower(follow, inSubquery) {
					start = after
				}
			}
		}

		var err error
		clause, end, err = scanGraphReference(trimmed, start)
		if err != nil {
			return UseClause{}, "", true, err
		}

	}
	next := skipSpacesAndComments(trimmed, end)
	if next < len(trimmed) && trimmed[next] == ';' {
		// A ';' ends the statement: after it only whitespace and comments
		// may follow (USE db; is then a USE with no clause); another
		// statement is Neo4j's "Expected exactly one statement per query
		// but got: <n>".
		if more := statementsAfter(trimmed[next:]); more > 0 {
			return UseClause{}, "", true, useSyntaxError(localization.CypherCommandRoutingMultipleStatements(1 + more))
		}
		next = len(trimmed)
	}
	if next >= len(trimmed) {
		if inSubquery {
			return UseClause{}, "", true, useSyntaxError(localization.CypherCommandRoutingUseSubqueryMustConclude())
		}
		return UseClause{}, "", true, useSyntaxError(localization.CypherCommandRoutingUseQueryCannotConclude())
	}
	word, _, ok := scanWord(trimmed, next)
	if !ok || !isUseClauseFollower(word, inSubquery) {
		if inSubquery {
			return UseClause{}, "", true, useSyntaxError(localization.CypherCommandRoutingUseInvalidSubqueryClauseAfterGraph(inputToken(trimmed, next)))
		}
		return UseClause{}, "", true, useSyntaxError(localization.CypherCommandRoutingUseInvalidClauseAfterGraph(inputToken(trimmed, next)))
	}
	if strings.EqualFold(word, "USE") {
		return UseClause{}, "", true, useSyntaxError(localization.CypherCommandRoutingUseNotFirstClause())
	}
	if !inSubquery && IsAdministrationCommand(trimmed[next:]) {
		return UseClause{}, "", true, useSyntaxError(localization.CypherCommandRoutingUseAdministrationCommand())
	}
	return clause, strings.TrimSpace(trimmed[end:]), true, nil
}

// scanGraphReference reads the graph reference at start: a name, or a
// function call (a dynamic reference). It returns the clause and where the
// reference ends.
func scanGraphReference(s string, start int) (UseClause, int, error) {
	var parts []string
	i := start
	quoted := false
	for {
		if i < len(s) && cyphertext.IsBacktick(s[i]) {
			part, next, ok := scanBacktickName(s, i)
			if !ok {
				return UseClause{}, start, useSyntaxError(localization.CypherCommandRoutingUseInvalidGraphReference("`"))
			}
			parts = append(parts, part)
			quoted = true
			i = next
		} else if i < len(s) && isNameStart(s[i]) {
			word, next, _ := scanWord(s, i)
			parts = append(parts, word)
			i = next
		} else if len(parts) == 0 {
			return UseClause{}, start, useSyntaxError(localization.CypherCommandRoutingUseInvalidGraphReference(inputToken(s, i)))
		} else {
			return UseClause{}, start, useSyntaxError(localization.CypherCommandRoutingUseInvalidGraphNamePart(inputToken(s, i)))
		}
		next := skipSpacesAndComments(s, i)
		if next < len(s) && s[next] == '(' && !quoted {
			// A function call: a dynamic graph reference.
			closeIdx, ok := matchingParen(s, next)
			if !ok {
				return UseClause{}, start, useSyntaxError(localization.CypherCommandRoutingUseInvalidGraphFunctionArgument(inputToken(s, len(s))))
			}
			args := splitArguments(s[next+1 : closeIdx])
			return UseClause{Function: strings.Join(parts, "."), Args: args}, closeIdx + 1, nil
		}
		if next >= len(s) || s[next] != '.' {
			return UseClause{Name: strings.Join(parts, ".")}, i, nil
		}
		i = skipSpacesAndComments(s, next+1)
	}
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

// isUseClauseFollower reports whether word may follow a USE clause's graph
// reference (useClauseFollowers, subqueryUseClauseFollowers).
func isUseClauseFollower(word string, inSubquery bool) bool {
	followers := useClauseFollowers[:]
	if inSubquery {
		followers = subqueryUseClauseFollowers[:]
	}
	for _, follower := range followers {
		if strings.EqualFold(word, follower) {
			return true
		}
	}
	return false
}

// inputToken is the token Neo4j names in "Invalid input '…'" at i: the run
// of identifier bytes (digits included: '1abc'), else the one byte, else ”
// at the end of the text.
func inputToken(s string, i int) string {
	if i >= len(s) {
		return ""
	}
	j := i
	for j < len(s) && isNameByte(s[j]) {
		j++
	}
	if j > i {
		return s[i:j]
	}
	return s[i : i+1]
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
		words = append(words, strings.ToUpper(word))
		i = skipSpacesAndComments(statement, next)
	}
	return words
}

// startsWithWordFold reports whether s starts with word (case-insensitive)
// followed by a byte that can't continue a name.
func startsWithWordFold(s, word string) bool {
	return len(s) >= len(word) && strings.EqualFold(s[:len(word)], word) &&
		(len(s) == len(word) || !isNameByte(s[len(word)]))
}

// scanWord reads the unquoted name at i: a letter, '_' or non-ASCII byte,
// then name bytes.
func scanWord(s string, i int) (string, int, bool) {
	if i >= len(s) || !isNameStart(s[i]) {
		return "", i, false
	}
	j := i + 1
	for j < len(s) && isNameByte(s[j]) {
		j++
	}
	return s[i:j], j, true
}

// scanBacktickName reads the backtick-quoted name at start (“ escapes a
// backtick) and returns it without the quotes and where it ends.
func scanBacktickName(s string, start int) (string, int, bool) {
	end, closed := cyphertext.ScanQuotedText(s, start)
	if !closed || !cyphertext.IsBacktick(s[start]) {
		return "", start, false
	}
	return strings.ReplaceAll(s[start+1:end-1], "``", "`"), end, true
}

// isNameStart reports whether b starts an unquoted name: a letter, '_' or a
// non-ASCII byte (a digit does not: USE 1abc is a SyntaxError).
func isNameStart(b byte) bool {
	return b >= 0x80 || b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func isNameByte(b byte) bool {
	return isNameStart(b) || (b >= '0' && b <= '9')
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
	for i < len(s) {
		switch {
		case isWhitespace(s[i]):
			i++
		case s[i] == '/' && i+1 < len(s) && s[i+1] == '/':
			end := strings.IndexByte(s[i:], '\n')
			if end < 0 {
				return len(s)
			}
			i += end + 1
		case s[i] == '/' && i+1 < len(s) && s[i+1] == '*':
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				return len(s)
			}
			i += 2 + end + 2
		default:
			return i
		}
	}
	return i
}

// matchingParen returns the index of the ')' closing the '(' at open,
// skipping string literals (with backslash escapes) and backtick names.
func matchingParen(s string, open int) (int, bool) {
	depth := 0
	for token := range queryTokens(s[open:]) {
		switch token.text {
		case "(":
			depth++
		case ")":
			depth--
			if depth == 0 {
				return open + token.start, true
			}
		}
	}
	return -1, false
}

// splitArguments splits a function call's argument text at top-level
// commas and trims each argument.
func splitArguments(text string) []string {
	if strings.TrimSpace(text) == "" {
		return nil
	}
	parts := splitTopLevelCSV(text)
	for index := range parts {
		parts[index] = strings.TrimSpace(parts[index])
	}
	return parts
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
