package cypher

import (
	"context"
	"strings"
)

// fineGrainedAuthRuleCommand is a parsed administrative authorization-rule
// statement of Cypher 25 (Neo4j EE's auth rules): the CREATE, ALTER, RENAME and
// DROP AUTH RULE forms, the role assignment forms
// GRANT ROLE[S] … TO AUTH RULE[S] … and REVOKE ROLE[S] … FROM AUTH RULE[S] …,
// and the SHOW forms that list auth rules.
//
// The reader reads syntax only. It never evaluates a condition, resolves a
// name, or writes a rule.
//
// Verb is canonical: "create", "alter", "rename", "drop", "grant", "revoke" or
// "show".
//
// Name is the rule a statement addresses, decoded (a backtick-quoted name loses
// its delimiters, and a doubled backtick stands for one) and with its case
// kept; NewName is the target name of RENAME.
//
// Condition is the text of the SET CONDITION expression exactly as written,
// without its framing whitespace. It is never parsed: a SET, WHERE or AND
// inside quoted text or a nested (), [] or {} group belongs to the expression.
//
// Enabled is the value of SET ENABLED, nil when the statement writes no
// SET ENABLED clause. IfExists, IfNotExists and Replace report the statement's
// IF EXISTS, IF NOT EXISTS and OR REPLACE markers.
//
// Roles and Names carry the role list and the rule list of a GRANT or REVOKE,
// with their quoting decoded. Names is empty for every other verb, which names
// at most one rule in Name.
//
// AsCommands reports AS COMMAND[S]. WithRoles reports the
// SHOW [ALL | POPULATED] ROLE[S] WITH AUTH RULE[S] form rather than
// SHOW AUTH RULE[S].
type fineGrainedAuthRuleCommand struct {
	Verb        string
	Name        string
	NewName     string
	Condition   string
	Enabled     *bool
	IfExists    bool
	IfNotExists bool
	Replace     bool
	AsCommands  bool
	WithRoles   bool
	Names       []string
	Roles       []string
}

// Verb values of fineGrainedAuthRuleCommand.Verb.
const (
	fineGrainedAuthRuleCreateVerb = "create"
	fineGrainedAuthRuleAlterVerb  = "alter"
	fineGrainedAuthRuleRenameVerb = "rename"
	fineGrainedAuthRuleDropVerb   = "drop"
	fineGrainedAuthRuleShowVerb   = "show"

	// A role assignment shares the privilege reader's canonical grant and revoke
	// verbs, so fineGrainedPrivilegeRolesKeyword names its target keyword: a
	// grant writes TO and a revoke writes FROM.
	fineGrainedAuthRuleGrantVerb  = fineGrainedGrantVerb
	fineGrainedAuthRuleRevokeVerb = fineGrainedRevokeVerb
)

// parseFineGrainedAuthRuleCommand parses the administrative authorization-rule
// statements:
//
//	CREATE [OR REPLACE] AUTH RULE[S] ruleName [IF NOT EXISTS]
//	    SET CONDITION conditionExpression [SET ENABLED {true | false}]
//	ALTER AUTH RULE[S] ruleName [IF EXISTS]
//	    [SET CONDITION conditionExpression] [SET ENABLED {true | false}]
//	RENAME AUTH RULE[S] ruleName [IF EXISTS] TO newRuleName
//	DROP AUTH RULE[S] ruleName [IF EXISTS]
//	{GRANT ROLE[S] role[, …] TO | REVOKE ROLE[S] role[, …] FROM}
//	    AUTH RULE[S] ruleName[, …]
//	SHOW AUTH RULE[S] [AS COMMAND[S]]
//	SHOW [ALL | POPULATED] ROLE[S] WITH AUTH RULE[S] [AS COMMAND[S]]
//
// ok is false for a statement of another reader: role assignment to an account
// or a role (GRANT ROLE reader TO alice, REVOKE ROLE reader FROM bob), a
// privilege statement, a SHOW of roles or privileges, and every other Cypher
// statement. A statement is claimed only once its own head is read, so the
// statements these fields cannot hold keep their own readers. err is a claimed
// authorization-rule statement that is malformed, and the whole statement must
// be consumed: text after the statement's own words is an error, not ignored.
// A SHOW statement is the exception, since the SHOW command owns its YIELD,
// WHERE, ORDER BY and LIMIT tail.
func parseFineGrainedAuthRuleCommand(ctx context.Context, text string) (fineGrainedAuthRuleCommand, bool, error) {
	var command fineGrainedAuthRuleCommand
	if err := ctx.Err(); err != nil {
		return command, false, err
	}
	cursor := fineGrainedPrivilegeCursor{ctx: ctx, text: text}
	cursor.skipGap()
	verb, ok := cursor.readAuthRuleVerb()
	if !ok {
		return command, false, nil
	}
	command.Verb = verb
	command, ok, err := parseFineGrainedAuthRuleClause(cursor, command)
	if err != nil && ctx.Err() != nil {
		// An interrupted structural reader has no complete statement verdict.
		return command, false, err
	}
	return command, ok, err
}

// fineGrainedAuthRuleSyntaxError is the deterministic syntax error of a
// malformed authorization-rule statement.
func fineGrainedAuthRuleSyntaxError(detail string) error {
	return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax",
		"Invalid auth rule statement: "+detail)
}

// readAuthRuleVerb reads the statement's leading verb and returns its canonical
// value.
func (c *fineGrainedPrivilegeCursor) readAuthRuleVerb() (string, bool) {
	switch {
	case c.takeKeyword("CREATE"):
		return fineGrainedAuthRuleCreateVerb, true
	case c.takeKeyword("ALTER"):
		return fineGrainedAuthRuleAlterVerb, true
	case c.takeKeyword("RENAME"):
		return fineGrainedAuthRuleRenameVerb, true
	case c.takeKeyword("DROP"):
		return fineGrainedAuthRuleDropVerb, true
	case c.takeKeyword("GRANT"):
		return fineGrainedAuthRuleGrantVerb, true
	case c.takeKeyword("REVOKE"):
		return fineGrainedAuthRuleRevokeVerb, true
	case c.takeKeyword("SHOW"):
		return fineGrainedAuthRuleShowVerb, true
	}
	return "", false
}

// parseFineGrainedAuthRuleClause reads the statement after its verb.
func parseFineGrainedAuthRuleClause(cursor fineGrainedPrivilegeCursor, command fineGrainedAuthRuleCommand) (fineGrainedAuthRuleCommand, bool, error) {
	switch command.Verb {
	case fineGrainedAuthRuleGrantVerb, fineGrainedAuthRuleRevokeVerb:
		return parseFineGrainedAuthRuleRoleAssignment(cursor, command)
	case fineGrainedAuthRuleShowVerb:
		return parseFineGrainedAuthRuleShow(cursor, command)
	}
	probe := cursor
	if probe.takeKeyword("OR") && probe.takeKeyword("REPLACE") {
		cursor, command.Replace = probe, true
	}
	if !cursor.takeAuthRuleHead() {
		return fineGrainedAuthRuleCommand{}, false, nil
	}
	if command.Replace && command.Verb != fineGrainedAuthRuleCreateVerb {
		return command, true, fineGrainedAuthRuleSyntaxError("OR REPLACE requires CREATE")
	}
	name, ok := cursor.readName()
	if !ok {
		return command, true, fineGrainedAuthRuleSyntaxError("expected an auth rule name")
	}
	cursor.skipGap()
	command.Name = name
	switch command.Verb {
	case fineGrainedAuthRuleCreateVerb:
		command.IfNotExists = cursor.takeIfNotExists()
		if command.Replace && command.IfNotExists {
			return command, true, fineGrainedAuthRuleSyntaxError("OR REPLACE and IF NOT EXISTS cannot be combined")
		}
		condition, enabled, err := cursor.readAuthRuleSetClauses(true)
		command.Condition, command.Enabled = condition, enabled
		if err != nil {
			return command, true, err
		}
	case fineGrainedAuthRuleAlterVerb:
		command.IfExists = cursor.takeIfExists()
		condition, enabled, err := cursor.readAuthRuleSetClauses(false)
		command.Condition, command.Enabled = condition, enabled
		if err != nil {
			return command, true, err
		}
	case fineGrainedAuthRuleRenameVerb:
		command.IfExists = cursor.takeIfExists()
		if !cursor.takeKeyword("TO") {
			return command, true, fineGrainedAuthRuleSyntaxError("expected TO followed by a new auth rule name")
		}
		newName, ok := cursor.readName()
		if !ok {
			return command, true, fineGrainedAuthRuleSyntaxError("expected a new auth rule name")
		}
		cursor.skipGap()
		command.NewName = newName
	case fineGrainedAuthRuleDropVerb:
		command.IfExists = cursor.takeIfExists()
	}
	return command, true, cursor.endAuthRuleStatement("unexpected text after the auth rule command")
}

// parseFineGrainedAuthRuleRoleAssignment parses
// {GRANT ROLE[S] role[, …] TO | REVOKE ROLE[S] role[, …] FROM} AUTH RULE[S]
// ruleName[, …]. The statement is claimed at its AUTH RULE[S] head: GRANT ROLE
// reader TO alice assigns a role to an account and AUTH is then an ordinary
// name, so that statement stays with the role assignment reader.
func parseFineGrainedAuthRuleRoleAssignment(cursor fineGrainedPrivilegeCursor, command fineGrainedAuthRuleCommand) (fineGrainedAuthRuleCommand, bool, error) {
	probe := cursor
	if !probe.takeKeyword("ROLE") && !probe.takeKeyword("ROLES") {
		return fineGrainedAuthRuleCommand{}, false, nil
	}
	roles, err := probe.readRoleList()
	if err != nil {
		return fineGrainedAuthRuleCommand{}, false, nil
	}
	if !probe.takeKeyword(fineGrainedPrivilegeRolesKeyword(command.Verb)) || !probe.takeAuthRuleHead() {
		return fineGrainedAuthRuleCommand{}, false, nil
	}
	command.Roles = roles
	names, err := probe.readNameList(false, "expected an auth rule name", "")
	if err != nil {
		return command, true, err
	}
	command.Names = names
	return command, true, probe.endAuthRuleStatement("unexpected text after the auth rules")
}

// parseFineGrainedAuthRuleShow parses the SHOW forms that list auth rules:
//
//	SHOW AUTH RULE[S] [AS COMMAND[S]]
//	SHOW [ALL | POPULATED] ROLE[S] WITH AUTH RULE[S] [AS COMMAND[S]]
//
// The rest of a SHOW statement is the SHOW command's own, so the reader stops
// after the head and the AS COMMANDS marker: a YIELD, WHERE, ORDER BY or LIMIT
// tail is read where SHOW statements are read.
func parseFineGrainedAuthRuleShow(cursor fineGrainedPrivilegeCursor, command fineGrainedAuthRuleCommand) (fineGrainedAuthRuleCommand, bool, error) {
	if cursor.takeAuthRuleHead() {
		command.AsCommands = cursor.takeShowAsCommands()
		return command, true, nil
	}
	probe := cursor
	if !probe.takeKeyword("ALL") {
		probe.takeKeyword("POPULATED")
	}
	if !probe.takeKeyword("ROLE") && !probe.takeKeyword("ROLES") {
		return fineGrainedAuthRuleCommand{}, false, nil
	}
	if !probe.takeKeyword("WITH") || !probe.takeAuthRuleHead() {
		return fineGrainedAuthRuleCommand{}, false, nil
	}
	command.WithRoles = true
	command.AsCommands = probe.takeShowAsCommands()
	return command, true, nil
}

// takeAuthRuleHead consumes AUTH RULE or AUTH RULES together with the trivia
// after it, and reports whether it stood there. AUTH alone is an ordinary name,
// so a head is claimed only with its RULE word.
func (c *fineGrainedPrivilegeCursor) takeAuthRuleHead() bool {
	probe := *c
	if !probe.takeKeyword("AUTH") {
		return false
	}
	if !probe.takeKeyword("RULE") && !probe.takeKeyword("RULES") {
		return false
	}
	c.at = probe.at
	return true
}

// takeShowAsCommands consumes AS COMMAND or AS COMMANDS with the trivia after
// it.
func (c *fineGrainedPrivilegeCursor) takeShowAsCommands() bool {
	probe := *c
	if !probe.takeKeyword("AS") {
		return false
	}
	if !probe.takeKeyword("COMMAND") && !probe.takeKeyword("COMMANDS") {
		return false
	}
	c.at = probe.at
	return true
}

// takeIfExists consumes IF EXISTS with the trivia after it.
func (c *fineGrainedPrivilegeCursor) takeIfExists() bool {
	probe := *c
	if !probe.takeKeyword("IF") || !probe.takeKeyword("EXISTS") {
		return false
	}
	c.at = probe.at
	return true
}

// takeIfNotExists consumes IF NOT EXISTS with the trivia after it.
func (c *fineGrainedPrivilegeCursor) takeIfNotExists() bool {
	probe := *c
	if !probe.takeKeyword("IF") || !probe.takeKeyword("NOT") || !probe.takeKeyword("EXISTS") {
		return false
	}
	c.at = probe.at
	return true
}

// readAuthRuleSetClauses reads the SET CONDITION and SET ENABLED clauses that
// follow a rule name: SET CONDITION conditionExpression [SET ENABLED
// {true | false}], where CREATE requires the condition and ALTER makes each
// clause optional but requires at least one.
func (c *fineGrainedPrivilegeCursor) readAuthRuleSetClauses(requireCondition bool) (string, *bool, error) {
	var condition string
	if probe := *c; probe.takeKeyword("SET") && probe.takeKeyword("CONDITION") {
		expression, err := probe.readAuthRuleCondition()
		if err != nil {
			return "", nil, err
		}
		c.at, condition = probe.at, expression
	} else if requireCondition {
		return "", nil, fineGrainedAuthRuleSyntaxError("expected SET CONDITION")
	}
	var enabled *bool
	if probe := *c; probe.takeKeyword("SET") && probe.takeKeyword("ENABLED") {
		value, err := probe.readAuthRuleEnabled()
		if err != nil {
			return "", nil, err
		}
		c.at, enabled = probe.at, value
	}
	if condition == "" && enabled == nil {
		return "", nil, fineGrainedAuthRuleSyntaxError("expected SET CONDITION or SET ENABLED")
	}
	return condition, enabled, nil
}

// readAuthRuleEnabled reads the true or false of a SET ENABLED clause.
func (c *fineGrainedPrivilegeCursor) readAuthRuleEnabled() (*bool, error) {
	value := false
	switch {
	case c.takeKeyword("TRUE"):
		value = true
	case c.takeKeyword("FALSE"):
	default:
		return nil, fineGrainedAuthRuleSyntaxError("expected true or false after SET ENABLED")
	}
	return &value, nil
}

// readAuthRuleCondition reads the condition expression after SET CONDITION and
// returns its text as written, without its framing whitespace. The expression
// is Cypher, so the reader does not parse it: it runs to the end of the
// statement, or to the SET clause that follows it, and the established
// top-level lexer keeps a SET, WHERE or AND inside quoted text, a comment or a
// nested (), [] or {} group out of that boundary.
func (c *fineGrainedPrivilegeCursor) readAuthRuleCondition() (string, error) {
	start := c.at
	end := c.authRuleClauseIndex(start)
	if end < 0 {
		end = len(c.text)
		if terminator := findTopLevelOperator(c.text[start:end], ";", false, true); terminator >= 0 {
			end = start + terminator
		}
	}
	if err := c.ctx.Err(); err != nil {
		return "", err
	}
	expression := strings.TrimSpace(c.text[start:end])
	if expression == "" {
		return "", fineGrainedAuthRuleSyntaxError("expected a condition expression")
	}
	c.at = end
	return expression, nil
}

// authRuleClauseIndex is the position of the next SET keyword at the top level
// of the statement at or after from whose clause is SET CONDITION or
// SET ENABLED, or -1. The condition is kept as text, so its expression is not
// parsed: a SET standing in it is a candidate too, and only the candidate that
// opens a clause is the statement's own.
func (c *fineGrainedPrivilegeCursor) authRuleClauseIndex(from int) int {
	opts := keywordScanOpts{
		SkipParens:    true,
		SkipBrackets:  true,
		SkipBraces:    true,
		SkipStrings:   true,
		SkipBackticks: true,
		SkipComments:  true,
		Boundary:      keywordBoundaryWord,
	}
	for pos := keywordIndexFrom(c.ctx, c.text, "SET", from, opts); pos >= 0; pos = keywordIndexFrom(c.ctx, c.text, "SET", pos+1, opts) {
		if c.ctx.Err() != nil {
			return -1
		}
		probe := fineGrainedPrivilegeCursor{ctx: c.ctx, text: c.text, at: keywordEndAt(c.text, pos, "SET")}
		probe.skipGap()
		if probe.wordAt("CONDITION") || probe.wordAt("ENABLED") {
			return pos
		}
	}
	return -1
}

// endAuthRuleStatement accepts the statement's optional terminator and requires
// the statement to end, with detail as the diagnostic for text after it.
func (c *fineGrainedPrivilegeCursor) endAuthRuleStatement(detail string) error {
	if c.at < len(c.text) && c.text[c.at] == ';' {
		c.at++
		c.skipGap()
	}
	if c.at != len(c.text) {
		return fineGrainedAuthRuleSyntaxError(detail)
	}
	return nil
}
