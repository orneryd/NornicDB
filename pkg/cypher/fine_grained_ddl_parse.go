package cypher

import (
	"context"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// fineGrainedPrivilegeCommand is a parsed GRANT, DENY or REVOKE statement of
// Neo4j 5.26's fine-grained privilege model: a graph privilege (ON GRAPH or
// ON HOME GRAPH with an element scope), a database privilege (ON DATABASE or
// ON HOME DATABASE) or a DBMS privilege (ON DBMS).
//
// Verb is "grant", "deny", "revoke", "revoke_grant" (REVOKE GRANT …) or
// "revoke_deny" (REVOKE DENY …). Immutable reports IMMUTABLE.
//
// Privilege is the action's canonical upper-case phrase: "READ", "SET LABEL",
// "CREATE NEW LABEL", "ALL DBMS PRIVILEGES". An abbreviation Neo4j accepts is
// completed to that phrase, so INDEX, NAME and ALL PRIVILEGES read as
// "INDEX MANAGEMENT", "NAME MANAGEMENT" and the ALL phrase of their scope.
//
// Graphs holds the graph names of an ON GRAPH scope or the database names of
// an ON DATABASE scope, exactly as written and with their quoting removed;
// the wildcard stays "*". HOME GRAPH and HOME DATABASE name no target, so
// Graphs is empty for them.
//
// Kind is the element scope of a graph privilege ("node", "relationship" or
// "elements", the latter also when the statement omits the element scope,
// which Neo4j reads as ELEMENTS *) or "node" for a FOR pattern, which matches
// nodes; "database" and "dbms" are the other two scopes.
//
// Types holds the labels or relationship types the statement names, in the
// element scope (NODES Person) or as the privilege's own qualifier
// (SET LABEL Person). Properties holds the property names of a property list
// ({name, age} or {*}). Both keep the case and the decoded identity of the
// names as written.
//
// Predicate holds the text of a FOR pattern and its WHERE clause exactly as
// written, from the FOR keyword on, for typed binding later ("FOR (n:Document)
// WHERE 'household' IN n.collections"). The parser never evaluates it.
//
// The parser claims only statements whose scope and operands these fields can
// hold. The procedure and user-defined function privilege family
// (GRANT EXECUTE …) and role assignment (GRANT ROLE …, REVOKE ROLE …) have
// their own readers, and ok is false for them. Users holds the optional target
// accounts of IMPERSONATE and transaction privileges.
type fineGrainedPrivilegeCommand struct {
	Verb       string
	Privilege  string
	Graphs     []string
	Roles      []string
	Users      []string
	Kind       string
	Types      []string
	Properties []string
	Predicate  string
	Immutable  bool
}

// Verb values of fineGrainedPrivilegeCommand.Verb.
const (
	fineGrainedGrantVerb       = "grant"
	fineGrainedDenyVerb        = "deny"
	fineGrainedRevokeVerb      = "revoke"
	fineGrainedRevokeGrantVerb = "revoke_grant"
	fineGrainedRevokeDenyVerb  = "revoke_deny"
)

// Kind values of fineGrainedPrivilegeCommand.Kind.
const (
	fineGrainedNodeKind         = "node"
	fineGrainedRelationshipKind = "relationship"
	fineGrainedElementsKind     = "elements"
	fineGrainedDatabaseKind     = "database"
	fineGrainedDBMSKind         = "dbms"
)

// fineGrainedPrivilegeScope is the ON scope a privilege action is written on.
type fineGrainedPrivilegeScope uint8

const (
	fineGrainedGraphScope fineGrainedPrivilegeScope = 1 << iota
	fineGrainedDatabaseScope
	fineGrainedDBMSScope
)

// fineGrainedPrivilegeOperand is the qualified name list an action writes
// after its words, before ON.
type fineGrainedPrivilegeOperand uint8

const (
	fineGrainedNoOperand       fineGrainedPrivilegeOperand = iota
	fineGrainedPropertyOperand                             // { * | property[, …] }
	fineGrainedLabelOperand                                // * | label[, …]
)

// fineGrainedPrivilegeEntity is the element scope a graph action admits.
type fineGrainedPrivilegeEntity uint8

const (
	// fineGrainedNoEntity admits no element scope (WRITE, SET LABEL,
	// REMOVE LABEL, ALL GRAPH PRIVILEGES).
	fineGrainedNoEntity fineGrainedPrivilegeEntity = iota
	// fineGrainedPatternEntity admits ELEMENTS, NODES, RELATIONSHIPS or a FOR
	// pattern, and defaults to ELEMENTS * (TRAVERSE, READ, MATCH).
	fineGrainedPatternEntity
	// fineGrainedElementEntity admits ELEMENTS, NODES or RELATIONSHIPS, and
	// defaults to ELEMENTS * (CREATE, DELETE, SET PROPERTY, MERGE). Neo4j
	// does not support a FOR pattern for a write privilege.
	fineGrainedElementEntity
)

// fineGrainedPrivilegeAction is one documented spelling of one privilege
// action. An action with an empty phrase is ALL [[scope] PRIVILEGES], whose
// canonical phrase follows the ON scope.
type fineGrainedPrivilegeAction struct {
	words   []string
	phrase  string
	scopes  fineGrainedPrivilegeScope
	operand fineGrainedPrivilegeOperand
	entity  fineGrainedPrivilegeEntity
	// userList marks the privileges that take an optional (user[, …]) list.
	userList bool
}

// fineGrainedPrivilegeRolesKeyword is the keyword that introduces the roles of
// verb's statement: TO grants and denies, FROM revokes.
func fineGrainedPrivilegeRolesKeyword(verb string) string {
	switch verb {
	case fineGrainedRevokeVerb, fineGrainedRevokeGrantVerb, fineGrainedRevokeDenyVerb:
		return "FROM"
	default:
		return "TO"
	}
}

// parseFineGrainedPrivilegeCommand parses Neo4j 5.26's fine-grained privilege
// statements:
//
//	{GRANT | DENY | REVOKE [GRANT | DENY]} [IMMUTABLE] graph-privilege
//	    ON {HOME GRAPH | GRAPH[S] {* | name[, …]}}
//	    [ELEMENT[S] {* | label[, …]} | NODE[S] {* | label[, …]}
//	     | RELATIONSHIP[S] {* | rel-type[, …]} | FOR pattern]
//	    {TO | FROM} role[, …]
//	{GRANT | DENY | REVOKE [GRANT | DENY]} [IMMUTABLE] database-privilege
//	    ON {HOME DATABASE | DATABASE[S] {* | name[, …]}} {TO | FROM} role[, …]
//	{GRANT | DENY | REVOKE [GRANT | DENY]} [IMMUTABLE] dbms-privilege
//	    ON DBMS {TO | FROM} role[, …]
//
// ok is false for a statement of another reader: procedure and user-defined
// function privileges (EXECUTE …) and role assignment (ROLE …). err is a
// fine-grained privilege statement that is malformed, or whose qualifier these
// fields cannot hold, and every statement must be consumed whole: text after
// the roles is an error, not ignored.
func parseFineGrainedPrivilegeCommand(ctx context.Context, text string) (fineGrainedPrivilegeCommand, bool, error) {
	var command fineGrainedPrivilegeCommand
	if err := ctx.Err(); err != nil {
		return command, false, err
	}
	cursor := fineGrainedPrivilegeCursor{text: text}
	cursor.skipGap()
	verb, immutable, ok := cursor.readVerb()
	if !ok {
		return command, false, nil
	}
	if cursor.readsProcedureOrFunctionPrivileges() || cursor.readsRoleAssignment() {
		return fineGrainedPrivilegeCommand{}, false, nil
	}
	command.Verb, command.Immutable = verb, immutable
	action, err := cursor.readAction()
	if err != nil {
		return command, true, err
	}
	switch action.operand {
	case fineGrainedPropertyOperand:
		properties, err := cursor.readPropertyList()
		if err != nil {
			return command, true, err
		}
		command.Properties = properties
	case fineGrainedLabelOperand:
		types, err := cursor.readTypeList()
		if err != nil {
			return command, true, err
		}
		command.Types = types
	}
	if action.userList && cursor.at < len(cursor.text) && cursor.text[cursor.at] == '(' {
		cursor.at++
		users, err := cursor.readNameList(true, "expected a username", "a username wildcard stands alone")
		if err != nil {
			return command, true, err
		}
		if cursor.at >= len(cursor.text) || cursor.text[cursor.at] != ')' {
			return command, true, fineGrainedPrivilegeSyntaxError("expected ')' after usernames")
		}
		cursor.at++
		cursor.skipGap()
		command.Users = users
	}
	scope, targets, err := cursor.readScope()
	if err != nil {
		return command, true, err
	}
	if action.scopes&scope == 0 {
		return command, true, fineGrainedPrivilegeSyntaxError(fineGrainedPrivilegePhrase(action, scope) +
			" is not granted ON " + fineGrainedScopeName(scope))
	}
	command.Graphs = targets
	command.Privilege = fineGrainedPrivilegePhrase(action, scope)
	roleKeyword := fineGrainedPrivilegeRolesKeyword(verb)
	switch scope {
	case fineGrainedGraphScope:
		kind, types, predicate, err := cursor.readGraphEntity(ctx, action.entity, command.Privilege, roleKeyword)
		if err != nil {
			return command, true, err
		}
		command.Kind = kind
		if types != nil {
			command.Types = types
		}
		command.Predicate = predicate
	case fineGrainedDatabaseScope:
		command.Kind = fineGrainedDatabaseKind
	default:
		command.Kind = fineGrainedDBMSKind
	}
	if !cursor.takeKeyword(roleKeyword) {
		return command, true, fineGrainedPrivilegeSyntaxError("expected " + roleKeyword + " followed by a role")
	}
	roles, err := cursor.readRoleList()
	if err != nil {
		return command, true, err
	}
	command.Roles = roles
	if cursor.at < len(cursor.text) && cursor.text[cursor.at] == ';' {
		cursor.at++
		cursor.skipGap()
	}
	if cursor.at != len(cursor.text) {
		return command, true, fineGrainedPrivilegeSyntaxError("unexpected text after the roles")
	}
	return command, true, nil
}

// fineGrainedPrivilegeSyntaxError is the deterministic syntax error of a
// malformed fine-grained privilege statement.
func fineGrainedPrivilegeSyntaxError(detail string) error {
	return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax",
		"Invalid privilege statement: "+detail)
}

// fineGrainedPrivilegeCursor reads one statement's tokens. Every step leaves
// at on the next token, with the trivia before it consumed.
type fineGrainedPrivilegeCursor struct {
	text   string
	at     int
	params map[string]any
}

// skipGap consumes the whitespace and comments at at.
func (c *fineGrainedPrivilegeCursor) skipGap() {
	c.at, _ = readQueryGap(c.text, c.at)
}

// wordAt reports whether word stands at at as a whole keyword, in any case.
func (c *fineGrainedPrivilegeCursor) wordAt(word string) bool {
	return matchKeywordAt(c.text, c.at, word)
}

// takeKeyword consumes word at at, with the trivia after it, and reports
// whether it stood there.
func (c *fineGrainedPrivilegeCursor) takeKeyword(word string) bool {
	if !matchKeywordAt(c.text, c.at, word) {
		return false
	}
	c.at += len(word)
	c.skipGap()
	return true
}

// readName reads one symbolic name, quoted or not, and returns its decoded
// identity: a quoted name loses its backtick delimiters (a doubled backtick
// stands for one) and keeps its case.
func (c *fineGrainedPrivilegeCursor) readName() (string, bool) {
	name, ok := cyphertext.ScanIdentifier(c.text, c.at)
	if !ok {
		return "", false
	}
	c.at = name.End
	return name.Value, true
}

// readVerb reads GRANT, DENY or REVOKE together with the IMMUTABLE marker and
// REVOKE's GRANT or DENY marker. Neo4j writes REVOKE [IMMUTABLE] GRANT …
// REVOKE GRANT IMMUTABLE … is accepted as well, as the procedure privilege
// reader accepts it.
func (c *fineGrainedPrivilegeCursor) readVerb() (string, bool, bool) {
	var verb string
	switch {
	case c.takeKeyword("GRANT"):
		verb = fineGrainedGrantVerb
	case c.takeKeyword("DENY"):
		verb = fineGrainedDenyVerb
	case c.takeKeyword("REVOKE"):
		verb = fineGrainedRevokeVerb
	default:
		return "", false, false
	}
	var immutable bool
	for step := 0; step < 2; step++ {
		if verb == fineGrainedRevokeVerb {
			switch {
			case c.takeKeyword("IMMUTABLE"):
				immutable = true
			case c.takeKeyword("GRANT"):
				verb = fineGrainedRevokeGrantVerb
			case c.takeKeyword("DENY"):
				verb = fineGrainedRevokeDenyVerb
			default:
				return verb, immutable, true
			}
			continue
		}
		if c.takeKeyword("IMMUTABLE") {
			immutable = true
		}
		return verb, immutable, true
	}
	return verb, immutable, true
}

// readsProcedureOrFunctionPrivileges reports the EXECUTE privilege family,
// which the procedure privilege reader owns.
func (c *fineGrainedPrivilegeCursor) readsProcedureOrFunctionPrivileges() bool {
	return c.wordAt("EXECUTE")
}

// readsRoleAssignment reports GRANT ROLE[S] … and REVOKE ROLE[S] …, which
// assign roles rather than grant, deny or revoke a privilege. ROLE MANAGEMENT
// is a privilege, not an assignment.
func (c *fineGrainedPrivilegeCursor) readsRoleAssignment() bool {
	probe := *c
	if probe.takeKeyword("ROLE") && probe.wordAt("MANAGEMENT") {
		return false
	}
	probe = *c
	if probe.takeKeyword("ROLES") && probe.wordAt("MANAGEMENT") {
		return false
	}
	return c.wordAt("ROLE") || c.wordAt("ROLES")
}

// readAction reads the privilege action's words and reports the matching
// table entry.
func (c *fineGrainedPrivilegeCursor) readAction() (fineGrainedPrivilegeAction, error) {
	if c.at >= len(c.text) {
		return fineGrainedPrivilegeAction{}, fineGrainedPrivilegeSyntaxError("expected a privilege action")
	}
	action, end, ok := fineGrainedPrivilegeActionAt(c.text, c.at)
	if !ok {
		return fineGrainedPrivilegeAction{}, fineGrainedPrivilegeSyntaxError("unsupported privilege action")
	}
	c.at = end
	c.skipGap()
	return action, nil
}

// readScope reads ON and its target: DBMS, HOME GRAPH, HOME DATABASE, or a
// graph or database name list. The targets are the graphs of an ON GRAPH
// scope or the databases of an ON DATABASE scope; the HOME scopes name none.
func (c *fineGrainedPrivilegeCursor) readScope() (fineGrainedPrivilegeScope, []string, error) {
	if !c.takeKeyword("ON") {
		return 0, nil, fineGrainedPrivilegeSyntaxError("expected ON followed by the privilege scope")
	}
	if c.takeKeyword("DBMS") {
		return fineGrainedDBMSScope, nil, nil
	}
	if c.takeKeyword("HOME") {
		if c.takeKeyword("GRAPH") {
			return fineGrainedGraphScope, nil, nil
		}
		if c.takeKeyword("DATABASE") {
			return fineGrainedDatabaseScope, nil, nil
		}
		return 0, nil, fineGrainedPrivilegeSyntaxError("expected GRAPH or DATABASE after HOME")
	}
	if c.takeKeyword("GRAPH") || c.takeKeyword("GRAPHS") {
		targets, err := c.readTargets()
		return fineGrainedGraphScope, targets, err
	}
	if c.takeKeyword("DATABASE") || c.takeKeyword("DATABASES") {
		targets, err := c.readTargets()
		return fineGrainedDatabaseScope, targets, err
	}
	return 0, nil, fineGrainedPrivilegeSyntaxError("expected GRAPH, HOME GRAPH, DATABASE, HOME DATABASE or DBMS after ON")
}

// readTargets reads the graphs of an ON GRAPH scope or the databases of an ON
// DATABASE scope: * or a name list.
func (c *fineGrainedPrivilegeCursor) readTargets() ([]string, error) {
	return c.readNameList(true, "expected a graph or database name",
		"a graph or database wildcard stands alone")
}

// readNameList reads * (when allowWildcard) or a comma-separated list of
// names, all with their quoting decoded and their case kept. A wildcard
// stands alone, as in Neo4j: it does not join a name list.
func (c *fineGrainedPrivilegeCursor) readNameList(allowWildcard bool, missing, wildcardDetail string) ([]string, error) {
	c.skipGap()
	if allowWildcard && c.at < len(c.text) && c.text[c.at] == '*' {
		c.at++
		c.skipGap()
		if c.at < len(c.text) && c.text[c.at] == ',' {
			return nil, fineGrainedPrivilegeSyntaxError(wildcardDetail)
		}
		return []string{"*"}, nil
	}
	var names []string
	for {
		name, ok := c.readName()
		if !ok {
			return nil, fineGrainedPrivilegeSyntaxError(missing)
		}
		names = append(names, name)
		c.skipGap()
		if c.at < len(c.text) && c.text[c.at] == ',' {
			c.at++
			c.skipGap()
			continue
		}
		return names, nil
	}
}

// readGraphEntity reads a graph privilege's element scope: ELEMENTS, NODES,
// RELATIONSHIPS or a FOR pattern, each with its own names. An omitted element
// scope is ELEMENTS *.
func (c *fineGrainedPrivilegeCursor) readGraphEntity(ctx context.Context, entity fineGrainedPrivilegeEntity, phrase, roleKeyword string) (string, []string, string, error) {
	elementWritten := c.wordAt("ELEMENT") || c.wordAt("ELEMENTS") ||
		c.wordAt("NODE") || c.wordAt("NODES") ||
		c.wordAt("RELATIONSHIP") || c.wordAt("RELATIONSHIPS") ||
		c.wordAt("FOR")
	if entity == fineGrainedNoEntity {
		if elementWritten {
			return "", nil, "", fineGrainedPrivilegeSyntaxError(phrase + " does not take an element scope")
		}
		return fineGrainedElementsKind, nil, "", nil
	}
	if !elementWritten {
		return fineGrainedElementsKind, []string{"*"}, "", nil
	}
	switch {
	case c.takeKeyword("ELEMENT"), c.takeKeyword("ELEMENTS"):
		types, err := c.readTypeList()
		return fineGrainedElementsKind, types, "", err
	case c.takeKeyword("NODE"), c.takeKeyword("NODES"):
		types, err := c.readTypeList()
		return fineGrainedNodeKind, types, "", err
	case c.takeKeyword("RELATIONSHIP"), c.takeKeyword("RELATIONSHIPS"):
		types, err := c.readTypeList()
		return fineGrainedRelationshipKind, types, "", err
	}
	if entity != fineGrainedPatternEntity {
		return "", nil, "", fineGrainedPrivilegeSyntaxError(phrase + " does not take a FOR pattern")
	}
	predicate, kind, err := c.readPattern(ctx, roleKeyword)
	return kind, nil, predicate, err
}

// readPattern reads FOR, its parenthesized pattern and its optional WHERE
// clause, and returns that text as written for later typed binding. The
// pattern and predicate are never evaluated here.
func (c *fineGrainedPrivilegeCursor) readPattern(ctx context.Context, roleKeyword string) (string, string, error) {
	start := c.at
	if !c.takeKeyword("FOR") {
		return "", "", fineGrainedPrivilegeSyntaxError("expected FOR")
	}
	_, end, kind, err := fineGrainedPatternTarget(c.text, c.at)
	if err != nil {
		return "", "", err
	}
	c.at = end
	c.skipGap()
	if !matchKeywordAt(c.text, c.at, "WHERE") {
		return strings.TrimSpace(c.text[start:c.at]), kind, nil
	}
	c.at += len("WHERE")
	c.skipGap()
	if c.at >= len(c.text) {
		return "", "", fineGrainedPrivilegeSyntaxError("expected a predicate after WHERE")
	}
	predicateStart := c.at
	keyword, ok := c.rolesKeywordAfter(ctx, roleKeyword, predicateStart)
	if !ok {
		return "", "", fineGrainedPrivilegeSyntaxError("expected " + roleKeyword + " followed by a role")
	}
	if strings.TrimSpace(c.text[predicateStart:keyword]) == "" {
		return "", "", fineGrainedPrivilegeSyntaxError("expected a predicate after WHERE")
	}
	return strings.TrimSpace(c.text[start:keyword]), kind, nil
}

// rolesKeywordAfter finds the roles keyword that ends the statement after a
// WHERE clause and leaves at on it. The clause is kept as text, so its
// expression is not parsed: a role keyword standing in it (a variable named
// to, a keyword inside a string) is a candidate too, and only the candidate
// whose role list ends the statement is the statement's own.
func (c *fineGrainedPrivilegeCursor) rolesKeywordAfter(ctx context.Context, roleKeyword string, from int) (int, bool) {
	for pos := fineGrainedRoleKeywordIndex(ctx, c.text, roleKeyword, from); pos >= 0; pos = fineGrainedRoleKeywordIndex(ctx, c.text, roleKeyword, pos+1) {
		if ctx.Err() != nil {
			return 0, false
		}
		end := keywordEndAt(c.text, pos, roleKeyword)
		if end <= pos {
			continue
		}
		if fineGrainedRolesEndStatement(ctx, c.text, end) {
			c.at = pos
			return pos, true
		}
	}
	return 0, false
}

func (c *fineGrainedPrivilegeCursor) readPropertyList() ([]string, error) {
	c.skipGap()
	if c.at >= len(c.text) || c.text[c.at] != '{' {
		return nil, fineGrainedPrivilegeSyntaxError("expected a property list")
	}
	c.at++
	c.skipGap()
	var properties []string
	if c.at < len(c.text) && c.text[c.at] == '*' {
		properties = []string{"*"}
		c.at++
		c.skipGap()
	} else {
		for {
			name, ok := c.readName()
			if !ok {
				return nil, fineGrainedPrivilegeSyntaxError("expected a property name")
			}
			properties = append(properties, name)
			c.skipGap()
			if c.at < len(c.text) && c.text[c.at] == ',' {
				c.at++
				c.skipGap()
				continue
			}
			break
		}
	}
	if c.at >= len(c.text) || c.text[c.at] != '}' {
		return nil, fineGrainedPrivilegeSyntaxError("expected the end of the property list")
	}
	c.at++
	c.skipGap()
	return properties, nil
}

// readTypeList reads the labels or relationship types of an element scope or
// of a SET LABEL / REMOVE LABEL qualifier.
func (c *fineGrainedPrivilegeCursor) readTypeList() ([]string, error) {
	return c.readNameList(true, "expected a label or relationship type",
		"a label or relationship type wildcard stands alone")
}

// readRoleList reads the roles of a TO or FROM clause.
func (c *fineGrainedPrivilegeCursor) readRoleList() ([]string, error) {
	return c.readNameList(false, "expected a role name", "")
}

// fineGrainedRoleKeywordIndex is the next position of keyword at or after from
// outside quoted text, comments and nested (), [] and {} groups, or -1.
func fineGrainedRoleKeywordIndex(ctx context.Context, text, keyword string, from int) int {
	return keywordIndexFrom(ctx, text, keyword, from, keywordScanOpts{
		SkipParens:    true,
		SkipBrackets:  true,
		SkipBraces:    true,
		SkipStrings:   true,
		SkipBackticks: true,
		SkipComments:  true,
		Boundary:      keywordBoundaryWord,
	})
}

// fineGrainedRolesEndStatement reports whether a role list at at, with the
// statement's optional terminator after it, ends the statement.
func fineGrainedRolesEndStatement(ctx context.Context, text string, at int) bool {
	probe := fineGrainedPrivilegeCursor{text: text, at: at}
	if _, err := probe.readRoleList(); err != nil {
		return false
	}
	if probe.at < len(text) && text[probe.at] == ';' {
		probe.at++
		probe.skipGap()
	}
	return probe.at == len(text) && ctx.Err() == nil
}

// fineGrainedPrivilegePatternEnd is the end of the parenthesized pattern that
// opens at open, counting nested groups, or false when it does not close.
func fineGrainedPrivilegePatternEnd(text string, open int) (int, bool) {
	depth := 0
	for index := open; index < len(text); {
		if end, opaque := cyphertext.SkipToken(text, index); opaque {
			index = end
			continue
		}
		switch text[index] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return index + 1, true
			}
		}
		index++
	}
	return 0, false
}

// fineGrainedPrivilegePhrase is the canonical upper-case privilege of action
// on scope. ALL [[scope] PRIVILEGES] names every privilege of that scope.
func fineGrainedPrivilegePhrase(action fineGrainedPrivilegeAction, scope fineGrainedPrivilegeScope) string {
	if action.phrase != "" {
		return action.phrase
	}
	return "ALL " + fineGrainedScopeName(scope) + " PRIVILEGES"
}

func fineGrainedScopeName(scope fineGrainedPrivilegeScope) string {
	switch scope {
	case fineGrainedGraphScope:
		return "GRAPH"
	case fineGrainedDatabaseScope:
		return "DATABASE"
	default:
		return "DBMS"
	}
}

// fineGrainedPrivilegeActionAt matches the longest documented spelling whose
// words stand at at, and returns the entry with the end of its last word.
func fineGrainedPrivilegeActionAt(text string, at int) (fineGrainedPrivilegeAction, int, bool) {
	var best fineGrainedPrivilegeAction
	bestEnd := at
	for _, action := range fineGrainedPrivilegeActions() {
		end, ok := fineGrainedPrivilegeWordsAt(text, at, action.words)
		if !ok || end <= bestEnd {
			continue
		}
		best, bestEnd = action, end
	}
	return best, bestEnd, bestEnd > at
}

// fineGrainedPrivilegeWordsAt reports whether words stand at at as keywords
// in that order, separated by trivia, and returns the end of the last one.
func fineGrainedPrivilegeWordsAt(text string, at int, words []string) (int, bool) {
	pos := at
	for _, word := range words {
		pos, _ = readQueryGap(text, pos)
		if !matchKeywordAt(text, pos, word) {
			return 0, false
		}
		pos += len(word)
	}
	return pos, true
}

// fineGrainedPrivilegeActions is every fine-grained privilege action Neo4j
// 5.26 documents, with the scope the manual gives it, its qualifier and its
// element scope. The Cypher Manual's Read privileges, Write privileges,
// Database privileges and DBMS privileges sections own this list.
var fineGrainedPrivilegeActionsOnce sync.Once
var fineGrainedPrivilegeActionTable []fineGrainedPrivilegeAction

func fineGrainedPrivilegeActions() []fineGrainedPrivilegeAction {
	fineGrainedPrivilegeActionsOnce.Do(func() {
		fineGrainedPrivilegeActionTable = newFineGrainedPrivilegeActions()
	})
	return fineGrainedPrivilegeActionTable
}

func newFineGrainedPrivilegeActions() []fineGrainedPrivilegeAction {
	actions := make([]fineGrainedPrivilegeAction, 0, 96)
	add := func(phrase string, scopes fineGrainedPrivilegeScope, operand fineGrainedPrivilegeOperand, entity fineGrainedPrivilegeEntity, spellings ...string) {
		for _, spelling := range spellings {
			actions = append(actions, fineGrainedPrivilegeAction{
				words:   strings.Fields(spelling),
				phrase:  phrase,
				scopes:  scopes,
				operand: operand,
				entity:  entity,
			})
		}
	}

	// Graph privileges.
	add("TRAVERSE", fineGrainedGraphScope, fineGrainedNoOperand, fineGrainedPatternEntity,
		"TRAVERSE")
	add("READ", fineGrainedGraphScope, fineGrainedPropertyOperand, fineGrainedPatternEntity,
		"READ")
	add("MATCH", fineGrainedGraphScope, fineGrainedPropertyOperand, fineGrainedPatternEntity,
		"MATCH")
	add("CREATE", fineGrainedGraphScope, fineGrainedNoOperand, fineGrainedElementEntity,
		"CREATE")
	add("DELETE", fineGrainedGraphScope, fineGrainedNoOperand, fineGrainedElementEntity,
		"DELETE")
	add("SET LABEL", fineGrainedGraphScope, fineGrainedLabelOperand, fineGrainedNoEntity,
		"SET LABEL")
	add("REMOVE LABEL", fineGrainedGraphScope, fineGrainedLabelOperand, fineGrainedNoEntity,
		"REMOVE LABEL")
	add("SET PROPERTY", fineGrainedGraphScope, fineGrainedPropertyOperand, fineGrainedElementEntity,
		"SET PROPERTY")
	add("MERGE", fineGrainedGraphScope, fineGrainedPropertyOperand, fineGrainedElementEntity,
		"MERGE")
	add("WRITE", fineGrainedGraphScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"WRITE")

	// Database privileges.
	add("ACCESS", fineGrainedDatabaseScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"ACCESS")
	add("START", fineGrainedDatabaseScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"START")
	add("STOP", fineGrainedDatabaseScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"STOP")
	add("CREATE INDEX", fineGrainedDatabaseScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"CREATE INDEX", "CREATE INDEXES")
	add("DROP INDEX", fineGrainedDatabaseScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"DROP INDEX", "DROP INDEXES")
	add("SHOW INDEX", fineGrainedDatabaseScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"SHOW INDEX", "SHOW INDEXES")
	add("INDEX MANAGEMENT", fineGrainedDatabaseScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"INDEX", "INDEX MANAGEMENT")
	add("CREATE CONSTRAINT", fineGrainedDatabaseScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"CREATE CONSTRAINT", "CREATE CONSTRAINTS")
	add("DROP CONSTRAINT", fineGrainedDatabaseScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"DROP CONSTRAINT", "DROP CONSTRAINTS")
	add("SHOW CONSTRAINT", fineGrainedDatabaseScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"SHOW CONSTRAINT", "SHOW CONSTRAINTS")
	add("CONSTRAINT MANAGEMENT", fineGrainedDatabaseScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"CONSTRAINT", "CONSTRAINT MANAGEMENT")
	add("CREATE NEW LABEL", fineGrainedDatabaseScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"CREATE NEW LABEL", "CREATE NEW LABELS", "CREATE NEW NODE LABEL", "CREATE NEW NODE LABELS")
	add("CREATE NEW RELATIONSHIP TYPE", fineGrainedDatabaseScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"CREATE NEW TYPE", "CREATE NEW TYPES", "CREATE NEW RELATIONSHIP TYPE", "CREATE NEW RELATIONSHIP TYPES")
	add("CREATE NEW PROPERTY NAME", fineGrainedDatabaseScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"CREATE NEW NAME", "CREATE NEW NAMES", "CREATE NEW PROPERTY NAME", "CREATE NEW PROPERTY NAMES")
	add("NAME MANAGEMENT", fineGrainedDatabaseScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"NAME", "NAME MANAGEMENT")
	add("SHOW TRANSACTION", fineGrainedDatabaseScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"SHOW TRANSACTION", "SHOW TRANSACTIONS")
	add("TERMINATE TRANSACTION", fineGrainedDatabaseScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"TERMINATE TRANSACTION", "TERMINATE TRANSACTIONS")
	add("TRANSACTION MANAGEMENT", fineGrainedDatabaseScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"TRANSACTION", "TRANSACTION MANAGEMENT")

	// DBMS privileges.
	for _, name := range []string{"CREATE AUTH RULE", "ALTER AUTH RULE", "RENAME AUTH RULE", "DROP AUTH RULE", "SHOW AUTH RULE", "AUTH RULE MANAGEMENT"} {
		add(name, fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity, name)
	}
	add("CREATE ROLE", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"CREATE ROLE")
	add("RENAME ROLE", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"RENAME ROLE")
	add("DROP ROLE", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"DROP ROLE")
	add("ASSIGN ROLE", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"ASSIGN ROLE")
	add("REMOVE ROLE", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"REMOVE ROLE")
	add("SHOW ROLE", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"SHOW ROLE")
	add("ROLE MANAGEMENT", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"ROLE MANAGEMENT")
	add("CREATE USER", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"CREATE USER")
	add("RENAME USER", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"RENAME USER")
	add("ALTER USER", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"ALTER USER")
	add("DROP USER", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"DROP USER")

	add("SHOW USER METADATA", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity, "SHOW USER METADATA")
	add("SET USER METADATA", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity, "SET USER METADATA")
	add("USER METADATA MANAGEMENT", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity, "USER METADATA MANAGEMENT")
	add("SHOW USER", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"SHOW USER")
	add("USER MANAGEMENT", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"USER MANAGEMENT")
	add("SET PASSWORD", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"SET PASSWORD", "SET PASSWORDS")
	add("SET AUTH", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"SET AUTH")
	add("SET USER HOME DATABASE", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"SET USER HOME DATABASE")
	add("SET USER STATUS", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"SET USER STATUS")
	add("IMPERSONATE", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"IMPERSONATE")
	add("CREATE DATABASE", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"CREATE DATABASE")
	add("DROP DATABASE", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"DROP DATABASE")
	add("ALTER DATABASE", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"ALTER DATABASE")
	add("SET DATABASE ACCESS", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"SET DATABASE ACCESS")
	add("CREATE COMPOSITE DATABASE", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"CREATE COMPOSITE DATABASE")
	add("DROP COMPOSITE DATABASE", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"DROP COMPOSITE DATABASE")
	add("COMPOSITE DATABASE MANAGEMENT", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"COMPOSITE DATABASE MANAGEMENT")
	add("DATABASE MANAGEMENT", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"DATABASE MANAGEMENT")
	add("CREATE ALIAS", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"CREATE ALIAS")
	add("DROP ALIAS", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"DROP ALIAS")
	add("ALTER ALIAS", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"ALTER ALIAS")
	add("SHOW ALIAS", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"SHOW ALIAS")
	add("ALIAS MANAGEMENT", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"ALIAS MANAGEMENT")
	add("SERVER MANAGEMENT", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"SERVER MANAGEMENT")
	add("SHOW SERVERS", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"SHOW SERVERS")
	add("SHOW PRIVILEGE", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"SHOW PRIVILEGE", "SHOW PRIVILEGES")
	add("ASSIGN PRIVILEGE", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"ASSIGN PRIVILEGE", "ASSIGN PRIVILEGES")
	add("REMOVE PRIVILEGE", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"REMOVE PRIVILEGE", "REMOVE PRIVILEGES")
	add("PRIVILEGE MANAGEMENT", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"PRIVILEGE MANAGEMENT")

	// ALL [[scope] PRIVILEGES]. An empty phrase defers to the ON scope.
	add("", fineGrainedGraphScope|fineGrainedDatabaseScope|fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"ALL", "ALL PRIVILEGES")
	add("ALL GRAPH PRIVILEGES", fineGrainedGraphScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"ALL GRAPH PRIVILEGES")
	add("ALL DATABASE PRIVILEGES", fineGrainedDatabaseScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"ALL DATABASE PRIVILEGES")
	add("ALL DBMS PRIVILEGES", fineGrainedDBMSScope, fineGrainedNoOperand, fineGrainedNoEntity,
		"ALL DBMS PRIVILEGES")

	// The transaction privileges and IMPERSONATE take an optional user list.
	for index := range actions {
		switch actions[index].phrase {
		case "SHOW TRANSACTION", "TERMINATE TRANSACTION", "TRANSACTION MANAGEMENT", "IMPERSONATE":
			actions[index].userList = true
		}
	}
	return actions
}
