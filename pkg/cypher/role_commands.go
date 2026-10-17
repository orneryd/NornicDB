package cypher

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// Neo4j's role administration commands (Cypher Manual, "Managing roles"):
//
//	CREATE [OR REPLACE] ROLE name [IF NOT EXISTS]
//	DROP ROLE name [IF EXISTS]
//	RENAME ROLE name [IF EXISTS] TO otherName
//	GRANT ROLE[S] name[, ...] TO user[, ...]
//	REVOKE ROLE[S] name[, ...] FROM user[, ...]
//	SHOW [ALL | POPULATED] ROLE[S] [WITH USER[S]]
//
// They run through the server's one owner of roles and role assignments
// (RoleAdministration, on the request identity), whose role mutations also
// move every role-owned record and publish the server's loaded caches, as
// the HTTP role API does. A name is an identifier, a backtick-quoted name or
// a $parameter. IMMUTABLE roles and CREATE ROLE … AS COPY OF are refused as
// unsupported: NornicDB has no immutable roles, and a role's privileges
// live in several stores that a copy would have to move together.

// RoleAdministration is the server's owner of roles and their members.
type RoleAdministration interface {
	// Roles lists every role, built-in ones included, with its members.
	Roles() []RoleListing
	// CreateRole creates a role: auth.ErrRoleExists when it exists.
	CreateRole(ctx context.Context, name string) error
	// DropRole deletes a role and every record it owns: auth.ErrRoleNotFound
	// when it doesn't exist, auth.ErrCannotDeleteBuiltinRole for a built-in.
	DropRole(ctx context.Context, name string) error
	// RenameRole renames a role and every record it owns.
	RenameRole(ctx context.Context, from, to string) error
	// RoleExists reports whether a role exists.
	RoleExists(name string) bool
	// UserRoles is a user's roles: auth.ErrUserNotFound when it doesn't exist.
	UserRoles(user string) ([]string, error)
	// ChangeUserRoles changes the roles of every user in users with change,
	// which gets a user's current roles and returns its new ones and whether
	// they differ. Each user's roles are read, changed and written in one
	// step no other role change interleaves with, so concurrent role
	// commands never lose each other's changes, and every user changes or
	// none does. A missing user is auth.ErrUserNotFound, before anything
	// changes, unless skipMissing.
	ChangeUserRoles(ctx context.Context, users []string, change func(current []string) ([]string, bool), skipMissing bool) error
}

// RoleListing is one role and its members, for SHOW ROLES.
type RoleListing struct {
	Name    string
	Members []string
}

// roleCommand is a parsed role administration command.
type roleCommand struct {
	verb        string   // CREATE, DROP, RENAME, GRANT, REVOKE
	replace     bool     // CREATE OR REPLACE
	ifNotExists bool     // CREATE … IF NOT EXISTS
	ifExists    bool     // DROP / RENAME … IF EXISTS
	roles       []string // the roles named
	target      string   // RENAME … TO target
	users       []string // GRANT … TO users / REVOKE … FROM users
	unsupported string   // IMMUTABLE or AS COPY OF
}

// isRoleCommand reports a role administration command other than GRANT /
// REVOKE ROLE (which isPrivilegeAdministrationCommand already covers):
// CREATE [OR REPLACE] [IMMUTABLE] ROLE, DROP ROLE, RENAME ROLE and SHOW
// [ALL | POPULATED] ROLE[S] without PRIVILEGES.
func isRoleCommand(cypher string) bool {
	words := leadingUpperWords(cypher, 5)
	if len(words) < 2 {
		return false
	}
	switch words[0] {
	case "CREATE":
		rest := words[1:]
		if len(rest) >= 2 && rest[0] == "OR" && rest[1] == "REPLACE" {
			rest = rest[2:]
		}
		if len(rest) >= 1 && rest[0] == "IMMUTABLE" {
			rest = rest[1:]
		}
		return len(rest) >= 1 && rest[0] == "ROLE"
	case "DROP", "RENAME":
		return words[1] == "ROLE"
	case "SHOW":
		rest := words[1:]
		if len(rest) >= 1 && (rest[0] == "ALL" || rest[0] == "POPULATED") {
			rest = rest[1:]
		}
		return len(rest) >= 1 && (rest[0] == "ROLE" || rest[0] == "ROLES") && !isShowPrivilegesCommand(cypher)
	}
	return false
}

// isRoleGrantCommand reports GRANT ROLE[S] … TO … and REVOKE ROLE[S] … FROM ….
func isRoleGrantCommand(cypher string) bool {
	words := leadingUpperWords(cypher, 2)
	return len(words) == 2 && (words[0] == "GRANT" || words[0] == "REVOKE") && (words[1] == "ROLE" || words[1] == "ROLES")
}

// leadingUpperWords is the first n words of cypher, upper-cased.
func leadingUpperWords(cypher string, n int) []string {
	fields := strings.Fields(strings.TrimSpace(cypher))
	if len(fields) > n {
		fields = fields[:n]
	}
	for i := range fields {
		fields[i] = strings.ToUpper(fields[i])
	}
	return fields
}

// roleToken is one token of a role command: a word, a name (backtick-quoted
// or a $parameter, resolved later) or a comma.
type roleToken struct {
	text   string
	quoted bool // backtick-quoted: a name, never a keyword
	param  bool // $parameter
}

func tokenizeRoleCommand(cypher string) ([]roleToken, error) {
	var tokens []roleToken
	runes := []rune(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(cypher), ";")))
	for i := 0; i < len(runes); {
		r := runes[i]
		switch {
		case unicode.IsSpace(r):
			i++
		case r == ',':
			tokens = append(tokens, roleToken{text: ","})
			i++
		case r == '`':
			var b strings.Builder
			i++
			for {
				if i >= len(runes) {
					return nil, errors.New("unterminated quoted name")
				}
				if runes[i] == '`' {
					if i+1 < len(runes) && runes[i+1] == '`' {
						b.WriteRune('`')
						i += 2
						continue
					}
					i++
					break
				}
				b.WriteRune(runes[i])
				i++
			}
			tokens = append(tokens, roleToken{text: b.String(), quoted: true})
		case r == '$':
			start := i + 1
			i++
			for i < len(runes) && (unicode.IsLetter(runes[i]) || unicode.IsDigit(runes[i]) || runes[i] == '_') {
				i++
			}
			if i == start {
				return nil, errors.New("invalid parameter")
			}
			tokens = append(tokens, roleToken{text: string(runes[start:i]), param: true})
		case unicode.IsLetter(r) || r == '_':
			start := i
			for i < len(runes) && (unicode.IsLetter(runes[i]) || unicode.IsDigit(runes[i]) || runes[i] == '_') {
				i++
			}
			tokens = append(tokens, roleToken{text: string(runes[start:i])})
		default:
			return nil, fmt.Errorf("unexpected %q", string(r))
		}
	}
	return tokens, nil
}

// roleCommandParser reads a role command's tokens.
type roleCommandParser struct {
	tokens []roleToken
	pos    int
	params map[string]interface{}
}

func (p *roleCommandParser) keyword(word string) bool {
	if p.pos < len(p.tokens) && !p.tokens[p.pos].quoted && !p.tokens[p.pos].param && strings.EqualFold(p.tokens[p.pos].text, word) {
		p.pos++
		return true
	}
	return false
}

func (p *roleCommandParser) keywords(words ...string) bool {
	start := p.pos
	for _, word := range words {
		if !p.keyword(word) {
			p.pos = start
			return false
		}
	}
	return true
}

// name reads one name: an identifier, a quoted name or a string parameter.
func (p *roleCommandParser) name() (string, error) {
	if p.pos >= len(p.tokens) || p.tokens[p.pos].text == "," && !p.tokens[p.pos].quoted {
		return "", errors.New("expected a name")
	}
	token := p.tokens[p.pos]
	p.pos++
	if !token.param {
		return token.text, nil
	}
	value, ok := p.params[token.text]
	if !ok {
		return "", fmt.Errorf("expected parameter $%s", token.text)
	}
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("parameter $%s must be a non-empty string", token.text)
	}
	return text, nil
}

// names reads name[, name …].
func (p *roleCommandParser) names() ([]string, error) {
	var out []string
	for {
		name, err := p.name()
		if err != nil {
			return nil, err
		}
		out = append(out, name)
		if p.pos < len(p.tokens) && p.tokens[p.pos].text == "," && !p.tokens[p.pos].quoted {
			p.pos++
			continue
		}
		return out, nil
	}
}

func (p *roleCommandParser) done() bool { return p.pos == len(p.tokens) }

// parseRoleCommand parses every role command except SHOW ROLES.
func parseRoleCommand(cypher string, params map[string]interface{}) (roleCommand, error) {
	var command roleCommand
	tokens, err := tokenizeRoleCommand(cypher)
	if err != nil {
		return command, err
	}
	p := &roleCommandParser{tokens: tokens, params: params}
	switch {
	case p.keyword("CREATE"):
		command.verb = "CREATE"
		command.replace = p.keywords("OR", "REPLACE")
		if p.keyword("IMMUTABLE") {
			command.unsupported = "IMMUTABLE"
		}
		if !p.keyword("ROLE") {
			return command, errors.New("expected ROLE")
		}
		name, err := p.name()
		if err != nil {
			return command, err
		}
		command.roles = []string{name}
		command.ifNotExists = p.keywords("IF", "NOT", "EXISTS")
		if command.replace && command.ifNotExists {
			return command, errors.New("OR REPLACE and IF NOT EXISTS cannot be used together")
		}
		if p.keywords("AS", "COPY", "OF") {
			if _, err := p.name(); err != nil {
				return command, err
			}
			command.unsupported = "AS COPY OF"
		}
	case p.keyword("DROP"):
		command.verb = "DROP"
		if !p.keyword("ROLE") {
			return command, errors.New("expected ROLE")
		}
		name, err := p.name()
		if err != nil {
			return command, err
		}
		command.roles = []string{name}
		command.ifExists = p.keywords("IF", "EXISTS")
	case p.keyword("RENAME"):
		command.verb = "RENAME"
		if !p.keyword("ROLE") {
			return command, errors.New("expected ROLE")
		}
		name, err := p.name()
		if err != nil {
			return command, err
		}
		command.roles = []string{name}
		command.ifExists = p.keywords("IF", "EXISTS")
		if !p.keyword("TO") {
			return command, errors.New("expected TO")
		}
		if command.target, err = p.name(); err != nil {
			return command, err
		}
	case p.keyword("GRANT"), p.keyword("REVOKE"):
		command.verb = strings.ToUpper(tokens[0].text)
		if !p.keyword("ROLES") && !p.keyword("ROLE") {
			return command, errors.New("expected ROLE")
		}
		if command.roles, err = p.names(); err != nil {
			return command, err
		}
		preposition := "TO"
		if command.verb == "REVOKE" {
			preposition = "FROM"
		}
		if !p.keyword(preposition) {
			return command, errors.New("expected " + preposition)
		}
		if command.users, err = p.names(); err != nil {
			return command, err
		}
	default:
		return command, errors.New("expected a role command")
	}
	if !p.done() {
		return command, fmt.Errorf("unexpected %q", p.tokens[p.pos].text)
	}
	return command, nil
}

// roleAdministration is the request's role owner; nil without one (an
// embedded executor or a server without role stores), and the role
// commands are then unsupported, as in Neo4j Community.
func roleAdministration(ctx context.Context) RoleAdministration {
	if identity := requestIdentityFromContext(ctx); identity != nil {
		return identity.Roles
	}
	return nil
}

func roleCommandSyntaxError(cypher string, cause error) error {
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax",
		localization.CypherAdminRoleCommandInvalid(strings.TrimSpace(cypher), cause.Error()))
}

// roleCommandError maps the role owner's errors to Neo4j's codes: a role
// or user that does or doesn't exist is an argument error, as Neo4j's
// GRANT to a missing role is (Neo.ClientError.Statement.ArgumentError).
func roleCommandError(err error, role, user string) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, auth.ErrRoleExists):
		return localizedStatusError("Neo.ClientError.Statement.ArgumentError", "RoleExists", localization.CypherAdminRoleAlreadyExists(role))
	case errors.Is(err, auth.ErrRoleNotFound):
		return localizedStatusError("Neo.ClientError.Statement.ArgumentError", "RoleNotFound", localization.CypherAdminRoleNotFound(role))
	case errors.Is(err, auth.ErrCannotDeleteBuiltinRole):
		return localizedStatusError("Neo.ClientError.Statement.ArgumentError", "BuiltinRole", localization.CypherAdminRoleBuiltin(role))
	case errors.Is(err, auth.ErrUserNotFound):
		return localizedStatusError("Neo.ClientError.Statement.ArgumentError", "UserNotFound", localization.CypherAdminRoleUserNotFound(user))
	case errors.Is(err, auth.ErrInvalidRoleName):
		return localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidRoleName", localization.CypherAdminRoleNameInvalid(role))
	}
	return err
}

// executeRoleCommand runs CREATE / DROP / RENAME ROLE and GRANT / REVOKE
// ROLE. It returns no rows, as Neo4j's role commands do. As in Neo4j:
//   - CREATE fails on an existing role, IF NOT EXISTS makes that no change,
//     and OR REPLACE drops the existing role first;
//   - DROP and RENAME fail on a missing role, IF EXISTS makes that no change;
//     RENAME to an existing role fails;
//   - GRANT ROLE needs every role and user to exist and changes nothing
//     otherwise; granting a role a user holds is no change. REVOKE ROLE of a
//     role or user that doesn't exist, or that the user doesn't hold, is no
//     change (Neo4j reports these with notifications, not errors).
//
// The commands change the server's role stores directly, so they run on
// their own, not inside an explicit transaction.
func (e *StorageExecutor) executeRoleCommand(ctx context.Context, cypher string) (*ExecuteResult, error) {
	command, err := parseRoleCommand(cypher, getParamsFromContext(ctx))
	if err != nil {
		return nil, roleCommandSyntaxError(cypher, err)
	}
	if command.unsupported != "" {
		return nil, unsupportedAdministrationCommandError(cypher)
	}
	roles := roleAdministration(ctx)
	if roles == nil {
		return nil, unsupportedAdministrationCommandError(cypher)
	}
	if e.txContext != nil && e.txContext.active {
		return nil, localizedStatusError("Neo.ClientError.Transaction.ForbiddenDueToTransactionType", "ForbiddenDueToTransactionType",
			localization.CypherAdminRoleCommandInTransaction())
	}
	empty := &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}
	switch command.verb {
	case "CREATE":
		role := command.roles[0]
		if roles.RoleExists(role) {
			switch {
			case command.ifNotExists:
				return empty, nil
			case command.replace:
				if err := roles.DropRole(ctx, role); err != nil {
					return nil, roleCommandError(err, role, "")
				}
			default:
				return nil, roleCommandError(auth.ErrRoleExists, role, "")
			}
		}
		return empty, roleCommandError(roles.CreateRole(ctx, role), role, "")
	case "DROP":
		role := command.roles[0]
		if !roles.RoleExists(role) {
			if command.ifExists {
				return empty, nil
			}
			return nil, roleCommandError(auth.ErrRoleNotFound, role, "")
		}
		return empty, roleCommandError(roles.DropRole(ctx, role), role, "")
	case "RENAME":
		role := command.roles[0]
		if !roles.RoleExists(role) {
			if command.ifExists {
				return empty, nil
			}
			return nil, roleCommandError(auth.ErrRoleNotFound, role, "")
		}
		if roles.RoleExists(command.target) {
			return nil, roleCommandError(auth.ErrRoleExists, command.target, "")
		}
		return empty, roleCommandError(roles.RenameRole(ctx, role, command.target), role, "")
	case "GRANT", "REVOKE":
		return empty, e.assignRoles(ctx, roles, command)
	}
	return nil, unsupportedAdministrationCommandError(cypher)
}

// assignRoles runs GRANT / REVOKE ROLE through one ChangeUserRoles: a GRANT
// naming a missing role or user changes no one, a REVOKE skips a missing
// user, a failure writing one user changes no one, and each user's roles
// are changed from what it holds when the change is written, so a
// concurrent role command's change is never lost.
func (e *StorageExecutor) assignRoles(ctx context.Context, roles RoleAdministration, command roleCommand) error {
	grant := command.verb == "GRANT"
	if grant {
		for _, role := range command.roles {
			if !roles.RoleExists(role) {
				return roleCommandError(auth.ErrRoleNotFound, role, "")
			}
		}
	}
	err := roles.ChangeUserRoles(ctx, command.users, func(current []string) ([]string, bool) {
		return assignedRoles(current, command.roles, grant)
	}, !grant)
	missing := ""
	if errors.Is(err, auth.ErrUserNotFound) {
		for _, user := range command.users {
			if _, lookupErr := roles.UserRoles(user); errors.Is(lookupErr, auth.ErrUserNotFound) {
				missing = user
				break
			}
		}
	}
	return roleCommandError(err, "", missing)
}

// assignedRoles is current with named added (grant) or removed, compared by
// normalized name, and whether that changes anything.
func assignedRoles(current, named []string, grant bool) ([]string, bool) {
	held := make(map[string]bool, len(current))
	for _, role := range current {
		held[auth.NormalizeRoleName(role)] = true
	}
	next := append([]string(nil), current...)
	changed := false
	for _, role := range named {
		key := auth.NormalizeRoleName(role)
		switch {
		case grant && !held[key]:
			next = append(next, key)
			held[key] = true
			changed = true
		case !grant && held[key]:
			filtered := next[:0]
			for _, existing := range next {
				if auth.NormalizeRoleName(existing) != key {
					filtered = append(filtered, existing)
				}
			}
			next = filtered
			delete(held, key)
			changed = true
		}
	}
	return next, changed
}

// showRolesColumns are SHOW ROLES' columns: role, plus member WITH USERS.
// Neo4j 5.26 adds immutable under YIELD *.
var (
	showRolesColumns          = []string{"role", "immutable"}
	showRolesWithUsersColumns = []string{"role", "member", "immutable"}
)

// executeShowRoles lists the roles (SHOW [ALL | POPULATED] ROLE[S] [WITH
// USER[S]]): ALL (the default) lists every role, POPULATED only roles with a
// member; WITH USERS lists one row per role and member, and for ALL a role
// without members once with a null member. No NornicDB role is immutable.
func (e *StorageExecutor) executeShowRoles(ctx context.Context, cypher string) (*ExecuteResult, error) {
	roles := roleAdministration(ctx)
	if roles == nil {
		return nil, unsupportedAdministrationCommandError(cypher)
	}
	words := leadingUpperWords(cypher, 6)
	populated := len(words) > 1 && words[1] == "POPULATED"
	withUsers := false
	for i := 1; i+1 < len(words); i++ {
		if words[i] == "WITH" && (words[i+1] == "USER" || words[i+1] == "USERS") {
			withUsers = true
		}
	}
	listings := roles.Roles()
	sort.Slice(listings, func(i, j int) bool { return listings[i].Name < listings[j].Name })
	result := &ExecuteResult{Columns: append([]string(nil), showRolesColumns...)}
	if withUsers {
		result.Columns = append([]string(nil), showRolesWithUsersColumns...)
	}
	for _, listing := range listings {
		if populated && len(listing.Members) == 0 {
			continue
		}
		if !withUsers {
			result.Rows = append(result.Rows, []interface{}{listing.Name, false})
			continue
		}
		members := append([]string(nil), listing.Members...)
		sort.Strings(members)
		if len(members) == 0 {
			result.Rows = append(result.Rows, []interface{}{listing.Name, nil, false})
		}
		for _, member := range members {
			result.Rows = append(result.Rows, []interface{}{listing.Name, member, false})
		}
	}
	if result.Rows == nil {
		result.Rows = [][]interface{}{}
	}
	// immutable is listed only under YIELD *, as in Neo4j 5.26.
	return withShowDefaultColumns(result, result.Columns[:len(result.Columns)-1]), nil
}
