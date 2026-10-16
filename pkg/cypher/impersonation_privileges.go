package cypher

import (
	"context"
	"errors"
	"strings"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// impersonatePrivilegeCommand is a parsed GRANT / DENY / REVOKE
// IMPERSONATE … ON DBMS TO / FROM … statement.
type impersonatePrivilegeCommand struct {
	// verb is GRANT, DENY or REVOKE.
	verb string
	// revokeGrant and revokeDeny are the privileges a REVOKE removes:
	// REVOKE GRANT one, REVOKE DENY the other, a plain REVOKE both.
	revokeGrant bool
	revokeDeny  bool
	immutable   bool
	targets     []auth.ImpersonateTarget
	roles       []string
}

// isImpersonatePrivilegeCommand reports whether cypher is a GRANT, DENY or
// REVOKE of IMPERSONATE.
func isImpersonatePrivilegeCommand(cypher string) bool {
	_, ok, _ := parseImpersonatePrivilegeCommand(cypher)
	return ok
}

// parseImpersonatePrivilegeCommand parses, as Neo4j 5.26 writes them:
//
//	GRANT [IMMUTABLE] IMPERSONATE [(*) | (user[, …])] ON DBMS TO role[, …]
//	DENY [IMMUTABLE] IMPERSONATE [(*) | (user[, …])] ON DBMS TO role[, …]
//	REVOKE [IMMUTABLE] [GRANT | DENY] IMPERSONATE [(*) | (user[, …])] ON DBMS FROM role[, …]
//
// IMPERSONATE without a list is IMPERSONATE (*). REVOKE also takes its
// GRANT / DENY before IMMUTABLE, as the procedure privilege statements do.
// ok is false for any other statement; err is a statement of this shape
// that is malformed.
func parseImpersonatePrivilegeCommand(cypher string) (impersonatePrivilegeCommand, bool, error) {
	var command impersonatePrivilegeCommand
	rest := strings.TrimSpace(cypher)
	for _, verb := range []string{"GRANT", "DENY", "REVOKE"} {
		if startsWithKeywordFold(rest, verb) {
			command.verb = verb
			rest = strings.TrimSpace(rest[len(verb):])
			break
		}
	}
	if command.verb == "" {
		return command, false, nil
	}
	revokeKind := func() {
		if command.verb != "REVOKE" || command.revokeGrant || command.revokeDeny {
			return
		}
		switch {
		case startsWithKeywordFold(rest, "GRANT"):
			command.revokeGrant = true
			rest = strings.TrimSpace(rest[len("GRANT"):])
		case startsWithKeywordFold(rest, "DENY"):
			command.revokeDeny = true
			rest = strings.TrimSpace(rest[len("DENY"):])
		}
	}
	revokeKind()
	if startsWithKeywordFold(rest, "IMMUTABLE") {
		command.immutable = true
		rest = strings.TrimSpace(rest[len("IMMUTABLE"):])
	}
	revokeKind()
	if command.verb == "REVOKE" && !command.revokeGrant && !command.revokeDeny {
		command.revokeGrant, command.revokeDeny = true, true
	}
	if !startsWithKeywordFold(rest, "IMPERSONATE") {
		return command, false, nil
	}
	rest = strings.TrimSpace(rest[len("IMPERSONATE"):])
	if strings.HasPrefix(rest, "(") {
		list, after, closed := extractParenSection(rest)
		if !closed {
			return command, true, impersonatePrivilegeSyntaxError("expected ) after the users")
		}
		targets, err := impersonateTargets(list)
		if err != nil {
			return command, true, err
		}
		command.targets = targets
		rest = strings.TrimSpace(after)
	} else {
		command.targets = []auth.ImpersonateTarget{auth.ImpersonateAllUsers}
	}
	if !startsWithKeywordFold(rest, "ON") {
		return command, true, impersonatePrivilegeSyntaxError("expected ON DBMS")
	}
	rest = strings.TrimSpace(rest[len("ON"):])
	if !startsWithKeywordFold(rest, "DBMS") {
		return command, true, impersonatePrivilegeSyntaxError("impersonation privileges are granted ON DBMS")
	}
	rest = strings.TrimSpace(rest[len("DBMS"):])
	target := "TO"
	if command.verb == "REVOKE" {
		target = "FROM"
	}
	if !startsWithKeywordFold(rest, target) {
		return command, true, impersonatePrivilegeSyntaxError("expected " + target + " followed by a role")
	}
	for _, written := range impersonationList(rest[len(target):]) {
		role, ok := impersonationName(written)
		if !ok {
			return command, true, impersonatePrivilegeSyntaxError("expected a role name, found " + written)
		}
		command.roles = append(command.roles, role)
	}
	if len(command.roles) == 0 {
		return command, true, impersonatePrivilegeSyntaxError("expected a role")
	}
	return command, true, nil
}

func impersonatePrivilegeSyntaxError(detail string) error {
	return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", "Invalid impersonation privilege statement: "+detail)
}

// impersonateTargets are the users IMPERSONATE (…) names, read as Neo4j's
// grammar reads them: a bare * alone is every user; otherwise each entry is
// one user name, plain or backticked, so `*` is the user named *. A bare *
// listed with users is a syntax error.
func impersonateTargets(list string) ([]auth.ImpersonateTarget, error) {
	entries := impersonationList(list)
	if len(entries) == 0 {
		return nil, impersonatePrivilegeSyntaxError("expected a user or *")
	}
	if len(entries) == 1 && entries[0] == "*" {
		return []auth.ImpersonateTarget{auth.ImpersonateAllUsers}, nil
	}
	targets := make([]auth.ImpersonateTarget, 0, len(entries))
	for _, written := range entries {
		if written == "*" {
			return nil, impersonatePrivilegeSyntaxError("* (every user) can't be listed with other users")
		}
		user, ok := impersonationName(written)
		if !ok {
			return nil, impersonatePrivilegeSyntaxError("expected a user name, found " + written)
		}
		targets = append(targets, auth.ImpersonateUser(user))
	}
	return targets, nil
}

// impersonationList is a comma-separated user or role list's entries; a
// trailing comma leaves an empty last entry, which no name matches.
func impersonationList(text string) []string {
	text = strings.TrimSpace(text)
	entries := splitTopLevelComma(text)
	if strings.HasSuffix(text, ",") {
		entries = append(entries, "")
	}
	return entries
}

// impersonationName is the user or role name written, a plain identifier or
// a backticked name (a doubled backtick inside stands for one); ok is false
// for anything else, such as a parameter.
func impersonationName(written string) (string, bool) {
	name, end, ok := scanIdentifierToken(written, 0)
	return name, ok && end == len(written) && name != ""
}

// executeImpersonatePrivilegeCommand runs GRANT / DENY / REVOKE [IMMUTABLE]
// IMPERSONATE: each named user for each role. It returns no rows, as Neo4j's
// privilege commands do, and follows the procedure privilege commands
// (executeProcedurePrivilegeCommand) where Neo4j treats them alike:
//   - GRANT and DENY need every role to exist (ArgumentError "Failed to
//     grant impersonate privilege to role …" / "Failed to deny …"); a REVOKE
//     from a role that doesn't exist changes nothing;
//   - while authentication is enabled, GRANT / DENY IMMUTABLE is refused, and
//     so is a REVOKE that names an immutable privilege; nothing is changed.
func (e *StorageExecutor) executeImpersonatePrivilegeCommand(ctx context.Context, cypher string) (*ExecuteResult, error) {
	command, _, err := parseImpersonatePrivilegeCommand(cypher)
	if err != nil {
		return nil, err
	}
	locked := !authenticationDisabled(ctx)
	if command.verb != "REVOKE" && command.immutable && locked {
		return nil, localizedStatusError("Neo.ClientError.Security.Forbidden", "Forbidden", localization.CypherAdminImmutablePrivilegeGrantForbidden())
	}
	system, err := e.privilegeStorage(true)
	if err != nil {
		return nil, err
	}
	var kinds []bool // denied: false for GRANT, true for DENY
	switch {
	case command.verb == "GRANT":
		kinds = []bool{false}
	case command.verb == "DENY":
		kinds = []bool{true}
	default:
		if command.revokeGrant {
			kinds = append(kinds, false)
		}
		if command.revokeDeny {
			kinds = append(kinds, true)
		}
	}
	apply := func(system storage.Engine) error {
		store := auth.NewImpersonatePrivilegeStore(system)
		if command.verb == "REVOKE" && locked {
			for _, role := range command.roles {
				for _, target := range command.targets {
					for _, denied := range kinds {
						privilege, found, err := store.Lookup(ctx, role, target, denied)
						if err != nil {
							return err
						}
						if found && privilege.Immutable {
							return localizedStatusError("Neo.ClientError.Security.Forbidden", "Forbidden", localization.CypherAdminImmutablePrivilegeRevokeForbidden())
						}
					}
				}
			}
		}
		for _, role := range command.roles {
			for _, target := range command.targets {
				for _, denied := range kinds {
					var err error
					if command.verb == "REVOKE" {
						_, err = store.Remove(ctx, role, target, denied, command.immutable)
					} else {
						err = store.Set(ctx, role, target, denied, command.immutable)
					}
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	if command.verb == "REVOKE" {
		err = auth.WithStoreTransaction(ctx, system, apply)
	} else {
		// GRANT and DENY write role-owned records through the one owner
		// that requires and locks their roles (auth.WithRoleRecords).
		err = auth.WithRoleRecords(ctx, system, command.roles, apply)
		var missing *auth.RoleNotFoundError
		if errors.As(err, &missing) {
			message := localization.CypherAdminPrivilegeRoleNotFound("impersonate", missing.Role)
			if command.verb == "DENY" {
				message = localization.CypherAdminPrivilegeDenyRoleNotFound("impersonate", missing.Role)
			}
			err = localizedStatusError("Neo.ClientError.Statement.ArgumentError", "RoleNotFound", message)
		}
	}
	if err != nil {
		return nil, err
	}
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

// impersonatePrivilegeRows are the SHOW PRIVILEGES rows of the impersonation
// privileges, in Neo4j's columns (access, action, resource, graph, segment,
// role, immutable): action impersonate, segment USER(name) or USER(*) for
// every user (USER(`*`) for the user named *).
func impersonatePrivilegeRows(ctx context.Context, system storage.Engine, only map[string]bool) ([][]interface{}, error) {
	privileges, err := auth.NewImpersonatePrivilegeStore(system).List(ctx)
	if err != nil {
		return nil, err
	}
	var rows [][]interface{}
	for _, privilege := range privileges {
		if len(only) > 0 && !only[privilege.Role] {
			continue
		}
		access := "GRANTED"
		if privilege.Denied {
			access = "DENIED"
		}
		segment := "USER(" + privilege.User + ")"
		switch {
		case privilege.AllUsers:
			segment = "USER(*)"
		case privilege.User == "*":
			// The user named *, told apart from every user.
			segment = "USER(`*`)"
		}
		rows = append(rows, []interface{}{access, "impersonate", "database", "*", segment, privilege.Role, privilege.Immutable})
	}
	return rows, nil
}
