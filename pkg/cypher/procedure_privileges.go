package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// procedurePrivilegeCommand is a parsed GRANT / REVOKE EXECUTE [BOOSTED]
// PROCEDURE[S] … ON DBMS TO / FROM … statement.
type procedurePrivilegeCommand struct {
	grant      bool
	boosted    bool
	procedures []string
	roles      []string
}

// isProcedurePrivilegeCommand reports whether cypher is a GRANT or REVOKE of
// procedure execution, the privilege statements NornicDB runs; every other
// GRANT, REVOKE or DENY is unsupported.
func isProcedurePrivilegeCommand(cypher string) bool {
	_, ok, _ := parseProcedurePrivilegeCommand(cypher)
	return ok
}

// isPrivilegeAdministrationCommand reports whether cypher is a GRANT, REVOKE
// or DENY, or a SHOW of privileges, which administrators alone may run.
func isPrivilegeAdministrationCommand(cypher string) bool {
	trimmed := strings.TrimSpace(cypher)
	return startsWithKeywordFold(trimmed, "GRANT") || startsWithKeywordFold(trimmed, "REVOKE") ||
		startsWithKeywordFold(trimmed, "DENY") || isShowPrivilegesCommand(trimmed)
}

// isPrivilegeCommandShaped reports a GRANT, REVOKE or DENY shaped as a
// privilege statement: GRANT / REVOKE ROLE[S] …, or one with an ON clause.
// Another statement starting with these words is a syntax error.
func isPrivilegeCommandShaped(cypher string) bool {
	rest := strings.TrimSpace(cypher)
	var keyword string
	for _, candidate := range []string{"GRANT", "REVOKE", "DENY"} {
		if startsWithKeywordFold(rest, candidate) {
			keyword = candidate
			break
		}
	}
	if keyword == "" {
		return false
	}
	rest = strings.TrimSpace(rest[len(keyword):])
	if keyword != "DENY" && (startsWithKeywordFold(rest, "ROLE") || startsWithKeywordFold(rest, "ROLES")) {
		return true
	}
	return findKeywordIndex(rest, "ON") > 0
}

// parseProcedurePrivilegeCommand parses
//
//	GRANT [IMMUTABLE] EXECUTE [BOOSTED] PROCEDURE[S] name[, …] ON DBMS TO role[, …]
//	REVOKE [GRANT] [IMMUTABLE] EXECUTE [BOOSTED] PROCEDURE[S] name[, …] ON DBMS FROM role[, …]
//
// ok is false for any other statement; err is a statement of this shape
// that is malformed.
func parseProcedurePrivilegeCommand(cypher string) (procedurePrivilegeCommand, bool, error) {
	var command procedurePrivilegeCommand
	rest := strings.TrimSpace(cypher)
	switch {
	case startsWithKeywordFold(rest, "GRANT"):
		command.grant = true
		rest = strings.TrimSpace(rest[len("GRANT"):])
	case startsWithKeywordFold(rest, "REVOKE"):
		rest = strings.TrimSpace(rest[len("REVOKE"):])
		if startsWithKeywordFold(rest, "GRANT") {
			rest = strings.TrimSpace(rest[len("GRANT"):])
		}
	default:
		return command, false, nil
	}
	if startsWithKeywordFold(rest, "IMMUTABLE") {
		rest = strings.TrimSpace(rest[len("IMMUTABLE"):])
	}
	if !startsWithKeywordFold(rest, "EXECUTE") {
		return command, false, nil
	}
	rest = strings.TrimSpace(rest[len("EXECUTE"):])
	if startsWithKeywordFold(rest, "BOOSTED") {
		command.boosted = true
		rest = strings.TrimSpace(rest[len("BOOSTED"):])
	}
	switch {
	case startsWithKeywordFold(rest, "PROCEDURES"):
		rest = strings.TrimSpace(rest[len("PROCEDURES"):])
	case startsWithKeywordFold(rest, "PROCEDURE"):
		rest = strings.TrimSpace(rest[len("PROCEDURE"):])
	default:
		return command, false, nil
	}
	on := findKeywordIndex(rest, "ON")
	if on < 0 {
		return command, true, procedurePrivilegeSyntaxError("expected ON DBMS")
	}
	for _, name := range splitTopLevelComma(rest[:on]) {
		if name = strings.TrimSpace(name); name != "" {
			command.procedures = append(command.procedures, name)
		}
	}
	rest = strings.TrimSpace(rest[on+len("ON"):])
	if !startsWithKeywordFold(rest, "DBMS") {
		return command, true, procedurePrivilegeSyntaxError("procedure privileges are granted ON DBMS")
	}
	rest = strings.TrimSpace(rest[len("DBMS"):])
	target := "TO"
	if !command.grant {
		target = "FROM"
	}
	if !startsWithKeywordFold(rest, target) {
		return command, true, procedurePrivilegeSyntaxError("expected " + target + " followed by a role")
	}
	for _, role := range splitTopLevelComma(strings.TrimSpace(rest[len(target):])) {
		role = strings.Trim(strings.TrimSpace(role), "`")
		if role != "" {
			command.roles = append(command.roles, role)
		}
	}
	if len(command.procedures) == 0 || len(command.roles) == 0 {
		return command, true, procedurePrivilegeSyntaxError("expected a procedure and a role")
	}
	return command, true, nil
}

func procedurePrivilegeSyntaxError(detail string) error {
	return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", "Invalid procedure privilege statement: "+detail)
}

// systemStorage is the system database's storage, where privileges live:
// the database manager's, or the executor's own when it has none (an
// executor on the system database).
func (e *StorageExecutor) systemStorage() (storage.Engine, error) {
	if e.dbManager != nil {
		resolved, err := e.dbManager.GetStorageForUse("system", "")
		if err != nil {
			return nil, err
		}
		if engine, ok := resolved.(storage.Engine); ok {
			return engine, nil
		}
	}
	if e.storage == nil {
		return nil, fmt.Errorf("the system database is unavailable")
	}
	return e.storage, nil
}

// executeProcedurePrivilegeCommand runs GRANT / REVOKE EXECUTE [BOOSTED]
// PROCEDURE: each named procedure (a name or glob) for each role, which must
// exist. It returns no rows, as Neo4j's privilege commands do.
func (e *StorageExecutor) executeProcedurePrivilegeCommand(ctx context.Context, cypher string) (*ExecuteResult, error) {
	command, _, err := parseProcedurePrivilegeCommand(cypher)
	if err != nil {
		return nil, err
	}
	system, err := e.systemStorage()
	if err != nil {
		return nil, err
	}
	roles := auth.NewRoleStore(system)
	if err := roles.Load(ctx); err != nil {
		return nil, err
	}
	for _, role := range command.roles {
		if !auth.IsBuiltinRole(role) && !roles.Exists(role) {
			action := "grant"
			if !command.grant {
				action = "revoke"
			}
			return nil, newSemanticError("Neo.ClientError.Statement.ArgumentError", "RoleNotFound",
				fmt.Sprintf("Failed to %s privilege to role '%s': Role does not exist.", action, role))
		}
	}
	store := auth.NewProcedurePrivilegeStore(system)
	for _, role := range command.roles {
		for _, procedure := range command.procedures {
			if command.grant {
				err = store.Grant(ctx, role, procedure, command.boosted)
			} else {
				_, err = store.Revoke(ctx, role, procedure, command.boosted)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

// isShowPrivilegesCommand reports SHOW PRIVILEGES and SHOW ROLE[S] r[, …]
// PRIVILEGES.
func isShowPrivilegesCommand(cypher string) bool {
	if startsWithKeywords(cypher, "SHOW", "PRIVILEGES") || startsWithKeywords(cypher, "SHOW", "PRIVILEGE") {
		return true
	}
	if startsWithKeywords(cypher, "SHOW", "ROLES") || startsWithKeywords(cypher, "SHOW", "ROLE") {
		return findKeywordIndex(showCommandHead(cypher), "PRIVILEGES") > 0 || findKeywordIndex(showCommandHead(cypher), "PRIVILEGE") > 0
	}
	return false
}

// showPrivilegeRoles are the roles SHOW ROLE r[, …] PRIVILEGES names; nil for
// SHOW PRIVILEGES (every role).
func showPrivilegeRoles(head string) []string {
	rest := strings.TrimSpace(head)
	rest = strings.TrimSpace(rest[len("SHOW"):])
	switch {
	case startsWithKeywordFold(rest, "ROLES"):
		rest = strings.TrimSpace(rest[len("ROLES"):])
	case startsWithKeywordFold(rest, "ROLE"):
		rest = strings.TrimSpace(rest[len("ROLE"):])
	default:
		return nil
	}
	end := findKeywordIndex(rest, "PRIVILEGES")
	if end < 0 {
		end = findKeywordIndex(rest, "PRIVILEGE")
	}
	if end < 0 {
		return nil
	}
	var roles []string
	for _, role := range splitTopLevelComma(rest[:end]) {
		if role = strings.Trim(strings.TrimSpace(role), "`"); role != "" {
			roles = append(roles, strings.ToLower(strings.TrimPrefix(role, "role_")))
		}
	}
	return roles
}

// executeShowPrivileges lists the procedure privileges, in Neo4j's columns:
// access, action, resource, graph, segment, role, immutable.
func (e *StorageExecutor) executeShowPrivileges(ctx context.Context, head string) (*ExecuteResult, error) {
	system, err := e.systemStorage()
	if err != nil {
		return nil, err
	}
	store := auth.NewProcedurePrivilegeStore(system)
	if err := store.Load(ctx); err != nil {
		return nil, err
	}
	only := map[string]bool{}
	for _, role := range showPrivilegeRoles(head) {
		only[role] = true
	}
	result := &ExecuteResult{Columns: []string{"access", "action", "resource", "graph", "segment", "role", "immutable"}, Rows: [][]interface{}{}}
	for _, grant := range store.List() {
		if len(only) > 0 && !only[grant.Role] {
			continue
		}
		action := "execute"
		if grant.Boosted {
			action = "execute_boosted"
		}
		result.Rows = append(result.Rows, []interface{}{"GRANTED", action, "database", "*", "PROCEDURE(" + grant.Procedure + ")", grant.Role, false})
	}
	return result, nil
}
