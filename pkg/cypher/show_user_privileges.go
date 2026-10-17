package cypher

import (
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/auth"
)

// SHOW USER [name] PRIVILEGES (Neo4j 5): the privileges a user holds through
// its roles and PUBLIC, in SHOW PRIVILEGES' row shape plus the user column.
// Without a name it lists the executing user's own privileges (the
// impersonated user's while a session impersonates one), which needs no
// administrator rights, as in Neo4j; another user's need them. Collection
// rules appear as their GRANT / DENY MATCH rows, so a service can read
// whether its principal is collection-limited; SHOW CURRENT USER YIELD
// collectionLimited answers that directly.

// isShowUserPrivilegesCommand reports SHOW USER [name] PRIVILEGES.
func isShowUserPrivilegesCommand(cypher string) bool {
	if !startsWithKeywords(cypher, "SHOW", "USER") {
		return false
	}
	head := showCommandHead(cypher)
	return findKeywordIndex(head, "PRIVILEGES") > 0 || findKeywordIndex(head, "PRIVILEGE") > 0
}

var showUserPrivilegesColumns = []string{"access", "action", "resource", "graph", "segment", "role", "immutable", "user"}

// executeShowUserPrivileges lists a user's privileges. AS COMMANDS and
// several users are unsupported.
func (e *StorageExecutor) executeShowUserPrivileges(ctx context.Context, head string) (*ExecuteResult, error) {
	fields := strings.Fields(head)
	if findKeywordIndex(head, "AS") > 0 || len(fields) > 4 || strings.Contains(head, ",") {
		return nil, unsupportedAdministrationCommandError(head)
	}
	current, signedIn := authenticatedUserFromContext(ctx)
	user, roles := current.Name, current.Roles
	if len(fields) == 4 {
		named := strings.Trim(fields[2], "`")
		if !signedIn || named != current.Name {
			identity := requestIdentityFromContext(ctx)
			if identity == nil || identity.Users == nil {
				return nil, unsupportedAdministrationCommandError(head)
			}
			if allows := principalPermissions(ctx); allows != nil && !allows("admin") {
				return nil, &PermissionDeniedError{Permission: "admin"}
			}
			user, roles = named, nil
			found := false
			for _, listing := range identity.Users() {
				if listing.Name == named {
					roles, found = listing.Roles, true
					break
				}
			}
			if !found {
				return &ExecuteResult{Columns: append([]string(nil), showUserPrivilegesColumns...), Rows: [][]interface{}{}}, nil
			}
			signedIn = true
		}
	}
	result := &ExecuteResult{Columns: append([]string(nil), showUserPrivilegesColumns...), Rows: [][]interface{}{}}
	if !signedIn {
		return result, nil
	}
	held := map[string]bool{publicRole: true}
	for _, role := range roles {
		held[auth.NormalizeRoleName(role)] = true
	}
	system, err := e.privilegeStorage(false)
	if err != nil {
		return nil, err
	}
	// The role listing's own enumerator, limited to the roles the user holds
	// (PUBLIC included), with the user column added.
	rows, err := privilegeRecordRows(ctx, system, held)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		result.Rows = append(result.Rows, append(row, user))
	}
	return result, nil
}

// collectionLimitedDatabases are the databases where the statement's
// principal (the executing user) holds a collection rule: SHOW CURRENT
// USER's collectionLimited. A visibility that can't be resolved counts as
// limited (fail closed). A statement without a principal is limited nowhere.
func (e *StorageExecutor) collectionLimitedDatabases(ctx context.Context) []string {
	limited := []string{}
	authorization, ok := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization)
	if !ok || authorization.visible == nil {
		return limited
	}
	var databases []string
	if e.dbManager != nil {
		for _, info := range e.dbManager.ListDatabases() {
			if info.Type() != "system" && !e.dbManager.IsCompositeDatabase(info.Name()) {
				databases = append(databases, info.Name())
			}
		}
	} else {
		databases = []string{e.currentDatabaseName()}
	}
	for _, database := range databases {
		visibility, err := authorization.visible(database)
		// A database the principal may not use at all is not one of its
		// limited databases, and its name is not the principal's to see.
		var denied *PermissionDeniedError
		if errors.As(err, &denied) {
			continue
		}
		if err != nil || visibility != nil {
			limited = append(limited, database)
		}
	}
	sort.Strings(limited)
	return limited
}

// executeShowUsersWithLimits is SHOW USERS / SHOW CURRENT USER with the
// collectionLimited column, listed only when yielded (default columns stay
// Neo4j's): the executing user's collection-limited databases on its own
// row, null on other users' rows.
func (e *StorageExecutor) executeShowUsersWithLimits(ctx context.Context, head string) (*ExecuteResult, error) {
	result, err := e.executeShowUsers(ctx, head)
	if err != nil {
		return nil, err
	}
	current, signedIn := authenticatedUserFromContext(ctx)
	var limited []string
	if signedIn {
		limited = e.collectionLimitedDatabases(ctx)
	}
	result.Columns = append(result.Columns, "collectionLimited")
	for i, row := range result.Rows {
		var value interface{}
		if signedIn && row[0] == current.Name {
			value = limited
		}
		result.Rows[i] = append(row, value)
	}
	return withShowDefaultColumns(result, append([]string(nil), showUsersColumns...)), nil
}
