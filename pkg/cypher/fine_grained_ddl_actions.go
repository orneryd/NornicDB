package cypher

import (
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/auth"
)

type fineGrainedActionPart struct {
	id       string
	kind     auth.FineGrainedResourceKind
	property bool
}

func fineGrainedGraphParts(command fineGrainedPrivilegeCommand) []fineGrainedActionPart {
	var actions []fineGrainedActionPart
	for _, kind := range []auth.FineGrainedResourceKind{auth.FineGrainedNode, auth.FineGrainedRelationship} {
		if command.Kind != fineGrainedElementsKind && command.Kind != string(kind) {
			continue
		}
		add := func(operation string, property bool) {
			actions = append(actions, fineGrainedActionPart{id: "graph." + string(kind) + "." + operation, kind: kind, property: property})
		}
		switch command.Privilege {
		case "TRAVERSE", "MATCH", "MERGE", "ALL GRAPH PRIVILEGES":
			add("traverse", false)
		}
		switch command.Privilege {
		case "READ", "MATCH", "MERGE", "ALL GRAPH PRIVILEGES":
			add("property.read", true)
		}
		switch command.Privilege {
		case "CREATE", "MERGE", "WRITE", "ALL GRAPH PRIVILEGES":
			add("create", false)
		}
		switch command.Privilege {
		case "DELETE", "WRITE", "ALL GRAPH PRIVILEGES":
			add("delete", false)
		}
		switch command.Privilege {
		case "SET PROPERTY", "MERGE", "WRITE", "ALL GRAPH PRIVILEGES":
			add("property.set", true)
		}
		if kind == auth.FineGrainedNode {
			switch command.Privilege {
			case "SET LABEL", "WRITE", "ALL GRAPH PRIVILEGES":
				add("label.add", false)
			}
			switch command.Privilege {
			case "REMOVE LABEL", "WRITE", "ALL GRAPH PRIVILEGES":
				add("label.remove", false)
			}
		}
	}
	return actions
}

var fineGrainedDatabasePrivilegesOnce sync.Once
var fineGrainedDatabasePrivilegesTable map[string][]string

func fineGrainedDatabasePrivileges() map[string][]string {
	fineGrainedDatabasePrivilegesOnce.Do(func() {
		fineGrainedDatabasePrivilegesTable = map[string][]string{
			"ACCESS":                       {"database.access"},
			"START":                        {"database.start"},
			"STOP":                         {"database.stop"},
			"CREATE INDEX":                 {"database.index.create"},
			"DROP INDEX":                   {"database.index.drop"},
			"SHOW INDEX":                   {"database.index.show"},
			"INDEX MANAGEMENT":             {"database.index.create", "database.index.drop", "database.index.show"},
			"CREATE CONSTRAINT":            {"database.constraint.create"},
			"DROP CONSTRAINT":              {"database.constraint.drop"},
			"SHOW CONSTRAINT":              {"database.constraint.show"},
			"CONSTRAINT MANAGEMENT":        {"database.constraint.create", "database.constraint.drop", "database.constraint.show"},
			"CREATE NEW LABEL":             {"database.token.label.create"},
			"CREATE NEW RELATIONSHIP TYPE": {"database.token.relationship_type.create"},
			"CREATE NEW PROPERTY NAME":     {"database.token.property.create"},
			"NAME MANAGEMENT":              {"database.token.label.create", "database.token.relationship_type.create", "database.token.property.create"},
			"SHOW TRANSACTION":             {"database.transaction.show"},
			"TERMINATE TRANSACTION":        {"database.transaction.terminate"},
			"TRANSACTION MANAGEMENT":       {"database.transaction.show", "database.transaction.terminate"},
		}
	})
	return fineGrainedDatabasePrivilegesTable
}

var fineGrainedDBMSPrivilegesOnce sync.Once
var fineGrainedDBMSPrivilegesTable map[string][]string

func fineGrainedDBMSPrivileges() map[string][]string {
	fineGrainedDBMSPrivilegesOnce.Do(func() {
		fineGrainedDBMSPrivilegesTable = map[string][]string{
			"SHOW USER METADATA":            {"dbms.user.metadata.show"},
			"SET USER METADATA":             {"dbms.user.metadata.set"},
			"USER METADATA MANAGEMENT":      {"dbms.user.metadata.show", "dbms.user.metadata.set"},
			"CREATE ROLE":                   {"dbms.role.create"},
			"RENAME ROLE":                   {"dbms.role.rename"},
			"DROP ROLE":                     {"dbms.role.drop"},
			"ASSIGN ROLE":                   {"dbms.role.assign"},
			"REMOVE ROLE":                   {"dbms.role.remove"},
			"SHOW ROLE":                     {"dbms.role.show"},
			"ROLE MANAGEMENT":               {"dbms.role.create", "dbms.role.rename", "dbms.role.drop", "dbms.role.assign", "dbms.role.remove", "dbms.role.show"},
			"CREATE USER":                   {"dbms.user.create"},
			"RENAME USER":                   {"dbms.user.rename"},
			"ALTER USER":                    {"dbms.user.alter", "dbms.user.password.set", "dbms.user.auth.set", "dbms.user.home.set", "dbms.user.status.set"},
			"DROP USER":                     {"dbms.user.drop"},
			"SHOW USER":                     {"dbms.user.show"},
			"USER MANAGEMENT":               {"dbms.user.create", "dbms.user.rename", "dbms.user.alter", "dbms.user.drop", "dbms.user.show", "dbms.user.password.set", "dbms.user.auth.set", "dbms.user.home.set", "dbms.user.status.set"},
			"SET PASSWORD":                  {"dbms.user.password.set"},
			"SET AUTH":                      {"dbms.user.auth.set"},
			"SET USER HOME DATABASE":        {"dbms.user.home.set"},
			"SET USER STATUS":               {"dbms.user.status.set"},
			"IMPERSONATE":                   {"dbms.user.impersonate"},
			"CREATE DATABASE":               {"dbms.database.create"},
			"DROP DATABASE":                 {"dbms.database.drop"},
			"ALTER DATABASE":                {"dbms.database.alter", "dbms.database.access.set"},
			"SET DATABASE ACCESS":           {"dbms.database.access.set"},
			"CREATE COMPOSITE DATABASE":     {"dbms.database.composite.create"},
			"DROP COMPOSITE DATABASE":       {"dbms.database.composite.drop"},
			"COMPOSITE DATABASE MANAGEMENT": {"dbms.database.composite.create", "dbms.database.composite.drop"},
			"DATABASE MANAGEMENT":           {"dbms.database.create", "dbms.database.drop", "dbms.database.alter", "dbms.database.access.set", "dbms.database.composite.create", "dbms.database.composite.drop"},
			"CREATE ALIAS":                  {"dbms.alias.create"},
			"DROP ALIAS":                    {"dbms.alias.drop"},
			"ALTER ALIAS":                   {"dbms.alias.alter"},
			"SHOW ALIAS":                    {"dbms.alias.show"},
			"ALIAS MANAGEMENT":              {"dbms.alias.create", "dbms.alias.drop", "dbms.alias.alter", "dbms.alias.show"},
			"SERVER MANAGEMENT":             {"dbms.server.manage", "dbms.server.show"},
			"SHOW SERVERS":                  {"dbms.server.show"},
			"SHOW PRIVILEGE":                {"dbms.security.privilege.show"},
			"ASSIGN PRIVILEGE":              {"dbms.security.privilege.assign"},
			"REMOVE PRIVILEGE":              {"dbms.security.privilege.remove"},
			"PRIVILEGE MANAGEMENT":          {"dbms.security.privilege.show", "dbms.security.privilege.assign", "dbms.security.privilege.remove"},
			"EXECUTE PROCEDURE":             {"dbms.procedure.execute"},
			"EXECUTE BOOSTED PROCEDURE":     {"dbms.procedure.execute_boosted"},
			"EXECUTE FUNCTION":              {"dbms.function.execute"},
			"EXECUTE BOOSTED FUNCTION":      {"dbms.function.execute_boosted"},
		}
	})
	return fineGrainedDBMSPrivilegesTable
}

func fineGrainedPrivilegeParts(command fineGrainedPrivilegeCommand) []fineGrainedActionPart {
	var kind auth.FineGrainedResourceKind
	var ids []string
	switch command.Kind {
	case fineGrainedDatabaseKind:
		kind = auth.FineGrainedDatabase
		ids = fineGrainedDatabasePrivileges()[command.Privilege]
	case fineGrainedDBMSKind:
		kind = auth.FineGrainedDBMS
		ids = fineGrainedDBMSPrivileges()[command.Privilege]
	default:
		return fineGrainedGraphParts(command)
	}

	var actions []fineGrainedActionPart
	all := command.Privilege == "ALL DATABASE PRIVILEGES" || command.Privilege == "ALL DBMS PRIVILEGES"
	for _, action := range auth.FineGrainedCoreActions() {
		if kind != action.ResourceKind {
			continue
		}
		// ALL DBMS PRIVILEGES contains the Neo4j operations. Editing the
		// NornicDB extension catalog is its separate explicit permission.
		inScope := all && (kind == auth.FineGrainedDBMS && action.ID != "dbms.security.policy.manage" ||
			kind == auth.FineGrainedDatabase && !strings.HasPrefix(action.ID, "database.procedure.") &&
				action.ID != "database.reveal" && action.ID != "database.history.read" && action.ID != "database.cdc.read" && action.ID != "database.schema.manage")
		for _, id := range ids {
			inScope = inScope || id == action.ID
		}
		if inScope {
			actions = append(actions, fineGrainedActionPart{action.ID, kind, action.Property})
		}
	}
	return actions
}
