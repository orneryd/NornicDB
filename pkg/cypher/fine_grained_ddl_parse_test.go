package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// requireFineGrainedPrivilege parses statement as a fine-grained privilege
// command and compares the whole product.
func requireFineGrainedPrivilege(t *testing.T, statement string, want fineGrainedPrivilegeCommand) {
	t.Helper()
	command, ok, err := parseFineGrainedPrivilegeCommand(t.Context(), statement)
	require.True(t, ok, statement)
	require.NoError(t, err, statement)
	require.Equal(t, want, command, statement)
}

func TestParseFineGrainedPrivilegeGraphCommands(t *testing.T) {
	for name, test := range map[string]struct {
		statement string
		want      fineGrainedPrivilegeCommand
	}{
		"node label": {
			statement: "GRANT TRAVERSE ON GRAPH neo4j NODES Post TO regularUsers",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "TRAVERSE", Graphs: []string{"neo4j"},
				Roles: []string{"regularUsers"}, Kind: "node", Types: []string{"Post"},
			},
		},
		"relationship types": {
			statement: "GRANT DELETE ON GRAPH * RELATIONSHIPS bar TO regularUsers",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "DELETE", Graphs: []string{"*"},
				Roles: []string{"regularUsers"}, Kind: "relationship", Types: []string{"bar"},
			},
		},
		"property list on node label": {
			statement: "GRANT MATCH { language, length } ON GRAPH neo4j NODES Message TO regularUsers",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "MATCH", Graphs: []string{"neo4j"},
				Roles: []string{"regularUsers"}, Kind: "node", Types: []string{"Message"},
				Properties: []string{"language", "length"},
			},
		},
		"omitted element scope is elements star": {
			statement: "GRANT TRAVERSE ON GRAPH * TO r",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "TRAVERSE", Graphs: []string{"*"},
				Roles: []string{"r"}, Kind: "elements", Types: []string{"*"},
			},
		},
		"wildcard element type": {
			statement: "GRANT CREATE ON GRAPH * NODES * TO builder",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "CREATE", Graphs: []string{"*"},
				Roles: []string{"builder"}, Kind: "node", Types: []string{"*"},
			},
		},
		"home graph": {
			statement: "GRANT SET PROPERTY {*} ON HOME GRAPH ELEMENTS * TO regularUsers",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "SET PROPERTY", Roles: []string{"regularUsers"},
				Kind: "elements", Types: []string{"*"}, Properties: []string{"*"},
			},
		},
		"label qualifier": {
			statement: "DENY REMOVE LABEL foo ON GRAPH * TO regularUsers",
			want: fineGrainedPrivilegeCommand{
				Verb: "deny", Privilege: "REMOVE LABEL", Graphs: []string{"*"},
				Roles: []string{"regularUsers"}, Kind: "elements", Types: []string{"foo"},
			},
		},
		"wildcard label qualifier": {
			statement: "GRANT SET LABEL * ON GRAPH neo4j TO regularUsers",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "SET LABEL", Graphs: []string{"neo4j"},
				Roles: []string{"regularUsers"}, Kind: "elements", Types: []string{"*"},
			},
		},
		"write": {
			statement: "GRANT WRITE ON GRAPH neo4j TO regularUsers",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "WRITE", Graphs: []string{"neo4j"},
				Roles: []string{"regularUsers"}, Kind: "elements",
			},
		},
		"merge": {
			statement: "GRANT MERGE {*} ON GRAPH * ELEMENTS * TO regularUsers",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "MERGE", Graphs: []string{"*"},
				Roles: []string{"regularUsers"}, Kind: "elements", Types: []string{"*"},
				Properties: []string{"*"},
			},
		},
		"all graph privileges": {
			statement: "GRANT ALL GRAPH PRIVILEGES ON GRAPH neo4j TO regularUsers",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "ALL GRAPH PRIVILEGES", Graphs: []string{"neo4j"},
				Roles: []string{"regularUsers"}, Kind: "elements",
			},
		},
		"all privileges of the graph scope": {
			statement: "GRANT ALL PRIVILEGES ON GRAPH neo4j TO admin",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "ALL GRAPH PRIVILEGES", Graphs: []string{"neo4j"},
				Roles: []string{"admin"}, Kind: "elements",
			},
		},
		"revoke granted on home graph": {
			statement: "REVOKE GRANT TRAVERSE ON HOME GRAPH NODES Post FROM regularUsers",
			want: fineGrainedPrivilegeCommand{
				Verb: "revoke_grant", Privilege: "TRAVERSE", Roles: []string{"regularUsers"},
				Kind: "node", Types: []string{"Post"},
			},
		},
		"revoke denied with properties and roles": {
			statement: "REVOKE DENY READ {secret} ON GRAPH neo4j RELATIONSHIPS KNOWS, LIKES FROM reader, `other role`",
			want: fineGrainedPrivilegeCommand{
				Verb: "revoke_deny", Privilege: "READ", Graphs: []string{"neo4j"},
				Roles: []string{"reader", "other role"}, Kind: "relationship",
				Types: []string{"KNOWS", "LIKES"}, Properties: []string{"secret"},
			},
		},
		"property-based pattern": {
			statement: "GRANT READ { address } ON GRAPH * FOR (n:Email|Website) WHERE n.domain = 'exampledomain.com' TO regularUsers",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "READ", Graphs: []string{"*"},
				Roles: []string{"regularUsers"}, Kind: "node", Properties: []string{"address"},
				Predicate: "FOR (n:Email|Website) WHERE n.domain = 'exampledomain.com'",
			},
		},
		"property-based pattern with property map": {
			statement: "GRANT READ { address } ON GRAPH * FOR (:Email|Website {domain: 'exampledomain.com'}) TO regularUsers",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "READ", Graphs: []string{"*"},
				Roles: []string{"regularUsers"}, Kind: "node", Properties: []string{"address"},
				Predicate: "FOR (:Email|Website {domain: 'exampledomain.com'})",
			},
		},
		"denied property-based pattern": {
			statement: "DENY MATCH {*} ON GRAPH * FOR (n) WHERE n.classification <> 'UNCLASSIFIED' TO regularUsers",
			want: fineGrainedPrivilegeCommand{
				Verb: "deny", Privilege: "MATCH", Graphs: []string{"*"},
				Roles: []string{"regularUsers"}, Kind: "node", Properties: []string{"*"},
				Predicate: "FOR (n) WHERE n.classification <> 'UNCLASSIFIED'",
			},
		},
		"pattern variable named as the roles keyword": {
			statement: "GRANT READ {*} ON GRAPH * FOR (n) WHERE n.to > 5 TO reader",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "READ", Graphs: []string{"*"},
				Roles: []string{"reader"}, Kind: "node", Properties: []string{"*"},
				Predicate: "FOR (n) WHERE n.to > 5",
			},
		},
		"pattern string holding the roles keyword": {
			statement: "GRANT READ {*} ON GRAPH * FOR (n) WHERE n.note = 'TO alice' TO reader",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "READ", Graphs: []string{"*"},
				Roles: []string{"reader"}, Kind: "node", Properties: []string{"*"},
				Predicate: "FOR (n) WHERE n.note = 'TO alice'",
			},
		},
		"quoting and case": {
			statement: "grant traverse on graph `My Graph`, `a,b` node `Per son` to `R`, `r 2`;",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "TRAVERSE", Graphs: []string{"My Graph", "a,b"},
				Roles: []string{"R", "r 2"}, Kind: "node", Types: []string{"Per son"},
			},
		},
		"escaped backticks": {
			statement: "GRANT TRAVERSE ON GRAPH `a``b` NODES `x``y` TO `r``s`",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "TRAVERSE", Graphs: []string{"a`b"},
				Roles: []string{"r`s"}, Kind: "node", Types: []string{"x`y"},
			},
		},
		"quoted property names": {
			statement: "GRANT READ {`first name`, age} ON GRAPH * NODES `Person` TO r",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "READ", Graphs: []string{"*"}, Roles: []string{"r"},
				Kind: "node", Types: []string{"Person"}, Properties: []string{"first name", "age"},
			},
		},
		"line comment after the roles": {
			statement: "GRANT TRAVERSE ON GRAPH * TO r // grants stay",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "TRAVERSE", Graphs: []string{"*"}, Roles: []string{"r"},
				Kind: "elements", Types: []string{"*"},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			requireFineGrainedPrivilege(t, test.statement, test.want)
		})
	}
}

func TestParseFineGrainedPrivilegeImmutability(t *testing.T) {
	for name, test := range map[string]struct {
		statement string
		want      fineGrainedPrivilegeCommand
	}{
		"grant": {
			statement: "GRANT IMMUTABLE TRAVERSE ON GRAPH * TO locked",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Immutable: true, Privilege: "TRAVERSE", Graphs: []string{"*"},
				Roles: []string{"locked"}, Kind: "elements", Types: []string{"*"},
			},
		},
		"deny": {
			statement: "DENY IMMUTABLE TRAVERSE ON GRAPH * TO locked",
			want: fineGrainedPrivilegeCommand{
				Verb: "deny", Immutable: true, Privilege: "TRAVERSE", Graphs: []string{"*"},
				Roles: []string{"locked"}, Kind: "elements", Types: []string{"*"},
			},
		},
		"revoke before the marker": {
			statement: "REVOKE IMMUTABLE GRANT READ {*} ON GRAPH neo4j FROM locked",
			want: fineGrainedPrivilegeCommand{
				Verb: "revoke_grant", Immutable: true, Privilege: "READ", Graphs: []string{"neo4j"},
				Roles: []string{"locked"}, Kind: "elements", Types: []string{"*"},
				Properties: []string{"*"},
			},
		},
		"revoke after the marker": {
			statement: "REVOKE GRANT IMMUTABLE READ {*} ON GRAPH neo4j FROM locked",
			want: fineGrainedPrivilegeCommand{
				Verb: "revoke_grant", Immutable: true, Privilege: "READ", Graphs: []string{"neo4j"},
				Roles: []string{"locked"}, Kind: "elements", Types: []string{"*"},
				Properties: []string{"*"},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			requireFineGrainedPrivilege(t, test.statement, test.want)
		})
	}
}

func TestParseFineGrainedPrivilegeDatabaseCommands(t *testing.T) {
	for name, test := range map[string]struct {
		statement string
		want      fineGrainedPrivilegeCommand
	}{
		"access": {
			statement: "GRANT ACCESS ON DATABASE neo4j TO reader",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "ACCESS", Graphs: []string{"neo4j"},
				Roles: []string{"reader"}, Kind: "database",
			},
		},
		"create index": {
			statement: "GRANT CREATE INDEXES ON DATABASE * TO architect",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "CREATE INDEX", Graphs: []string{"*"},
				Roles: []string{"architect"}, Kind: "database",
			},
		},
		"index management": {
			statement: "GRANT INDEX ON DATABASE neo4j, docs TO architect",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "INDEX MANAGEMENT", Graphs: []string{"neo4j", "docs"},
				Roles: []string{"architect"}, Kind: "database",
			},
		},
		"constraint management on home database": {
			statement: "DENY CONSTRAINT MANAGEMENT ON HOME DATABASE TO architect",
			want: fineGrainedPrivilegeCommand{
				Verb: "deny", Privilege: "CONSTRAINT MANAGEMENT", Roles: []string{"architect"},
				Kind: "database",
			},
		},
		"drop constraints": {
			statement: "REVOKE DROP CONSTRAINTS ON DATABASE neo4j FROM architect",
			want: fineGrainedPrivilegeCommand{
				Verb: "revoke", Privilege: "DROP CONSTRAINT", Graphs: []string{"neo4j"},
				Roles: []string{"architect"}, Kind: "database",
			},
		},
		"create new label": {
			statement: "GRANT CREATE NEW NODE LABELS ON DATABASE * TO architect",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "CREATE NEW LABEL", Graphs: []string{"*"},
				Roles: []string{"architect"}, Kind: "database",
			},
		},
		"create new relationship type": {
			statement: "GRANT CREATE NEW RELATIONSHIP TYPE ON DATABASE * TO architect",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "CREATE NEW RELATIONSHIP TYPE", Graphs: []string{"*"},
				Roles: []string{"architect"}, Kind: "database",
			},
		},
		"create new property name": {
			statement: "GRANT CREATE NEW PROPERTY NAMES ON HOME DATABASE TO architect",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "CREATE NEW PROPERTY NAME", Roles: []string{"architect"},
				Kind: "database",
			},
		},
		"name management": {
			statement: "DENY NAME ON DATABASE neo4j TO architect",
			want: fineGrainedPrivilegeCommand{
				Verb: "deny", Privilege: "NAME MANAGEMENT", Graphs: []string{"neo4j"},
				Roles: []string{"architect"}, Kind: "database",
			},
		},
		"show transaction": {
			statement: "GRANT SHOW TRANSACTIONS ON DATABASE neo4j TO auditor",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "SHOW TRANSACTION", Graphs: []string{"neo4j"},
				Roles: []string{"auditor"}, Kind: "database",
			},
		},
		"terminate transaction": {
			statement: "GRANT TERMINATE TRANSACTIONS ON DATABASE * TO auditor",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "TERMINATE TRANSACTION", Graphs: []string{"*"},
				Roles: []string{"auditor"}, Kind: "database",
			},
		},
		"transaction management": {
			statement: "GRANT TRANSACTION ON HOME DATABASE TO auditor",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "TRANSACTION MANAGEMENT", Roles: []string{"auditor"},
				Kind: "database",
			},
		},
		"all database privileges": {
			statement: "GRANT ALL DATABASE PRIVILEGES ON DATABASE * TO admin",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "ALL DATABASE PRIVILEGES", Graphs: []string{"*"},
				Roles: []string{"admin"}, Kind: "database",
			},
		},
		"all privileges of the database scope": {
			statement: "GRANT ALL ON DATABASE neo4j TO admin",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "ALL DATABASE PRIVILEGES", Graphs: []string{"neo4j"},
				Roles: []string{"admin"}, Kind: "database",
			},
		},
		"stop on all databases": {
			statement: "GRANT STOP ON DATABASES * TO ops",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "STOP", Graphs: []string{"*"},
				Roles: []string{"ops"}, Kind: "database",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			requireFineGrainedPrivilege(t, test.statement, test.want)
		})
	}
}

func TestParseFineGrainedPrivilegeDBMSCommands(t *testing.T) {
	for name, test := range map[string]struct {
		statement string
		want      fineGrainedPrivilegeCommand
	}{
		"create role": {
			statement: "GRANT CREATE ROLE ON DBMS TO securityAdmin",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "CREATE ROLE", Roles: []string{"securityAdmin"},
				Kind: "dbms",
			},
		},
		"role management": {
			statement: "GRANT ROLE MANAGEMENT ON DBMS TO securityAdmin",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "ROLE MANAGEMENT", Roles: []string{"securityAdmin"},
				Kind: "dbms",
			},
		},
		"set user home database": {
			statement: "GRANT SET USER HOME DATABASE ON DBMS TO securityAdmin",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "SET USER HOME DATABASE", Roles: []string{"securityAdmin"},
				Kind: "dbms",
			},
		},
		"set passwords": {
			statement: "GRANT SET PASSWORDS ON DBMS TO securityAdmin",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "SET PASSWORD", Roles: []string{"securityAdmin"},
				Kind: "dbms",
			},
		},
		"impersonate": {
			statement: "GRANT IMPERSONATE ON DBMS TO support",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "IMPERSONATE", Roles: []string{"support"}, Kind: "dbms",
			},
		},
		"create alias": {
			statement: "GRANT CREATE ALIAS ON DBMS TO aliasAdder",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "CREATE ALIAS", Roles: []string{"aliasAdder"},
				Kind: "dbms",
			},
		},
		"server management": {
			statement: "GRANT SERVER MANAGEMENT ON DBMS TO serverManager",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "SERVER MANAGEMENT", Roles: []string{"serverManager"},
				Kind: "dbms",
			},
		},
		"show servers": {
			statement: "GRANT SHOW SERVERS ON DBMS TO serverViewer",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "SHOW SERVERS", Roles: []string{"serverViewer"},
				Kind: "dbms",
			},
		},
		"privilege management": {
			statement: "GRANT PRIVILEGE MANAGEMENT ON DBMS TO root",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "PRIVILEGE MANAGEMENT", Roles: []string{"root"},
				Kind: "dbms",
			},
		},
		"show privileges": {
			statement: "GRANT SHOW PRIVILEGES ON DBMS TO root",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "SHOW PRIVILEGE", Roles: []string{"root"}, Kind: "dbms",
			},
		},
		"set database access": {
			statement: "GRANT SET DATABASE ACCESS ON DBMS TO dba",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "SET DATABASE ACCESS", Roles: []string{"dba"},
				Kind: "dbms",
			},
		},
		"composite database management": {
			statement: "GRANT COMPOSITE DATABASE MANAGEMENT ON DBMS TO dba",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "COMPOSITE DATABASE MANAGEMENT", Roles: []string{"dba"},
				Kind: "dbms",
			},
		},
		"create composite database": {
			statement: "GRANT CREATE COMPOSITE DATABASE ON DBMS TO dba",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "CREATE COMPOSITE DATABASE", Roles: []string{"dba"},
				Kind: "dbms",
			},
		},
		"deny database management": {
			statement: "DENY DATABASE MANAGEMENT ON DBMS TO customAdministrator",
			want: fineGrainedPrivilegeCommand{
				Verb: "deny", Privilege: "DATABASE MANAGEMENT",
				Roles: []string{"customAdministrator"}, Kind: "dbms",
			},
		},
		"revoke granted assign role": {
			statement: "REVOKE GRANT ASSIGN ROLE ON DBMS FROM securityAdmin",
			want: fineGrainedPrivilegeCommand{
				Verb: "revoke_grant", Privilege: "ASSIGN ROLE", Roles: []string{"securityAdmin"},
				Kind: "dbms",
			},
		},
		"all privileges of the dbms scope": {
			statement: "GRANT ALL ON DBMS TO root",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "ALL DBMS PRIVILEGES", Roles: []string{"root"}, Kind: "dbms",
			},
		},
		"all dbms privileges": {
			statement: "GRANT ALL DBMS PRIVILEGES ON DBMS TO root",
			want: fineGrainedPrivilegeCommand{
				Verb: "grant", Privilege: "ALL DBMS PRIVILEGES", Roles: []string{"root"}, Kind: "dbms",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			requireFineGrainedPrivilege(t, test.statement, test.want)
		})
	}
}

// The procedure and user-defined function privilege family and role assignment
// belong to their own readers: this parser must not claim them.
func TestParseFineGrainedPrivilegeLeavesOtherPrivilegeCommands(t *testing.T) {
	for _, statement := range []string{
		"GRANT EXECUTE PROCEDURE db.x ON DBMS TO r",
		"GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO r",
		"GRANT IMMUTABLE EXECUTE PROCEDURE db.ping ON DBMS TO r",
		"DENY EXECUTE PROCEDURE db.labels ON DBMS TO r",
		"REVOKE EXECUTE PROCEDURE db.x ON DBMS FROM r",
		"REVOKE GRANT IMMUTABLE EXECUTE PROCEDURES db.cdc.query ON DBMS FROM r",
		"GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO r",
		"GRANT EXECUTE USER DEFINED FUNCTION apoc.coll.* ON DBMS TO r",
		"GRANT ROLE admin TO bob",
		"GRANT ROLES a, b TO bob",
		"REVOKE ROLE admin FROM bob",
		"REVOKE GRANT ROLE admin FROM bob",
		"CREATE ROLE security",
		"SHOW PRIVILEGES",
		"SHOW ROLE admin PRIVILEGES",
		"MATCH (n) RETURN n",
		"",
		"   ",
	} {
		command, ok, err := parseFineGrainedPrivilegeCommand(t.Context(), statement)
		require.NoError(t, err, statement)
		require.False(t, ok, statement)
		require.Equal(t, fineGrainedPrivilegeCommand{}, command, statement)
	}
}

// A fine-grained privilege command is never a procedure privilege, so the two
// readers own disjoint statements.
func TestParseFineGrainedPrivilegeIsNotProcedurePrivilege(t *testing.T) {
	ctx := t.Context()
	for _, statement := range []string{
		"GRANT TRAVERSE ON GRAPH * NODES Post TO r",
		"DENY MATCH {*} ON GRAPH * FOR (n) WHERE n.classification <> 'UNCLASSIFIED' TO r",
		"REVOKE GRANT TRAVERSE ON HOME GRAPH NODES Post FROM r",
		"GRANT ACCESS ON DATABASE neo4j TO r",
		"GRANT CREATE NEW LABEL ON DATABASE * TO r",
		"GRANT CREATE ROLE ON DBMS TO r",
		"GRANT ALL DBMS PRIVILEGES ON DBMS TO r",
	} {
		_, ok, err := parseFineGrainedPrivilegeCommand(ctx, statement)
		require.True(t, ok, statement)
		require.NoError(t, err, statement)
		require.False(t, isProcedurePrivilegeCommand(ctx, statement), statement)
	}
}

func TestParseFineGrainedPrivilegeSyntaxErrors(t *testing.T) {
	for name, test := range map[string]struct {
		statement string
		detail    string
	}{
		"verb only": {
			statement: "GRANT",
			detail:    "expected a privilege action",
		},
		"revoke marker only": {
			statement: "REVOKE GRANT",
			detail:    "expected a privilege action",
		},
		"unknown action": {
			statement: "GRANT BANANA ON GRAPH * TO r",
			detail:    "unsupported privilege action",
		},
		"two privileges": {
			statement: "GRANT TRAVERSE, READ {*} ON GRAPH * TO r",
			detail:    "expected ON followed by the privilege scope",
		},
		"missing element types": {
			statement: "GRANT TRAVERSE ON GRAPH * NODES",
			detail:    "expected a label or relationship type",
		},
		"element type wildcard joined to a list": {
			statement: "GRANT TRAVERSE ON GRAPH * NODES *, Person TO r",
			detail:    "a label or relationship type wildcard stands alone",
		},
		"missing property list": {
			statement: "GRANT READ ON GRAPH * TO r",
			detail:    "expected a property list",
		},
		"empty property list": {
			statement: "GRANT READ {} ON GRAPH * TO r",
			detail:    "expected a property name",
		},
		"unclosed property list": {
			statement: "GRANT READ {name ON GRAPH * TO r",
			detail:    "expected the end of the property list",
		},
		"missing label qualifier": {
			statement: "GRANT SET LABEL",
			detail:    "expected a label or relationship type",
		},
		"missing scope target": {
			statement: "GRANT TRAVERSE ON GRAPH , TO r",
			detail:    "expected a graph or database name",
		},
		"scope wildcard joined to a list": {
			statement: "GRANT TRAVERSE ON GRAPH *,",
			detail:    "a graph or database wildcard stands alone",
		},
		"missing home scope": {
			statement: "GRANT TRAVERSE ON HOME TO r",
			detail:    "expected GRAPH or DATABASE after HOME",
		},
		"missing roles": {
			statement: "GRANT TRAVERSE ON GRAPH *",
			detail:    "expected TO followed by a role",
		},
		"missing role name": {
			statement: "GRANT TRAVERSE ON GRAPH * TO",
			detail:    "expected a role name",
		},
		"trailing comma in the roles": {
			statement: "GRANT TRAVERSE ON GRAPH * TO r,",
			detail:    "expected a role name",
		},
		"text after the roles": {
			statement: "GRANT TRAVERSE ON GRAPH * TO r extra",
			detail:    "unexpected text after the roles",
		},
		"second terminator": {
			statement: "GRANT TRAVERSE ON GRAPH * TO r ; ;",
			detail:    "unexpected text after the roles",
		},
		"graph privilege on a database": {
			statement: "GRANT TRAVERSE ON DATABASE neo4j TO r",
			detail:    "TRAVERSE is not granted ON DATABASE",
		},
		"database privilege on a graph": {
			statement: "GRANT ACCESS ON GRAPH * TO r",
			detail:    "ACCESS is not granted ON GRAPH",
		},
		"database privilege on dbms": {
			statement: "GRANT ACCESS ON DBMS TO r",
			detail:    "ACCESS is not granted ON DBMS",
		},
		"dbms privilege on a graph": {
			statement: "GRANT CREATE ROLE ON GRAPH * TO r",
			detail:    "CREATE ROLE is not granted ON GRAPH",
		},
		"all graph privileges on dbms": {
			statement: "GRANT ALL GRAPH PRIVILEGES ON DBMS TO r",
			detail:    "ALL GRAPH PRIVILEGES is not granted ON DBMS",
		},
		"write with an element scope": {
			statement: "GRANT WRITE ON GRAPH * NODES * TO r",
			detail:    "WRITE does not take an element scope",
		},
		"write privilege with a pattern": {
			statement: "GRANT CREATE ON GRAPH * FOR (n) WHERE n.x = 1 TO r",
			detail:    "CREATE does not take a FOR pattern",
		},
		"unparenthesized pattern": {
			statement: "GRANT TRAVERSE ON GRAPH * FOR n TO r",
			detail:    "expected a parenthesized FOR pattern",
		},
		"unclosed pattern": {
			statement: "GRANT TRAVERSE ON GRAPH * FOR (n TO r",
			detail:    "unclosed FOR pattern",
		},
		"empty predicate": {
			statement: "GRANT READ {*} ON GRAPH * FOR (n) WHERE TO r",
			detail:    "expected a predicate after WHERE",
		},
		"missing predicate": {
			statement: "GRANT READ {*} ON GRAPH * FOR (n) WHERE",
			detail:    "expected a predicate after WHERE",
		},
		"predicate without roles": {
			statement: "GRANT READ {*} ON GRAPH * FOR (n) WHERE n.x = 1",
			detail:    "expected TO followed by a role",
		},
		"unclosed user list": {
			statement: "GRANT IMPERSONATE (bob ON DBMS TO r",
			detail:    "expected ')' after usernames",
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, ok, err := parseFineGrainedPrivilegeCommand(t.Context(), test.statement)
			require.True(t, ok, test.statement)
			require.ErrorContains(t, err, test.detail, test.statement)
			var semantic *SemanticError
			require.ErrorAs(t, err, &semantic, test.statement)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semantic.Code, test.statement)
		})
	}
}

func TestParseFineGrainedPrivilegeHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	command, ok, err := parseFineGrainedPrivilegeCommand(ctx, "GRANT TRAVERSE ON GRAPH * TO r")
	require.Equal(t, fineGrainedPrivilegeCommand{}, command)
	require.False(t, ok)
	require.ErrorIs(t, err, context.Canceled)
}

func TestParseFineGrainedPrivilegeUserSelectors(t *testing.T) {
	for _, statement := range []string{
		"GRANT IMPERSONATE (bob, `Jane Doe`) ON DBMS TO r",
		"DENY SHOW TRANSACTION (bob, `Jane Doe`) ON HOME DATABASE TO r",
		"REVOKE GRANT TRANSACTION MANAGEMENT (bob, `Jane Doe`) ON DATABASE neo4j FROM r",
	} {
		command, handled, err := parseFineGrainedPrivilegeCommand(t.Context(), statement)
		require.True(t, handled, statement)
		require.NoError(t, err, statement)
		require.Equal(t, []string{"bob", "Jane Doe"}, command.Users, statement)
	}
}
