package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// A refused statement gets Neo4j 5.26's text naming the executing principal:
// the schema action and database for schema (assertSchemaWrites), the
// procedure for an admin procedure (ProcedureRegistry), "Permission denied."
// for an administration command, and Neo4j's static access-mode wording for
// a write. Reads keep NornicDB's message (Neo4j hides what it may not read).
func TestNeo4jDenialTexts(t *testing.T) {
	ctx := WithRequestIdentity(context.Background(), &RequestIdentity{User: &AuthenticatedUser{Name: "alice", Roles: []string{"reader", "auditor", "reader"}}})
	ctx = WithDatabasePermissionResolver(ctx, "tenant", func(_, permission string) bool {
		return permission == "access"
	})
	for query, message := range map[string]string{
		"CREATE INDEX person_name FOR (n:Person) ON (n.name)":                                   "Schema operation 'create_index' on database 'tenant' is not allowed for user 'alice' with roles [auditor, reader].",
		"DROP INDEX person_name":                                                                "Schema operation 'drop_index' on database 'tenant' is not allowed for user 'alice' with roles [auditor, reader].",
		"CREATE CONSTRAINT person_id FOR (n:Person) REQUIRE n.id IS UNIQUE":                     "Schema operation 'create_constraint' on database 'tenant' is not allowed for user 'alice' with roles [auditor, reader].",
		"DROP CONSTRAINT person_id":                                                             "Schema operation 'drop_constraint' on database 'tenant' is not allowed for user 'alice' with roles [auditor, reader].",
		"CALL db.index.fulltext.createNodeIndex('docs', ['Doc'], ['text'])":                     "Schema operation 'create_index' on database 'tenant' is not allowed for user 'alice' with roles [auditor, reader].",
		"CALL db.index.fulltext.drop('docs')":                                                   "Schema operation 'drop_index' on database 'tenant' is not allowed for user 'alice' with roles [auditor, reader].",
		"CALL dbms.listConfig()":                                                                "Executing admin procedure 'dbms.listConfig' permission has not been granted for user 'alice' with roles [auditor, reader].",
		"SHOW USERS":                                                                            "Permission denied.",
		"CREATE (:Person {name: 'x'})":                                                          "Write operations are not allowed for user 'alice' with roles [auditor, reader].",
		"MATCH (n) RETURN n":                                                                    "Read operations require read permission",
	} {
		err := AuthorizeQuery(ctx, query)
		var denied *PermissionDeniedError
		require.True(t, errors.As(err, &denied), query)
		require.Equal(t, "Neo.ClientError.Security.Forbidden", denied.BoltErrorCode(), query)
		require.Equal(t, message, denied.StatusMessage(), query)
	}

	// Without a principal Neo4j's description names no user and no roles.
	bare := WithDatabasePermissionResolver(context.Background(), "tenant", func(_, permission string) bool { return permission == "access" })
	var denied *PermissionDeniedError
	require.True(t, errors.As(AuthorizeQuery(bare, "CREATE (:Person)"), &denied))
	require.Equal(t, "Write operations are not allowed for user '' with no roles.", denied.StatusMessage())
}
