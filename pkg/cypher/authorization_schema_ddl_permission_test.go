package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-300 bounded correction: the schema permission classifier (authorization.go)
// kept its own short list of ordinary schema DDL, so CREATE TEXT / POINT / LOOKUP
// INDEX were admitted as plain writes. A principal holding read and write but no
// schema permission could create them through the public Execute path and the
// schema changed. The classifier now reads the canonical owner of ordinary schema
// DDL (isOrdinarySchemaDDL), so these tests hold every supported form to the same
// boundary through Execute: a schema mutation is denied with PermissionDenied
// (schema) and leaves the schema untouched, a schema-authorized principal keeps
// the supported behavior, and the same words in a literal, a comment or a name
// need no schema permission.

const (
	nor300ShowIndexNames      = "SHOW INDEXES YIELD name RETURN name ORDER BY name"
	nor300ShowConstraintNames = "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name"
)

// nor300SchemaWithoutPermission is a principal that may read and write data but
// holds no schema permission.
func nor300SchemaWithoutPermission() context.Context {
	return WithPermissionChecker(context.Background(), func(permission string) bool {
		return permission == "read" || permission == "write"
	})
}

// nor300SchemaPermissions is a principal holding every entitlement.
func nor300SchemaPermissions() context.Context {
	return WithPermissionChecker(context.Background(), func(string) bool { return true })
}

// TestExecuteDeniesIndexAndConstraintDDLWithoutSchemaPermission is the NOR-300
// regression: every supported schema DDL form, including CREATE TEXT, POINT and
// LOOKUP INDEX, must reach a read+write principal as PermissionDenied(schema)
// and must not change the schema. Declaration forms start from an untouched
// schema; DROP forms start from an existing object, which must still be there
// afterwards.
func TestExecuteDeniesIndexAndConstraintDDLWithoutSchemaPermission(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		cases := []struct {
			name  string
			setup []string
			query string
		}{
			{
				name:  "index",
				query: "CREATE INDEX nor300_denied_index FOR (n:D) ON (n.p)",
			},
			{
				name:  "range index",
				query: "CREATE RANGE INDEX nor300_denied_range FOR (n:D) ON (n.p)",
			},
			{
				name:  "text index",
				query: "CREATE TEXT INDEX nor300_denied_text FOR (n:D) ON (n.t)",
			},
			{
				name:  "point index",
				query: "CREATE POINT INDEX nor300_denied_point FOR (n:D) ON (n.loc)",
			},
			{
				name:  "vector index",
				query: "CREATE VECTOR INDEX nor300_denied_vector FOR (n:D) ON (n.e) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}",
			},
			{
				name:  "fulltext index",
				query: "CREATE FULLTEXT INDEX nor300_denied_full FOR (n:D) ON EACH [n.t]",
			},
			{
				name:  "node lookup index",
				setup: []string{"DROP INDEX " + storage.DefaultNodeLookupIndexName},
				query: "CREATE LOOKUP INDEX nor300_denied_lookup FOR (n) ON EACH labels(n)",
			},
			{
				name:  "relationship lookup index",
				setup: []string{"DROP INDEX " + storage.DefaultRelationshipLookupIndexName},
				query: "CREATE LOOKUP INDEX nor300_denied_rel_lookup FOR ()-[r]-() ON EACH type(r)",
			},
			{
				name:  "unique constraint",
				query: "CREATE CONSTRAINT nor300_denied_unique IF NOT EXISTS FOR (n:D) REQUIRE n.id IS UNIQUE",
			},
			{
				// A leading comment is trivia, not statement text: the statement
				// keeps its schema requirement behind it.
				name:  "block commented text index",
				query: "/* migration */ CREATE TEXT INDEX nor300_denied_commented_text FOR (n:D) ON (n.t)",
			},
			{
				name:  "line commented point index",
				query: "// migration\nCREATE POINT INDEX nor300_denied_commented_point FOR (n:D) ON (n.loc)",
			},
			{
				// A CYPHER version preamble keeps its own route; the statement it
				// runs is authorized again on its own text.
				name:  "cypher preamble text index",
				query: "CYPHER 5 CREATE TEXT INDEX nor300_denied_preamble_text FOR (n:D) ON (n.t)",
			},
			{
				name:  "drop index",
				setup: []string{"CREATE INDEX nor300_denied_drop FOR (n:D) ON (n.p)"},
				query: "DROP INDEX nor300_denied_drop",
			},
			{
				name:  "drop text index",
				setup: []string{"CREATE TEXT INDEX nor300_denied_drop_text FOR (n:D) ON (n.t)"},
				query: "DROP INDEX nor300_denied_drop_text IF EXISTS",
			},
			{
				name:  "drop constraint",
				setup: []string{"CREATE CONSTRAINT nor300_denied_drop_unique IF NOT EXISTS FOR (n:D) REQUIRE n.id IS UNIQUE"},
				query: "DROP CONSTRAINT nor300_denied_drop_unique IF EXISTS",
			},
		}
		for _, test := range cases {
			t.Run(test.name, func(t *testing.T) {
				exec, _ := newTestExecutor(t)
				for _, statement := range test.setup {
					_, err := exec.Execute(t.Context(), statement, nil)
					require.NoError(t, err, statement)
				}
				indexes := readSchemaNames(t, t.Context(), exec, nor300ShowIndexNames)
				constraints := readSchemaNames(t, t.Context(), exec, nor300ShowConstraintNames)

				_, err := exec.Execute(nor300SchemaWithoutPermission(), test.query, nil)

				var denied *PermissionDeniedError
				require.True(t, errors.As(err, &denied), "%s: %v", test.query, err)
				require.Equal(t, "schema", denied.Permission, test.query)
				require.Equal(t, "Neo.ClientError.Security.Forbidden", denied.BoltErrorCode(), test.query)
				require.Equal(t, "Schema operations require schema permission", denied.StatusMessage(), test.query)
				require.Equal(t, indexes, readSchemaNames(t, t.Context(), exec, nor300ShowIndexNames), test.query)
				require.Equal(t, constraints, readSchemaNames(t, t.Context(), exec, nor300ShowConstraintNames), test.query)
			})
		}
	})
}

// TestExecuteKeepsSupportedSchemaDDLWithSchemaPermission is the positive control
// for the correction: a principal holding schema permission still creates and
// drops every supported index and constraint form, and the created object is
// visible in SHOW INDEXES / SHOW CONSTRAINTS.
func TestExecuteKeepsSupportedSchemaDDLWithSchemaPermission(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		cases := []struct {
			name   string
			setup  []string
			create string
			drop   string
			object string
			show   string
		}{
			{
				name:   "index",
				create: "CREATE INDEX nor300_allowed_index FOR (n:D) ON (n.p)",
				drop:   "DROP INDEX nor300_allowed_index",
				object: "nor300_allowed_index",
				show:   nor300ShowIndexNames,
			},
			{
				name:   "range index",
				create: "CREATE RANGE INDEX nor300_allowed_range FOR (n:D) ON (n.p)",
				drop:   "DROP INDEX nor300_allowed_range",
				object: "nor300_allowed_range",
				show:   nor300ShowIndexNames,
			},
			{
				name:   "text index",
				create: "CREATE TEXT INDEX nor300_allowed_text FOR (n:D) ON (n.t)",
				drop:   "DROP INDEX nor300_allowed_text",
				object: "nor300_allowed_text",
				show:   nor300ShowIndexNames,
			},
			{
				name:   "point index",
				create: "CREATE POINT INDEX nor300_allowed_point FOR (n:D) ON (n.loc)",
				drop:   "DROP INDEX nor300_allowed_point",
				object: "nor300_allowed_point",
				show:   nor300ShowIndexNames,
			},
			{
				name:   "vector index",
				create: "CREATE VECTOR INDEX nor300_allowed_vector FOR (n:D) ON (n.e) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}",
				drop:   "DROP INDEX nor300_allowed_vector",
				object: "nor300_allowed_vector",
				show:   nor300ShowIndexNames,
			},
			{
				name:   "fulltext index",
				create: "CREATE FULLTEXT INDEX nor300_allowed_full FOR (n:D) ON EACH [n.t]",
				drop:   "DROP INDEX nor300_allowed_full",
				object: "nor300_allowed_full",
				show:   nor300ShowIndexNames,
			},
			{
				name:   "node lookup index",
				setup:  []string{"DROP INDEX " + storage.DefaultNodeLookupIndexName},
				create: "CREATE LOOKUP INDEX nor300_allowed_lookup FOR (n) ON EACH labels(n)",
				drop:   "DROP INDEX nor300_allowed_lookup",
				object: "nor300_allowed_lookup",
				show:   nor300ShowIndexNames,
			},
			{
				name:   "relationship lookup index",
				setup:  []string{"DROP INDEX " + storage.DefaultRelationshipLookupIndexName},
				create: "CREATE LOOKUP INDEX nor300_allowed_rel_lookup FOR ()-[r]-() ON EACH type(r)",
				drop:   "DROP INDEX nor300_allowed_rel_lookup",
				object: "nor300_allowed_rel_lookup",
				show:   nor300ShowIndexNames,
			},
			{
				name:   "unique constraint",
				create: "CREATE CONSTRAINT nor300_allowed_unique IF NOT EXISTS FOR (n:D) REQUIRE n.id IS UNIQUE",
				drop:   "DROP CONSTRAINT nor300_allowed_unique IF EXISTS",
				object: "nor300_allowed_unique",
				show:   nor300ShowConstraintNames,
			},
			{
				// The corrected classifier reads a statement behind its leading
				// trivia; the supported behavior stays the same there too.
				name:   "block commented text index",
				create: "/* migration */ CREATE TEXT INDEX nor300_allowed_commented_text FOR (n:D) ON (n.t)",
				drop:   "DROP INDEX nor300_allowed_commented_text",
				object: "nor300_allowed_commented_text",
				show:   nor300ShowIndexNames,
			},
		}
		for _, test := range cases {
			t.Run(test.name, func(t *testing.T) {
				exec, _ := newTestExecutor(t)
				ctx := nor300SchemaPermissions()
				for _, statement := range test.setup {
					_, err := exec.Execute(ctx, statement, nil)
					require.NoError(t, err, statement)
				}

				_, err := exec.Execute(ctx, test.create, nil)
				require.NoError(t, err, test.create)
				require.Contains(t, readSchemaNames(t, t.Context(), exec, test.show), test.object, test.create)

				_, err = exec.Execute(ctx, test.drop, nil)
				require.NoError(t, err, test.drop)
				require.NotContains(t, readSchemaNames(t, t.Context(), exec, test.show), test.object, test.drop)
			})
		}
	})
}

// TestExecuteDoesNotRequireSchemaPermissionForIndexWordsInLiteralsCommentsAndNames
// is the other side of the boundary: the same DDL words in a string literal, a
// comment or a name are not a schema statement, so a principal without schema
// permission keeps its ordinary read or write behavior and nothing is declared.
func TestExecuteDoesNotRequireSchemaPermissionForIndexWordsInLiteralsCommentsAndNames(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		readOnly := WithPermissionChecker(context.Background(), func(permission string) bool {
			return permission == "read"
		})
		readWrite := nor300SchemaWithoutPermission()

		reads := []struct {
			name  string
			query string
			value interface{}
		}{
			{
				name:  "string literal",
				query: "RETURN 'CREATE TEXT INDEX nor300_literal FOR (n:D) ON (n.t)' AS text",
				value: "CREATE TEXT INDEX nor300_literal FOR (n:D) ON (n.t)",
			},
			{
				name:  "line comment",
				query: "// CREATE POINT INDEX nor300_line_comment FOR (n:D) ON (n.loc)\nRETURN 1 AS one",
				value: int64(1),
			},
			{
				name:  "block comment",
				query: "/* CREATE LOOKUP INDEX nor300_block_comment FOR (n) ON EACH labels(n) */ RETURN 'done' AS status",
				value: "done",
			},
		}
		for _, test := range reads {
			t.Run(test.name, func(t *testing.T) {
				exec, _ := newTestExecutor(t)
				indexes := readSchemaNames(t, t.Context(), exec, nor300ShowIndexNames)

				result, err := exec.Execute(readOnly, test.query, nil)
				require.NoError(t, err, test.query)
				require.Equal(t, [][]interface{}{{test.value}}, result.Rows, test.query)
				require.False(t, QueryPermissionRequirements(t.Context(), test.query, AcceptedSource).Schema, test.query)
				require.Equal(t, indexes, readSchemaNames(t, t.Context(), exec, nor300ShowIndexNames), test.query)
			})
		}

		writes := []struct {
			name  string
			query string
			value interface{}
		}{
			{
				name:  "property value literal",
				query: "CREATE (n:D {note: 'CREATE TEXT INDEX nor300_property FOR (n:D) ON (n.t)'}) RETURN n.note AS note",
				value: "CREATE TEXT INDEX nor300_property FOR (n:D) ON (n.t)",
			},
			{
				name:  "backtick quoted name",
				query: "CREATE (n:`CREATE TEXT INDEX`) RETURN labels(n) AS labels",
				value: []interface{}{"CREATE TEXT INDEX"},
			},
		}
		for _, test := range writes {
			t.Run(test.name, func(t *testing.T) {
				exec, _ := newTestExecutor(t)
				indexes := readSchemaNames(t, t.Context(), exec, nor300ShowIndexNames)

				result, err := exec.Execute(readWrite, test.query, nil)
				require.NoError(t, err, test.query)
				require.Equal(t, [][]interface{}{{test.value}}, result.Rows, test.query)
				require.False(t, QueryPermissionRequirements(t.Context(), test.query, AcceptedSource).Schema, test.query)
				require.Equal(t, indexes, readSchemaNames(t, t.Context(), exec, nor300ShowIndexNames), test.query)
			})
		}

		t.Run("property key", func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			_, err := exec.Execute(readWrite, "CREATE (n:D {t: 1})", nil)
			require.NoError(t, err)

			result, err := exec.Execute(readOnly, "MATCH (n:D) RETURN n.`CREATE TEXT INDEX` AS value", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{nil}}, result.Rows)
			require.False(t, QueryPermissionRequirements(t.Context(), "MATCH (n:D) RETURN n.`CREATE TEXT INDEX` AS value", AcceptedSource).Schema)
		})
	})
}

// TestExecuteDeniesSchemaDDLForShippedEditorRole resolves the entitlement checker
// from the built-in editor role through the production role mapping, the way HTTP
// and Bolt build a principal's checker: the editor role may read and write data,
// so it sat exactly on the boundary the missing index forms crossed, and every
// schema form now stops there while its data writes keep working.
func TestExecuteDeniesSchemaDDLForShippedEditorRole(t *testing.T) {
	entitlements := auth.PermissionsForRoles([]string{string(auth.RoleEditor)}, nil)
	editor := WithPermissionChecker(context.Background(), func(permission string) bool {
		return auth.EntitlementsAllow(entitlements, permission)
	})
	require.False(t, auth.EntitlementsAllow(entitlements, "schema"))
	require.True(t, auth.EntitlementsAllow(entitlements, "write"))

	exec, _ := newTestExecutor(t)
	indexes := readSchemaNames(t, t.Context(), exec, nor300ShowIndexNames)
	for _, query := range []string{
		"CREATE INDEX nor300_editor_index FOR (n:D) ON (n.p)",
		"CREATE TEXT INDEX nor300_editor_text FOR (n:D) ON (n.t)",
		"CREATE POINT INDEX nor300_editor_point FOR (n:D) ON (n.loc)",
		"CREATE LOOKUP INDEX nor300_editor_lookup FOR ()-[r]-() ON EACH type(r)",
	} {
		_, err := exec.Execute(editor, query, nil)
		var denied *PermissionDeniedError
		require.True(t, errors.As(err, &denied), "%s: %v", query, err)
		require.Equal(t, "schema", denied.Permission, query)
	}
	require.Equal(t, indexes, readSchemaNames(t, t.Context(), exec, nor300ShowIndexNames))

	// The editor role keeps its ordinary data write.
	_, err := exec.Execute(editor, "CREATE (n:D {note: 'editor'}) RETURN n.note AS note", nil)
	require.NoError(t, err)
}
