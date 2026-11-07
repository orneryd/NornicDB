package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A native schema DDL command (CREATE / DROP CONSTRAINT and INDEX) declares its
// empty result schema in its own execute owner, which returns no columns and no
// records. EXPLAIN of such a command therefore returns that same declared
// schema with no rows and leaves the schema catalog untouched, exactly as
// EXPLAIN already does for CREATE DATABASE (NOR-211).
//
// Before this correction the mode path compiled a graph projection program for
// the command, found no scope or projection to read, and failed with "Cannot
// establish the statement scope and result columns".
//
// The parser is process-wide, so each parser's run is its own process.
func TestExplainSchemaCommandDeclaresEmptyResultSchema(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		for _, command := range []string{
			"CREATE CONSTRAINT unique_property FOR (n:Example) REQUIRE n.p IS UNIQUE",
			"CREATE CONSTRAINT unique_composite FOR (n:Example) REQUIRE (n.a, n.b) IS UNIQUE",
			"CREATE CONSTRAINT node_key_composite FOR (n:Example) REQUIRE (n.a, n.b) IS NODE KEY",
			"CREATE CONSTRAINT node_key_single FOR (n:Example) REQUIRE n.p IS NODE KEY",
			"CREATE CONSTRAINT exists_property FOR (n:Example) REQUIRE n.p IS NOT NULL",
			"CREATE CONSTRAINT typed_property FOR (n:Example) REQUIRE n.p IS TYPED STRING",
			"CREATE CONSTRAINT typed_property_colon FOR (n:Example) REQUIRE n.p IS :: STRING",
			"CREATE INDEX index_for_property FOR (n:Example) ON (n.p)",
			"CREATE RANGE INDEX range_index_for_property FOR (n:Example) ON (n.p)",
			"DROP INDEX index_for_property IF EXISTS",
			"DROP CONSTRAINT unique_property IF EXISTS",
		} {
			t.Run(command, func(t *testing.T) {
				ctx := t.Context()
				exec, _ := newCypherTestExecutor(t, "schema_command_mode")
				before := schemaCatalogNames(t, ctx, exec)

				result, err := exec.Execute(ctx, "EXPLAIN "+command, nil)
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Equal(t, []string{}, result.Columns, "EXPLAIN declares the command's empty result schema")
				require.Empty(t, result.Rows, "EXPLAIN runs nothing")
				require.Equal(t, "EXPLAIN", result.Metadata["planType"], "EXPLAIN returns a plan")
				require.Equal(t, before, schemaCatalogNames(t, ctx, exec), "EXPLAIN must not create or drop a schema rule")
			})
		}

		// The catalog this test reads does change when a schema command runs, so
		// "unchanged" above is not vacuous.
		t.Run("control", func(t *testing.T) {
			ctx := t.Context()
			exec, _ := newCypherTestExecutor(t, "schema_command_mode")
			before := schemaCatalogNames(t, ctx, exec)
			result, err := exec.Execute(ctx, "CREATE CONSTRAINT control_unique FOR (n:Example) REQUIRE n.p IS UNIQUE", nil)
			require.NoError(t, err)
			require.Equal(t, []string{}, result.Columns, "the command declares no result columns")
			require.Empty(t, result.Rows)
			after := schemaCatalogNames(t, ctx, exec)
			require.Contains(t, after.names(), "control_unique", "the fixture catalog records a creation")
			require.NotEqual(t, before, after)
		})
	})
}

// PROFILE executes the statement it profiles, so a profiled schema command still
// declares the command's empty result schema and performs its schema change.
func TestProfileSchemaCommandExecutesAndDeclaresEmptyResultSchema(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		for _, step := range []struct{ command, name string }{
			{"CREATE CONSTRAINT profile_unique FOR (n:Example) REQUIRE n.p IS UNIQUE", "profile_unique"},
			{"CREATE CONSTRAINT profile_node_key FOR (n:Example) REQUIRE (n.a, n.b) IS NODE KEY", "profile_node_key"},
			{"CREATE INDEX profile_index FOR (n:Example) ON (n.p)", "profile_index"},
		} {
			t.Run(step.command, func(t *testing.T) {
				ctx := t.Context()
				exec, _ := newCypherTestExecutor(t, "schema_command_mode")

				result, err := exec.Execute(ctx, "PROFILE "+step.command, nil)
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Equal(t, []string{}, result.Columns, "PROFILE declares the command's empty result schema")
				require.Empty(t, result.Rows, "a schema command returns no records")
				require.Equal(t, "PROFILE", result.Metadata["planType"], "PROFILE returns a plan")
				require.Contains(t, schemaCatalogNames(t, ctx, exec).names(), step.name, "PROFILE executes the profiled schema command")
			})
		}
	})
}

// The grammar rejects requirement forms this implementation does not support.
// EXPLAIN and PROFILE keep rejecting them instead of reporting a plan for a
// command that could never run, and reject them before any schema change.
func TestSchemaCommandModesRejectUnsupportedSyntax(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		for _, command := range []string{
			"CREATE CONSTRAINT unsupported_exists FOR (n:Example) REQUIRE n.p EXISTS",
			"CREATE CONSTRAINT unsupported_not_unique FOR (n:Example) REQUIRE n.p IS NOT UNIQUE",
			"CREATE CONSTRAINT bad FOR (n:Example) REQUIRE () IS NODE KEY",
			"CREATE CONSTRAINT bad FOR (n:Example) REQUIRE (n.a) IS NODE KEY OPTIONS 7",
		} {
			for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
				t.Run(mode+command, func(t *testing.T) {
					ctx := t.Context()
					exec, _ := newCypherTestExecutor(t, "schema_command_mode")
					before := schemaCatalogNames(t, ctx, exec)

					_, err := exec.Execute(ctx, mode+command, nil)
					require.Error(t, err, "unsupported legacy requirement syntax is rejected")
					require.Equal(t, before, schemaCatalogNames(t, ctx, exec), "a rejected command changes no schema rule")
				})
			}
		}
	})
}

type schemaCatalog struct {
	constraints []string
	indexes     []string
}

// names lists every schema rule this executor owns, whatever its kind.
func (catalog schemaCatalog) names() []string {
	names := make([]string, 0, len(catalog.constraints)+len(catalog.indexes))
	names = append(names, catalog.constraints...)
	return append(names, catalog.indexes...)
}

// schemaCatalogNames reads the schema rules this executor owns. It is the same
// read the schema DDL owners publish, so a mutation, a no-op and a rejected
// command are distinguishable.
func schemaCatalogNames(t *testing.T, ctx context.Context, exec *StorageExecutor) schemaCatalog {
	t.Helper()
	return schemaCatalog{
		constraints: readSchemaNames(t, ctx, exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name"),
		indexes:     readSchemaNames(t, ctx, exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"),
	}
}

func TestSchemaMetadataValidatesTheParsedCommandUnderModePrefixes(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newCypherTestExecutor(t, "schema_command_mode")
		before := schemaCatalogNames(t, t.Context(), exec)
		for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
			valid := prefix + "CREATE CONSTRAINT valid_key FOR (n:Example) REQUIRE (n.a, n.b) IS NODE KEY"
			columns, err := exec.StatementResultColumnsContext(t.Context(), valid, RawSource)
			require.NoError(t, err, valid)
			require.Empty(t, columns, valid)
			invalid := prefix + "CREATE CONSTRAINT invalid_key FOR (n:Example) REQUIRE () IS NODE KEY"
			_, err = exec.StatementResultColumnsContext(t.Context(), invalid, RawSource)
			require.Error(t, err, invalid)
		}
		require.Equal(t, before, schemaCatalogNames(t, t.Context(), exec), "header discovery must not publish schema")
	})
}
