package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// NOR-300 bounded follow-up: CREATE INDEX ... OPTIONS and CREATE CONSTRAINT
// ... OPTIONS without a map are malformed. The ANTLR grammar requires OPTIONS
// to carry a map literal, but the native parser checked only that the tail
// started with the keyword, accepted the statement and applied the schema
// change — so the two parsers disagreed. Both parser configurations must
// reject it with the established SyntaxError classification and leave the
// schema untouched, while a valid map (including an empty one) and an omitted
// OPTIONS still create the schema object.
//
// The parser selection is process-wide, so each parser's run is its own
// process (see runCypherTestProcess).
func TestNOR300CreateIndexOptionsRequiresMap(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		// Every index DDL form that admits an OPTIONS tail: the unqualified name
		// and RANGE / VECTOR forms, the relationship form, the legacy
		// ON :Label(p) form and FULLTEXT.
		missingMap := []string{
			"CREATE INDEX nor300_generic FOR (n:D) ON (n.p) OPTIONS",
			"CREATE RANGE INDEX nor300_range FOR (n:D) ON (n.p) OPTIONS",
			"CREATE VECTOR INDEX nor300_vector FOR (n:D) ON (n.e) OPTIONS",
			"CREATE INDEX nor300_rel FOR ()-[r:D]-() ON (r.p) OPTIONS",
			"CREATE INDEX nor300_legacy ON :D(p) OPTIONS",
			"CREATE FULLTEXT INDEX nor300_full FOR (n:D) ON EACH [n.t] OPTIONS",
		}
		accepted := []struct{ query, index string }{
			{"CREATE INDEX nor300_map FOR (n:D) ON (n.p) OPTIONS { indexProvider: 'range-1.0' }", "nor300_map"},
			{"CREATE INDEX nor300_emptymap FOR (n:D) ON (n.q) OPTIONS {}", "nor300_emptymap"},
			{"CREATE INDEX nor300_tight FOR (n:D) ON (n.s) OPTIONS{indexProvider: 'range-1.0'}", "nor300_tight"},
			{"CREATE INDEX nor300_omitted FOR (n:D) ON (n.r)", "nor300_omitted"},
			{"CREATE VECTOR INDEX nor300_vector_map FOR (n:D) ON (n.e) OPTIONS { indexConfig: { `vector.dimensions`: 3, `vector.similarity_function`: 'cosine' } }", "nor300_vector_map"},
			{"CREATE FULLTEXT INDEX nor300_full_map FOR (n:D) ON EACH [n.t] OPTIONS { indexProvider: 'fulltext-1.0' }", "nor300_full_map"},
		}

		t.Run("missing map is a SyntaxError without schema mutation", func(t *testing.T) {
			for _, query := range missingMap {
				exec, _ := newTestExecutor(t)
				before := readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name")
				_, err := exec.Execute(t.Context(), query, nil)
				requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", query)
				require.Equal(t, before, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"), query)
			}
		})

		t.Run("valid map and omitted OPTIONS still create the index", func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			for _, control := range accepted {
				_, err := exec.Execute(t.Context(), control.query, nil)
				require.NoError(t, err, control.query)
				require.Contains(t, readSchemaNames(t, t.Context(), exec, "SHOW INDEXES YIELD name RETURN name ORDER BY name"), control.index, control.query)
			}
		})
	})
}

func TestNOR300CreateConstraintOptionsRequiresMap(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		// Every FOR ... REQUIRE shape whose OPTIONS tail the shared constraint
		// head admits: a constraint and its owned property index are schema
		// objects, so a bare OPTIONS created both.
		missingMap := []string{
			"CREATE CONSTRAINT nor300c_unique FOR (n:NOR300C) REQUIRE n.p IS UNIQUE OPTIONS",
			"CREATE CONSTRAINT nor300c_exists FOR (n:NOR300C) REQUIRE n.p IS NOT NULL OPTIONS",
			"CREATE CONSTRAINT nor300c_type FOR (n:NOR300C) REQUIRE n.p IS TYPED STRING OPTIONS",
			"CREATE CONSTRAINT nor300c_nodekey FOR (n:NOR300C) REQUIRE (n.a, n.b) IS NODE KEY OPTIONS",
		}
		accepted := []struct{ query, constraint string }{
			{"CREATE CONSTRAINT nor300c_map FOR (n:NOR300C) REQUIRE n.p IS UNIQUE OPTIONS { indexProvider: 'range-1.0' }", "nor300c_map"},
			{"CREATE CONSTRAINT nor300c_emptymap FOR (n:NOR300C) REQUIRE n.q IS UNIQUE OPTIONS {}", "nor300c_emptymap"},
			{"CREATE CONSTRAINT nor300c_tight FOR (n:NOR300C) REQUIRE n.r IS UNIQUE OPTIONS{indexProvider: 'range-1.0'}", "nor300c_tight"},
			{"CREATE CONSTRAINT nor300c_omitted FOR (n:NOR300C) REQUIRE n.s IS UNIQUE", "nor300c_omitted"},
			{"CREATE CONSTRAINT nor300c_nodekey_map FOR (n:NOR300C) REQUIRE (n.c, n.d) IS NODE KEY OPTIONS {}", "nor300c_nodekey_map"},
		}

		t.Run("missing map is a SyntaxError without schema mutation", func(t *testing.T) {
			for _, query := range missingMap {
				exec, _ := newTestExecutor(t)
				before := readSchemaNames(t, t.Context(), exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name")
				_, err := exec.Execute(t.Context(), query, nil)
				requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", query)
				require.Equal(t, before, readSchemaNames(t, t.Context(), exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name"), query)
			}
		})

		t.Run("valid map and omitted OPTIONS still create the constraint", func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			for _, control := range accepted {
				_, err := exec.Execute(t.Context(), control.query, nil)
				require.NoError(t, err, control.query)
				require.Contains(t, readSchemaNames(t, t.Context(), exec, "SHOW CONSTRAINTS YIELD name RETURN name ORDER BY name"), control.constraint, control.query)
			}
		})

		// The legacy ON ... ASSERT branches are rejected by
		// executeCreateConstraint before any parser runs (the entry refuses ON
		// before FOR), so their OPTIONS admission is only observable through the
		// parser itself. They read the OPTIONS tail through the same owner and
		// must admit exactly the same tails as the FOR ... REQUIRE head.
		exec, _ := newTestExecutor(t)
		legacy := []struct {
			owner string
			base  string
			parse func(string) (bool, error)
		}{
			{
				owner: "node key",
				base:  "CREATE CONSTRAINT ON (n:Legacy) ASSERT (n.tenant, n.id) IS NODE KEY",
				parse: func(query string) (bool, error) {
					parsed, err := exec.parseCreateConstraintNodeKeyDDL(t.Context(), query)
					return len(parsed.properties) == 2, err
				},
			},
			{
				owner: "property type",
				base:  "CREATE CONSTRAINT ON (n:Legacy) ASSERT n.active IS TYPED BOOLEAN",
				parse: func(query string) (bool, error) {
					parsed, err := exec.parseCreateConstraintTypeDDL(t.Context(), query)
					return parsed.property == "active", err
				},
			},
			{
				owner: "single property",
				base:  "CREATE CONSTRAINT ON (n:Legacy) ASSERT EXISTS(n.email)",
				parse: func(query string) (bool, error) {
					parsed, err := exec.parseCreateConstraintSimplePropertyDDL(t.Context(), query)
					return parsed.property == "email", err
				},
			},
		}
		for _, branch := range legacy {
			t.Run(branch.owner+" legacy ASSERT tail", func(t *testing.T) {
				_, err := branch.parse(branch.base + " OPTIONS")
				requireErrorStatus(t, err, "Neo.ClientError.Statement.SyntaxError", branch.base+" OPTIONS")
				for _, tail := range []string{"", " OPTIONS {}", " OPTIONS {indexProvider: 'range-1.0'}"} {
					parsed, err := branch.parse(branch.base + tail)
					require.NoError(t, err, branch.base+tail)
					require.True(t, parsed, branch.base+tail)
				}
			})
		}
	})
}
