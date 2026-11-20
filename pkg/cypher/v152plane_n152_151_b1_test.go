package cypher

// NOR-84: "ORDER BY and LIMIT keep their meaning when legal whitespace or
// comments separate the keywords."
//
// Contract. NORNICDB_CYPHER_QUERY_NORMALIZATION is enabled by default: comments
// and runs of whitespace outside string literals and quoted names become one
// space before a statement runs. With it off the statement runs as sent, and
// the same option's documented contract (pkg/config/feature_flags.go) allows a
// statement with comments or unusual spacing to fail — an explicitly
// constrained configuration, not an accidental parser route. The issue's
// approved outcome is the stronger statement above: the modifiers keep their
// meaning, so this test requires the modifier result, not merely a non-error.
//
// Owner trace. StorageExecutor.Execute (executor_entry.go) and
// executeInternal (executor_internal.go) read config.IsCypherQueryNormalizationEnabled
// and skip normalizeQueryText when it is off, so everything downstream reads
// the client's text verbatim. The composite (Fabric) planner receives that
// text as sent and decomposes it into per-constituent fragments; the
// constituent executors then apply RETURN … ORDER BY … [SKIP] [LIMIT] to the
// body's rows. Nothing may therefore depend on the two ORDER/BY tokens being
// separated by exactly one space, and a quoted string that spells a comment or
// a clause keyword must stay data.
//
// The fixture is the package's existing real composite fixture
// (newFabricCallRowsExecutor: a real multidb.DatabaseManager with local
// databases one/two, composite "comp", and a StorageExecutor bound to the
// default database plus the composite executor for explicit transactions), not
// a stub composite. Only the three :R3Mod {k:1, v:1..3} nodes are seeded, in
// "two"; the body returns rows through the public Execute path.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// nor84SeedR3Mod seeds exactly the three :R3Mod {k:1, v:1..3} nodes the issue's
// reproduction names, in constituent "two" of composite "comp".
func nor84SeedR3Mod(t *testing.T, exec *StorageExecutor) {
	t.Helper()
	mgr := exec.dbManager.(*testDatabaseManagerAdapter).manager
	store, err := mgr.GetStorage("two")
	require.NoError(t, err)
	seed := NewStorageExecutor(store)
	for _, statement := range []string{
		"CREATE (:R3Mod {k: 1, v: 1})",
		"CREATE (:R3Mod {k: 1, v: 2})",
		"CREATE (:R3Mod {k: 1, v: 3})",
	} {
		_, err := seed.Execute(t.Context(), statement, nil)
		require.NoError(t, err)
	}
}

// nor84Body is the issue's correlated CALL body with gap between ORDER and BY.
func nor84Body(gap string) string {
	return "RETURN n.v AS v ORDER" + gap + "BY v DESC LIMIT 1"
}

// nor84Query is the issue's exact composite query; gap replaces the two spaces
// between ORDER and BY in the second CALL body.
func nor84Query(gap string) string {
	return "USE comp CALL { USE comp.one RETURN 1 AS k } CALL { WITH k USE comp.two MATCH (n:R3Mod) WHERE n.k=k " +
		nor84Body(gap) + " } RETURN v"
}

func TestCompositeResultModifiersAcrossNormalizationAndClauseTrivia(t *testing.T) {
	restore := config.WithCypherQueryNormalizationDisabled()
	defer restore()

	exec := newFabricCallRowsExecutor(t, true) // Disable result caching: every parser/normalization cell must execute.
	nor84SeedR3Mod(t, exec)
	ctx := context.Background()

	// Single space is the normalized spelling the acceptance compares against;
	// the rest are the legal trivia spellings named by the issue. The block
	// comment is the original D17 replacement spelling, with no padding.
	trivia := []struct{ name, gap string }{
		{"single-space", " "},
		{"double-space", "  "},
		{"newline", "\n"},
		{"tab", "\t"},
		{"block-comment", "/* UNION CALL RETURN */"},
		{"line-comment", "// c\n"},
	}

	forEachParser(t, func(t *testing.T) {
		for _, normalization := range []bool{false, true} {
			for _, tc := range trivia {
				for _, explicit := range []bool{false, true} {
					config.SetCypherQueryNormalizationEnabled(normalization)
					query := nor84Query(tc.gap)
					name := tc.name + "/normalization="
					if normalization {
						name += "on"
					} else {
						name += "off"
					}
					if explicit {
						name += "/explicit-transaction"
					} else {
						name += "/autocommit"
					}
					t.Run(name, func(t *testing.T) {
						runExec := exec
						if explicit {
							// The composite executor owns the explicit transaction
							// (the same mode TestFabricCallRowStream uses), so the
							// query runs with autoCommit=false through Fabric.
							runExec = fabricCallRowsCompositeExecutor(t, exec)
							_, err := runExec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer func() {
								_, err := runExec.Execute(ctx, "ROLLBACK", nil)
								require.NoError(t, err)
							}()
						}
						result, err := runExec.Execute(ctx, query, nil)
						require.NoError(t, err, query)
						// ORDER BY v DESC plus LIMIT 1 keeps its meaning: the
						// largest seeded v, one row, under the body's alias.
						require.Equal(t, []string{"v"}, result.Columns, query)
						require.Len(t, result.Rows, 1, query)
						encoded, err := json.Marshal(result.Rows)
						require.NoError(t, err)
						require.JSONEq(t, "[[3]]", string(encoded), query)
					})
				}
			}
		}
	})
}

// TestCompositeResultModifiersControlsAndErrors pins what the matrix cells
// mean: the same query with only ORDER BY returns the whole ordered body, an
// ascending order returns the other end, LIMIT alone still limits, quoted
// comment/keyword text stays data, and genuine errors stay errors under both
// normalization settings.
func TestCompositeResultModifiersControlsAndErrors(t *testing.T) {
	restore := config.WithCypherQueryNormalizationDisabled()
	defer restore()

	exec := newFabricCallRowsExecutor(t, true) // Disable result caching: every parser/normalization cell must execute.
	nor84SeedR3Mod(t, exec)
	ctx := context.Background()
	prefix := "USE comp CALL { USE comp.one RETURN 1 AS k } CALL { WITH k USE comp.two MATCH (n:R3Mod) WHERE n.k=k "

	valid := []struct{ name, query, want string }{
		{
			name:  "order-by-only",
			query: prefix + "RETURN n.v AS v ORDER BY v DESC } RETURN v",
			want:  "[[3],[2],[1]]",
		},
		{
			name:  "ascending-order-and-limit",
			query: prefix + "RETURN n.v AS v ORDER BY v ASC LIMIT 1 } RETURN v",
			want:  "[[1]]",
		},
		{
			name:  "quoted-block-comment-in-where",
			query: prefix + "AND '/* ORDER BY */' <> '' RETURN n.v AS v ORDER BY v DESC LIMIT 1 } RETURN v",
			want:  "[[3]]",
		},
		{
			name:  "quoted-keywords-are-data",
			query: prefix + "RETURN '/* UNION CALL RETURN */' AS v ORDER BY v DESC LIMIT 1 } RETURN v",
			want:  "[[\"/* UNION CALL RETURN */\"]]",
		},
	}
	genuineErrors := []struct{ name, query, message string }{
		{
			name:    "missing-variable",
			query:   prefix + "RETURN missing AS v ORDER BY v DESC LIMIT 1 } RETURN v",
			message: "Variable `missing` not defined",
		},
		{
			name:    "missing-constituent",
			query:   "USE comp CALL { USE comp.nope RETURN 1 AS k } RETURN k",
			message: "invalid USE target 'comp.nope'",
		},
	}

	forEachParser(t, func(t *testing.T) {
		for _, normalization := range []bool{false, true} {
			config.SetCypherQueryNormalizationEnabled(normalization)
			label := "normalization=off"
			if normalization {
				label = "normalization=on"
			}
			for _, tc := range valid {
				t.Run(tc.name+"/"+label, func(t *testing.T) {
					result, err := exec.Execute(ctx, tc.query, nil)
					require.NoError(t, err, tc.query)
					require.Equal(t, []string{"v"}, result.Columns, tc.query)
					encoded, err := json.Marshal(result.Rows)
					require.NoError(t, err)
					require.JSONEq(t, tc.want, string(encoded), tc.query)
				})
			}
			t.Run("limit-only-keeps-one-row/"+label, func(t *testing.T) {
				// Row order without ORDER BY is unspecified, so only the limit
				// is pinned here: one seeded row, not the whole body.
				result, err := exec.Execute(ctx, prefix+"RETURN n.v AS v LIMIT 1 } RETURN v", nil)
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				require.Contains(t, []int64{1, 2, 3}, result.Rows[0][0])
			})
			for _, tc := range genuineErrors {
				t.Run(tc.name+"/"+label, func(t *testing.T) {
					_, err := exec.Execute(ctx, tc.query, nil)
					require.ErrorContains(t, err, tc.message, tc.query)
				})
			}
		}
	})
}
