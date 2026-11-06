package cypher

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-46 acceptance: a backtick-quoted relationship type names one exact
// decoded type. Public Execute keeps that name through MATCH, variable-length
// path traversal, type(r), db.relationshipTypes and schema retrieval, so a
// type holding a space, a '|', a ':' or an escaped backtick is never read as
// alternatives, split on its punctuation, or trimmed into a different type.
// Both parser paths run the same cases; expectations are Neo4j 5.26's.
func TestQuotedRelationshipTypeNamesAreExactOnPublicExecute(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quoted_name_consumers"))
		ctx := context.Background()
		// Every competing type exists once, so a consumer that splits,
		// trims or unquotes a name selects a type it does not name.
		for _, statement := range []string{
			"CREATE (:QN {id: 1})-[:`A B`]->(:QN {id: 2})",
			"CREATE (:QN {id: 1})-[:`A  B`]->(:QN {id: 2})",
			"CREATE (:QN {id: 1})-[:`a|b`]->(:QN {id: 2})",
			"CREATE (:QN {id: 1})-[:a]->(:QN {id: 2})",
			"CREATE (:QN {id: 1})-[:b]->(:QN {id: 2})",
			"CREATE (:QN {id: 1})-[:`a:b`]->(:QN {id: 2})",
			"CREATE (:QN {id: 1})-[:`a``b`]->(:QN {id: 2})",
			"CREATE (:QN {id: 1})-[:ab]->(:QN {id: 2})",
		} {
			_, err := exec.Execute(ctx, statement, nil)
			require.NoError(t, err, statement)
		}

		for _, tc := range []struct {
			name  string
			query string
			want  [][]interface{}
		}{
			// A space is part of the name, and two spaces are a different name.
			{"space", "MATCH ()-[r:`A B`]->() RETURN type(r) AS t", [][]interface{}{{"A B"}}},
			{"two spaces", "MATCH ()-[r:`A  B`]->() RETURN type(r) AS t", [][]interface{}{{"A  B"}}},
			{"space is not trimmed away", "MATCH ()-[r:`A B `]->() RETURN type(r) AS t", nil},

			// '|' inside a quoted type is a character; outside it separates
			// alternatives, and each direction excludes the other reading.
			{"quoted pipe is one type", "MATCH ()-[r:`a|b`]->() RETURN type(r) AS t", [][]interface{}{{"a|b"}}},
			{"unquoted pipe is alternatives", "MATCH ()-[r:a|b]->() RETURN type(r) AS t", [][]interface{}{{"a"}, {"b"}}},

			// ':' inside a quoted type is a character.
			{"quoted colon is one type", "MATCH ()-[r:`a:b`]->() RETURN type(r) AS t", [][]interface{}{{"a:b"}}},
			{"single letter type is not a colon prefix", "MATCH ()-[r:a]->() RETURN type(r) AS t", [][]interface{}{{"a"}}},

			// A doubled backtick inside a quoted type is one backtick, not
			// the end of the name.
			{"escaped backtick", "MATCH ()-[r:`a``b`]->() RETURN type(r) AS t", [][]interface{}{{"a`b"}}},
			{"escaped backtick is not dropped", "MATCH ()-[r:ab]->() RETURN type(r) AS t", [][]interface{}{{"ab"}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				result, err := exec.Execute(ctx, tc.query, nil)
				require.NoError(t, err, tc.query)
				if tc.want == nil {
					require.Empty(t, result.Rows, tc.query)
					return
				}
				require.ElementsMatch(t, tc.want, result.Rows, tc.query)
			})
		}

		// Variable-length traversal steps over the exact type only: the
		// single-step P and Q edges around the chain are not steps of `P Q`.
		for _, statement := range []string{
			"CREATE (:QP {id: 1})-[:`P Q`]->(:QP {id: 2})-[:`P Q`]->(:QP {id: 3})",
			"MATCH (n:QP {id: 1}) CREATE (n)-[:P]->(:QP {id: 9})",
			"MATCH (n:QP {id: 2}) CREATE (n)-[:Q]->(:QP {id: 10})",
		} {
			_, err := exec.Execute(ctx, statement, nil)
			require.NoError(t, err, statement)
		}
		result, err := exec.Execute(ctx, "MATCH p = (s:QP {id: 1})-[:`P Q`*1..2]->(e:QP) RETURN length(p) AS l", nil)
		require.NoError(t, err)
		require.ElementsMatch(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows)

		result, err = exec.Execute(ctx, "MATCH p = (s:QP {id: 1})-[:`P  Q`*1..2]->(e:QP) RETURN count(p) AS c", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)

		// The type listing reports the decoded name as stored.
		result, err = exec.Execute(ctx, "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType AS t", nil)
		require.NoError(t, err)
		listed := make([]interface{}, len(result.Rows))
		for i, row := range result.Rows {
			listed[i] = row[0]
		}
		for _, name := range []string{"A B", "A  B", "a|b", "a:b", "a`b", "ab"} {
			require.Contains(t, listed, name)
		}
	})
}

// A relationship index names its quoted type exactly, and a read cached before
// an edit uses the current data and schema after it: no cached contract
// outlives the edit that invalidates it. Both parser paths run the same steps.
func TestQuotedRelationshipTypeCachedReadsFollowEdits(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quoted_name_cache"),
			QueryCachePolicy{MaxEntries: 50, TTL: time.Minute})
		ctx := context.Background()
		const read = "MATCH ()-[r:`C T` {tag: 'x'}]->() RETURN count(r) AS c"
		count := func() int64 {
			t.Helper()
			result, err := exec.Execute(ctx, read, nil)
			require.NoError(t, err, read)
			require.Len(t, result.Rows, 1)
			require.Len(t, result.Rows[0], 1)
			value, ok := result.Rows[0][0].(int64)
			require.True(t, ok, "count returns an integer: %#v", result.Rows[0][0])
			return value
		}

		_, err := exec.Execute(ctx, "CREATE (:QC)-[:`C T` {tag: 'x'}]->(:QC)", nil)
		require.NoError(t, err)
		require.Equal(t, int64(1), count())
		require.Equal(t, int64(1), count())
		hits, _, _, _, _ := exec.cache.Stats()
		require.GreaterOrEqual(t, hits, int64(1), "the repeated read is served from the result cache")

		// A data edit invalidates the cached read, and an edge of a competing
		// type is not part of what the quoted type selects.
		_, err = exec.Execute(ctx, "CREATE (:QC)-[:`C T` {tag: 'x'}]->(:QC)", nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, "CREATE (:QC)-[:C {tag: 'x'}]->(:QC)", nil)
		require.NoError(t, err)
		require.Equal(t, int64(2), count())

		// The schema reports the exact decoded type, and the cached read still
		// answers from the current data once the index exists.
		_, err = exec.Execute(ctx, "CREATE INDEX qc_rel FOR ()-[r:`C T`]-() ON (r.tag)", nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, "CREATE INDEX qc_rel_bt FOR ()-[r:`a``b`]-() ON (r.tag)", nil)
		require.NoError(t, err)
		schema, err := exec.Execute(ctx, "SHOW INDEXES YIELD name, entityType, labelsOrTypes, createStatement", nil)
		require.NoError(t, err)
		reported := map[string][]interface{}{}
		for _, row := range schema.Rows {
			reported[row[0].(string)] = row[1:]
		}
		require.Equal(t, []interface{}{"RELATIONSHIP", []string{"C T"}, "CREATE RANGE INDEX `qc_rel` FOR ()-[r:`C T`]-() ON (r.`tag`)"}, reported["qc_rel"])
		require.Equal(t, []interface{}{"RELATIONSHIP", []string{"a`b"}, "CREATE RANGE INDEX `qc_rel_bt` FOR ()-[r:`a``b`]-() ON (r.`tag`)"}, reported["qc_rel_bt"])
		require.Equal(t, int64(2), count())

		// Dropping the index leaves the read on the current data too.
		_, err = exec.Execute(ctx, "DROP INDEX qc_rel", nil)
		require.NoError(t, err)
		require.Equal(t, int64(2), count())
	})
}

// NOR-46 acceptance for the schema DDL readers: a quoted name written in a
// CREATE INDEX statement names the same exact name the data path reads.
// Leading, trailing and whitespace-only spaces are part of a quoted name, so
// the created index reports that name in labelsOrTypes and in a
// createStatement that recreates it, and two names that differ only by
// surrounding space stay distinct graph identities. Schema object names follow
// their creation-time normalization policy. Both parser paths run the same
// statements.
func TestQuotedIndexNamesKeepWhitespaceOnPublicDDL(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quoted_schema_index_names"))
		ctx := context.Background()

		// Every quoted type below is a name of its own: `a b `, `a b` and
		// ` a b` differ only by surrounding space, and ` ` and `  ` differ
		// only by how much space they hold.
		for _, statement := range []string{
			"CREATE (:QS {id: 1})-[:`a b `]->(:QS {id: 2})",
			"CREATE (:QS {id: 1})-[:`a b`]->(:QS {id: 2})",
			"CREATE (:QS {id: 1})-[:` a b`]->(:QS {id: 2})",
			"CREATE (:QS {id: 1})-[:` `]->(:QS {id: 2})",
			"CREATE (:QS {id: 1})-[:`  `]->(:QS {id: 2})",
		} {
			_, err := exec.Execute(ctx, statement, nil)
			require.NoError(t, err, statement)
		}

		// The data path and the DDL path agree on the name: each MATCH below
		// selects exactly the type the statement wrote.
		for _, tc := range []struct {
			name  string
			query string
			want  [][]interface{}
		}{
			{"trailing space", "MATCH ()-[r:`a b `]->() RETURN type(r) AS t", [][]interface{}{{"a b "}}},
			{"leading space", "MATCH ()-[r:` a b`]->() RETURN type(r) AS t", [][]interface{}{{" a b"}}},
			{"one space", "MATCH ()-[r:` `]->() RETURN type(r) AS t", [][]interface{}{{" "}}},
			{"two spaces", "MATCH ()-[r:`  `]->() RETURN type(r) AS t", [][]interface{}{{"  "}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				result, err := exec.Execute(ctx, tc.query, nil)
				require.NoError(t, err, tc.query)
				require.ElementsMatch(t, tc.want, result.Rows, tc.query)
			})
		}

		for _, statement := range []string{
			"CREATE INDEX qs_trailing FOR ()-[r:`a b `]-() ON (r.v)",
			"CREATE INDEX qs_plain FOR ()-[r:`a b`]-() ON (r.v)",
			"CREATE INDEX qs_leading FOR ()-[r:` a b`]-() ON (r.v)",
			"CREATE INDEX qs_one_space FOR ()-[r:` `]-() ON (r.v)",
			"CREATE INDEX qs_two_spaces FOR ()-[r:`  `]-() ON (r.v)",
			"CREATE INDEX qs_node_label FOR (n:`L `) ON (n.v)",
			"CREATE INDEX `qs ddl ` FOR ()-[r:`q`]-() ON (r.v)",
			// An unquoted name is still the token without surrounding space.
			"CREATE INDEX qs_unquoted FOR ()-[r: qs_unquoted_type ]-() ON (r.v)",
		} {
			_, err := exec.Execute(ctx, statement, nil)
			require.NoError(t, err, statement)
		}

		schema, err := exec.Execute(ctx, "SHOW INDEXES YIELD name, entityType, labelsOrTypes, createStatement", nil)
		require.NoError(t, err)
		reported := map[string][]interface{}{}
		for _, row := range schema.Rows {
			require.Len(t, row, 4, "SHOW INDEXES row: %#v", row)
			reported[row[0].(string)] = row[1:]
		}
		for _, want := range []struct {
			name            string
			entityType      string
			labelsOrTypes   []string
			createStatement string
		}{
			{"qs_trailing", "RELATIONSHIP", []string{"a b "}, "CREATE RANGE INDEX `qs_trailing` FOR ()-[r:`a b `]-() ON (r.`v`)"},
			{"qs_plain", "RELATIONSHIP", []string{"a b"}, "CREATE RANGE INDEX `qs_plain` FOR ()-[r:`a b`]-() ON (r.`v`)"},
			{"qs_leading", "RELATIONSHIP", []string{" a b"}, "CREATE RANGE INDEX `qs_leading` FOR ()-[r:` a b`]-() ON (r.`v`)"},
			{"qs_one_space", "RELATIONSHIP", []string{" "}, "CREATE RANGE INDEX `qs_one_space` FOR ()-[r:` `]-() ON (r.`v`)"},
			{"qs_two_spaces", "RELATIONSHIP", []string{"  "}, "CREATE RANGE INDEX `qs_two_spaces` FOR ()-[r:`  `]-() ON (r.`v`)"},
			{"qs_node_label", "NODE", []string{"L "}, "CREATE RANGE INDEX `qs_node_label` FOR (n:`L `) ON (n.`v`)"},
			{"qs ddl", "RELATIONSHIP", []string{"q"}, "CREATE RANGE INDEX `qs ddl` FOR ()-[r:`q`]-() ON (r.`v`)"},
			{"qs_unquoted", "RELATIONSHIP", []string{"qs_unquoted_type"}, "CREATE RANGE INDEX `qs_unquoted` FOR ()-[r:`qs_unquoted_type`]-() ON (r.`v`)"},
		} {
			require.Equal(t, []interface{}{want.entityType, want.labelsOrTypes, want.createStatement}, reported[want.name], want.name)
		}

		// The supported empty-name rejection is unchanged: a quoted name that
		// decodes to nothing is not a name.
		for _, statement := range []string{
			"CREATE INDEX qs_empty_type FOR ()-[r:``]-() ON (r.v)",
			"CREATE INDEX `` FOR ()-[r:qs_empty_name]-() ON (r.v)",
		} {
			_, err := exec.Execute(ctx, statement, nil)
			require.Error(t, err, statement)
		}
	})
}

// A constraint written with quoted names keeps them exactly too: the label,
// relationship type and property it names are the decoded ones, and SHOW
// CONSTRAINTS reports and recreates that same statement. Both parser paths run
// the same cases.
func TestQuotedConstraintNamesKeepWhitespaceOnPublicDDL(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quoted_schema_constraint_names"))
		ctx := context.Background()

		// `L ` and `L` are different labels, and `v ` is not `v`.
		for _, statement := range []string{
			"CREATE CONSTRAINT qs_label FOR (n:`L `) REQUIRE n.`v ` IS UNIQUE",
			"CREATE CONSTRAINT qs_prop FOR (n:`L`) REQUIRE n.`v ` IS UNIQUE",
			"CREATE CONSTRAINT qs_rel FOR ()-[r:`a b `]-() REQUIRE (r.`v `) IS RELATIONSHIP KEY",
			"CREATE CONSTRAINT qs_exists FOR (n:`L `) REQUIRE n.`w ` IS NOT NULL",
			// The cardinality and policy forms read their type and labels
			// through their own readers; both keep the quoted names exactly.
			"CREATE CONSTRAINT qs_card FOR ()-[r:`a b `]->() REQUIRE MAX COUNT 2",
			"CREATE CONSTRAINT qs_policy FOR (:`L `)-[r:`a b `]->(:`T `) REQUIRE ALLOWED",
		} {
			_, err := exec.Execute(ctx, statement, nil)
			require.NoError(t, err, statement)
		}

		schema, err := exec.Execute(ctx, "SHOW CONSTRAINTS YIELD name, entityType, labelsOrTypes, properties, createStatement", nil)
		require.NoError(t, err)
		reported := map[string][]interface{}{}
		for _, row := range schema.Rows {
			require.Len(t, row, 5, "SHOW CONSTRAINTS row: %#v", row)
			reported[row[0].(string)] = row[1:]
		}
		for _, want := range []struct {
			name            string
			entityType      string
			labelsOrTypes   []string
			properties      interface{}
			createStatement string
		}{
			{"qs_label", "NODE", []string{"L "}, []string{"v "}, "CREATE CONSTRAINT `qs_label` FOR (n:`L `) REQUIRE (n.`v `) IS UNIQUE"},
			{"qs_prop", "NODE", []string{"L"}, []string{"v "}, "CREATE CONSTRAINT `qs_prop` FOR (n:`L`) REQUIRE (n.`v `) IS UNIQUE"},
			{"qs_rel", "RELATIONSHIP", []string{"a b "}, []string{"v "}, "CREATE CONSTRAINT `qs_rel` FOR ()-[r:`a b `]-() REQUIRE (r.`v `) IS RELATIONSHIP KEY"},
			{"qs_exists", "NODE", []string{"L "}, []string{"w "}, "CREATE CONSTRAINT `qs_exists` FOR (n:`L `) REQUIRE (n.`w `) IS NOT NULL"},
			{"qs_card", "RELATIONSHIP", []string{"a b "}, []string(nil), "CREATE CONSTRAINT `qs_card` FOR ()-[r:`a b `]->() REQUIRE MAX COUNT 2"},
			{"qs_policy", "RELATIONSHIP", []string{"a b "}, []string(nil), "CREATE CONSTRAINT `qs_policy` FOR (:`L `)-[r:`a b `]->(:`T `) REQUIRE ALLOWED"},
		} {
			require.Equal(t, []interface{}{want.entityType, want.labelsOrTypes, want.properties, want.createStatement}, reported[want.name], want.name)
		}

		// The supported empty-name rejection is unchanged here as well.
		_, err = exec.Execute(ctx, "CREATE CONSTRAINT qs_empty FOR (n:`L `) REQUIRE n.`` IS NOT NULL", nil)
		require.Error(t, err, "a quoted property name that decodes to nothing is rejected")
	})
}

// Schema rule names are not graph names: Neo4j trims them on creation and
// rejects whitespace-only rule names, while DROP addresses the stored name.
func TestQuotedSchemaObjectNamesFollowReferenceCreationPolicy(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "schema_object_names"))
		for _, kind := range []string{"INDEX", "CONSTRAINT"} {
			for _, name := range []string{"ordinary_rule", " padded_rule ", "tick`rule", "café_规则", "odd-name.with$punctuation"} {
				source := cyphertext.PrintIdentifier(name, nil, cyphertext.RawSource)
				canonical := strings.TrimSpace(name)
				command := "CREATE INDEX " + source + " FOR (n:DropRule) ON (n.v)"
				show := "SHOW INDEXES YIELD name RETURN name"
				if kind == "CONSTRAINT" {
					command = "CREATE CONSTRAINT " + source + " FOR (n:DropRule) REQUIRE n.v IS UNIQUE"
					show = "SHOW CONSTRAINTS YIELD name RETURN name"
				}
				_, err := exec.Execute(t.Context(), command, nil)
				require.NoError(t, err, command)
				result, err := exec.Execute(t.Context(), show, nil)
				require.NoError(t, err)
				require.Contains(t, result.Rows, []any{canonical})
				if canonical != name {
					_, err = exec.Execute(t.Context(), "DROP "+kind+" "+source, nil)
					require.Error(t, err, "DROP addresses the normalized stored name, as the reference does")
					code := "Neo.DatabaseError.Schema.IndexDropFailed"
					if kind == "CONSTRAINT" {
						code = "Neo.DatabaseError.Schema.ConstraintDropFailed"
					}
					requireStatusCode(t, err, code)
				}
				_, err = exec.Execute(t.Context(), "DROP "+kind+" "+cyphertext.PrintIdentifier(canonical, nil, cyphertext.RawSource), nil)
				require.NoError(t, err)
				result, err = exec.Execute(t.Context(), show, nil)
				require.NoError(t, err)
				require.NotContains(t, result.Rows, []any{canonical})
			}
		}
		for _, q := range []string{
			"CREATE INDEX ` ` FOR (n:BadRule) ON (n.v)",
			"CREATE CONSTRAINT ` ` FOR (n:BadRule) REQUIRE n.v IS UNIQUE",
			"CREATE LOOKUP INDEX ` ` FOR (n) ON EACH labels(n)",
		} {
			_, err := exec.Execute(t.Context(), q, nil)
			require.Error(t, err, q)
		}
	})
}
