package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDirectNameCommitRetryIdentity(t *testing.T) {
	for _, name := range []string{"k", "`k`", " ", "a`b"} {
		t.Run(name, func(t *testing.T) {
			violation := &storage.ConstraintViolationError{Type: storage.ConstraintUnique, Label: "U", Properties: []string{name}}
			key := cyphertext.QuoteName(name)
			for _, value := range []string{"1", "2"} {
				query := "MERGE (u:U {" + key + ":1}) SET u." + key + "=" + value
				require.Equal(t, value == "1", MergeUniqueConflictIsRetrySafe([]CommitStatement{{Query: query}}, violation), query)
			}
		})
	}
}

func TestDirectNameAggregateInvocationIdentity(t *testing.T) {
	exec := setupTestExecutor(t)
	for _, expression := range []string{"'rand()'", "'`rand`()'", "1 /* rand() */", "$value"} {
		t.Run(expression, func(t *testing.T) {
			result, err := exec.Execute(context.Background(), "RETURN collect("+expression+") AS values", map[string]interface{}{"value": "rand()"})
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.Len(t, result.Rows[0][0], 1)
		})
	}
	_, err := exec.Execute(context.Background(), "RETURN collect(rand()) AS values", nil)
	require.Error(t, err, "a real nondeterministic invocation must still be rejected")
}

func TestDirectNameOptimizedIdentity(t *testing.T) {
	exec := setupTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE ()-[:DirectIdentity {rating:2, RATING:9, ` `:5, `a``b`:7}]->({name:'lower', NAME:'upper'})", nil)
	require.NoError(t, err)
	for _, tc := range []struct{ label, query string }{
		{"ordinary", "MATCH (a)-[r:DirectIdentity]->(b) RETURN b.name AS name, avg(r.rating) AS value"},
		{"uppercase_property", "MATCH (a)-[r:DirectIdentity]->(b) RETURN b.name AS name, avg(r.RATING) AS value"},
		{"uppercase_endpoint_property", "MATCH (a)-[r:DirectIdentity]->(b) RETURN b.NAME AS name, count(a) AS value"},
		{"space_property", "MATCH (a)-[r:DirectIdentity]->(b) RETURN b.name AS name, avg(r.` `) AS value"},
		{"backtick_property", "MATCH (a)-[r:DirectIdentity]->(b) RETURN b.name AS name, avg(r.`a``b`) AS value"},
	} {
		t.Run(tc.label, func(t *testing.T) {
			want, err := exec.Execute(ctx, tc.query, nil)
			require.NoError(t, err)
			got, handled := exec.ExecuteOptimized(ctx, tc.query, DetectQueryPattern(ctx, tc.query))
			if handled {
				require.Equal(t, want.Columns, got.Columns)
				require.Equal(t, want.Rows, got.Rows)
			}
		})
	}
}

func TestDirectNameGeneratedSchemaIdentity(t *testing.T) {
	for _, tc := range []struct{ name, first, second string }{
		{"constraint_label_boundary", "CREATE CONSTRAINT FOR (n:Auto_A) REQUIRE (n.k,n.v) IS UNIQUE", "CREATE CONSTRAINT FOR (n:Auto) REQUIRE (n.A_k,n.v) IS UNIQUE"},
		{"constraint_property_boundary", "CREATE CONSTRAINT FOR (n:AutoProps) REQUIRE (n.a_b,n.c) IS UNIQUE", "CREATE CONSTRAINT FOR (n:AutoProps) REQUIRE (n.a,n.b_c) IS UNIQUE"},
		{"index_label_boundary", "CREATE INDEX FOR (n:Auto_A) ON (n.k,n.v)", "CREATE INDEX FOR (n:Auto) ON (n.A_k,n.v)"},
		{"index_property_boundary", "CREATE INDEX FOR (n:AutoProps) ON (n.a_b,n.c)", "CREATE INDEX FOR (n:AutoProps) ON (n.a,n.b_c)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := setupTestExecutor(t)
			_, err := exec.Execute(context.Background(), tc.first, nil)
			require.NoError(t, err)
			_, err = exec.Execute(context.Background(), tc.second, nil)
			require.NoError(t, err, "distinct unnamed schema definitions need distinct generated names")
			show := "SHOW CONSTRAINTS YIELD name RETURN name"
			if strings.HasPrefix(tc.name, "index_") {
				show = "SHOW INDEXES YIELD name, type WHERE type = 'RANGE' RETURN name"
			}
			result, err := exec.Execute(context.Background(), show, nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 2)
			require.NotEqual(t, result.Rows[0][0], result.Rows[1][0])
		})
	}
}
