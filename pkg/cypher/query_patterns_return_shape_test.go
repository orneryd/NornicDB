package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReturnShapeItems pins the single shared owner of RETURN-clause location,
// trailing ORDER BY / SKIP / LIMIT trimming and comma splitting reused by
// isReturnNameCountShape and isReturnEdgePropertyAggNameShape.
func TestReturnShapeItems(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{
			name:  "plain two items",
			query: "MATCH (p)<-[:FOLLOWS]-(f) RETURN p.name, count(f)",
			want:  []string{"p.name", "count(f)"},
		},
		{
			name:  "trailing order by / limit trimmed",
			query: "MATCH (p)<-[:FOLLOWS]-(f) RETURN p.name, count(f) ORDER BY count(f) DESC LIMIT 5",
			want:  []string{"p.name", "count(f)"},
		},
		{
			name:  "trailing skip trimmed",
			query: "MATCH (p)<-[:FOLLOWS]-(f) RETURN p.name, count(f) SKIP 2",
			want:  []string{"p.name", "count(f)"},
		},
		{
			name:  "aliases kept here, stripped by trimReturnItemAlias",
			query: "MATCH (p)<-[:FOLLOWS]-(f) RETURN p.name AS name, count(f) AS followers",
			want:  []string{"p.name AS name", "count(f) AS followers"},
		},
		{
			name:  "comma inside call stays in one item",
			query: "MATCH (p) RETURN p.name, coalesce(p.a, p.b)",
			want:  []string{"p.name", "coalesce(p.a, p.b)"},
		},
		{
			name:  "no return clause",
			query: "MATCH (p) WHERE p.name IS NOT NULL",
			want:  nil,
		},
		{
			name:  "empty return body",
			query: "MATCH (p) RETURN",
			want:  nil,
		},
		{
			name:  "order by immediately after return",
			query: "MATCH (p) RETURN ORDER BY p.name",
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, returnShapeItems(tt.query))
		})
	}
}

func TestTrimReturnItemAlias(t *testing.T) {
	assert.Equal(t, "p.name", trimReturnItemAlias("  p.name  "))
	assert.Equal(t, "p.name", trimReturnItemAlias("p.name AS name"))
	assert.Equal(t, "p.name", trimReturnItemAlias("p.name as displayName"))
	assert.Equal(t, "count(f)", trimReturnItemAlias("count(f) AS   followers"))
	// Alias detection is case-insensitive but requires literal space separators,
	// exactly as the original per-owner inline code behaved.
	assert.Equal(t, "p.name\tAs\tn", trimReturnItemAlias("p.name\tAs\tn"))
	// No alias marker: unchanged apart from surrounding trim.
	assert.Equal(t, "p.name", trimReturnItemAlias("p.name"))
	assert.Equal(t, "", trimReturnItemAlias("   "))
}

// TestIsReturnNameCountShape covers the accepted grammar of the incoming /
// outgoing count fast path after the canonicalization.
func TestIsReturnNameCountShape(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "plain",
			query: "MATCH (p:Person)<-[:FOLLOWS]-(f:Person) RETURN p.name, count(f)",
			want:  true,
		},
		{
			name:  "alias and trailing clauses",
			query: "MATCH (p:Person)<-[:FOLLOWS]-(f:Person) RETURN p.name, count(f) as followers ORDER BY followers DESC LIMIT 10",
			want:  true,
		},
		{
			name:  "uppercase AS aliases still accepted",
			query: "MATCH (p:Person)<-[:FOLLOWS]-(f:Person) RETURN p.name AS name, count(f) AS cnt",
			want:  true,
		},
		{
			name:  "order by aggregate expression then limit",
			query: "MATCH (p:Person)<-[:FOLLOWS]-(f:Person) RETURN p.name, count(f) ORDER BY count(f) DESC LIMIT 5",
			want:  true,
		},
		{
			name:  "skip after order by",
			query: "MATCH (p:Person)<-[:FOLLOWS]-(f:Person) RETURN p.name, count(f) ORDER BY count(f) SKIP 2 LIMIT 5",
			want:  true,
		},
		{
			name:  "count star",
			query: "MATCH (p:Person)<-[:FOLLOWS]-(f) RETURN p.name, count(*) as cnt",
			want:  true,
		},
		{
			name:  "spaces inside aggregate call",
			query: "MATCH (p:Person)<-[:FOLLOWS]-(f) RETURN p.name, count( f )",
			want:  true,
		},
		{
			name:  "case insensitive",
			query: "match (p:person)<-[:follows]-(f:person) return P.NAME, COUNT(F)",
			want:  true,
		},
		{
			name:  "wrong aggregate target",
			query: "MATCH (p:Person)<-[:FOLLOWS]-(f) RETURN p.name, count(p)",
			want:  false,
		},
		{
			name:  "wrong projected property",
			query: "MATCH (p:Person)<-[:FOLLOWS]-(f) RETURN p.id, count(f)",
			want:  false,
		},
		{
			name:  "wrong start variable",
			query: "MATCH (p:Person)<-[:FOLLOWS]-(f) RETURN f.name, count(f)",
			want:  false,
		},
		{
			name:  "three items",
			query: "MATCH (p:Person)<-[:FOLLOWS]-(f) RETURN p.name, count(f), p.age",
			want:  false,
		},
		{
			name:  "single item",
			query: "MATCH (p:Person)<-[:FOLLOWS]-(f) RETURN p.name",
			want:  false,
		},
		{
			name:  "no return clause",
			query: "MATCH (p:Person)<-[:FOLLOWS]-(f)",
			want:  false,
		},
		{
			name:  "other aggregate",
			query: "MATCH (p:Person)<-[:FOLLOWS]-(f) RETURN p.name, sum(f.x)",
			want:  false,
		},
		{
			name:  "empty return body",
			query: "MATCH (p:Person)<-[:FOLLOWS]-(f) RETURN",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isReturnNameCountShape(tt.query, "p", "f"))
		})
	}
}

// TestIsReturnEdgePropertyAggNameShape_ClearCases covers accepted and rejected
// inputs of the edge property aggregation fast path after the canonicalization,
// in addition to the pre-existing assertions in cypher_helpers_extra_test.go.
func TestIsReturnEdgePropertyAggNameShape_ClearCases(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "alias and trailing clauses",
			query: "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN p.name, avg(r.rating) as avgRating, count(r) as reviewCount ORDER BY avgRating DESC LIMIT 10",
			want:  true,
		},
		{
			name:  "order by aggregate call then limit",
			query: "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN p.name, avg(r.rating), count(r) ORDER BY avg(r.rating) DESC LIMIT 3",
			want:  true,
		},
		{
			name:  "all supported aggregates",
			query: "MATCH (c)-[r:R]->(p) RETURN p.name, sum(r.rating), min(r.rating), max(r.rating), count(*)",
			want:  true,
		},
		{
			name:  "case insensitive",
			query: "match (c)-[r:R]->(p) return P.NAME, AVG(R.RATING)",
			want:  true,
		},
		{
			name:  "non aggregation trailing item",
			query: "MATCH (c)-[r:R]->(p) RETURN p.name, avg(r.rating), p.id",
			want:  false,
		},
		{
			name:  "count over other variable",
			query: "MATCH (c)-[r:R]->(p) RETURN p.name, avg(r.rating), count(p)",
			want:  false,
		},
		{
			name:  "aggregate over other property",
			query: "MATCH (c)-[r:R]->(p) RETURN p.name, avg(r.other)",
			want:  false,
		},
		{
			name:  "first item not a name property",
			query: "MATCH (c)-[r:R]->(p) RETURN p.id, avg(r.rating)",
			want:  false,
		},
		{
			name:  "single item",
			query: "MATCH (c)-[r:R]->(p) RETURN p.name",
			want:  false,
		},
		{
			name:  "no return clause",
			query: "MATCH (c)-[r:R]->(p)",
			want:  false,
		},
		{
			name:  "empty return body",
			query: "MATCH (c)-[r:R]->(p) RETURN",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isReturnEdgePropertyAggNameShape(tt.query, "r", "rating"))
		})
	}
}

// TestDetectQueryPattern_ReturnShapeCanonicalized keeps the public routing
// behavior visible end-to-end through DetectQueryPattern for the inputs used by
// the shared helpers.
func TestDetectQueryPattern_ReturnShapeCanonicalized(t *testing.T) {
	ctx := context.Background()

	incoming := DetectQueryPattern(ctx, "MATCH (p:Person)<-[:FOLLOWS]-(f:Person) RETURN p.name, count(f) ORDER BY count(f) DESC SKIP 1 LIMIT 5")
	require.Equal(t, PatternIncomingCountAgg, incoming.Pattern)
	assert.Equal(t, "p", incoming.StartVar)
	assert.Equal(t, "f", incoming.EndVar)

	outgoing := DetectQueryPattern(ctx, "MATCH (p:Person)-[:FOLLOWS]->(f:Person) RETURN p.name, count(f) ORDER BY count(f) DESC LIMIT 5")
	require.Equal(t, PatternOutgoingCountAgg, outgoing.Pattern)

	edgeAgg := DetectQueryPattern(ctx, "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN p.name, avg(r.rating), count(r) ORDER BY avg(r.rating) DESC")
	require.Equal(t, PatternEdgePropertyAgg, edgeAgg.Pattern)
	assert.Equal(t, "rating", edgeAgg.AggProperty)

	// Non-matching shapes keep falling through to generic execution.
	generic := DetectQueryPattern(ctx, "MATCH (p:Person)<-[:FOLLOWS]-(f:Person) RETURN p.name, count(p)")
	assert.Equal(t, PatternGeneric, generic.Pattern)
}
