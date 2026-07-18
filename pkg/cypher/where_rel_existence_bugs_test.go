package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ====================================================================================
// BUG: relationship-existence WHERE predicates evaluate incorrectly
// ====================================================================================
// Discovered: eshu #5147
// Impact: WHERE NOT (n)--() matches nothing (always false); WHERE (n)--() is
//         always true; COUNT { (n)--() } = 0 matches everything (subquery
//         always returns 0); EXISTS { (n)--() } is always false.
// Root Cause: the WHERE relationship-pattern gate in
//         executor_mutations_where_eval.go and traversal.go only recognized
//         bracketed patterns with an explicit arrow ("-[...]->" or
//         "<-[...]-"). Bracket-less patterns ((n)--(), (n)-->(), (n)<--())
//         and the bracketed *undirected* form ((n)-[r]-()) fell through to
//         evaluateWhereAsBoolean, whose default branch treats an
//         unrecognized expression as true. Separately, checkSubqueryMatch
//         and countSubqueryMatches required the subquery body to start with
//         "MATCH ", so bare "COUNT { (n)--() }" / "EXISTS { (n)--() }"
//         bodies (no MATCH keyword) always returned 0 / false.
// ====================================================================================

// setupRelExistenceFixture creates a 3-node fixture used across this file's
// tables: "orphan" has no relationships; "connected" has an outgoing
// :CONTAINS relationship to "peer".
func setupRelExistenceFixture(t *testing.T) (*StorageExecutor, *storage.MemoryEngine) {
	t.Helper()
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (n:Item {name: 'orphan'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
		CREATE (c:Item {name: 'connected'})
		CREATE (p:Item {name: 'peer'})
		CREATE (c)-[:CONTAINS]->(p)
	`, nil)
	require.NoError(t, err)

	return exec, base
}

func namesFromRows(rows [][]interface{}) []string {
	names := make([]string, 0, len(rows))
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		if s, ok := row[0].(string); ok {
			names = append(names, s)
		}
	}
	return names
}

func TestBug_WhereBareRelExistencePattern(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{
			name:  "NOT bare undirected matches only orphan",
			query: `MATCH (n:Item) WHERE NOT (n)--() RETURN n.name ORDER BY n.name`,
			want:  []string{"orphan"},
		},
		{
			name:  "positive bare undirected matches connected and peer",
			query: `MATCH (n:Item) WHERE (n)--() RETURN n.name ORDER BY n.name`,
			want:  []string{"connected", "peer"},
		},
		{
			name:  "bare outgoing arrow matches only connected",
			query: `MATCH (n:Item) WHERE (n)-->() RETURN n.name ORDER BY n.name`,
			want:  []string{"connected"},
		},
		{
			name:  "bare incoming arrow matches only peer",
			query: `MATCH (n:Item) WHERE (n)<--() RETURN n.name ORDER BY n.name`,
			want:  []string{"peer"},
		},
		{
			name:  "NOT bare outgoing arrow matches orphan and peer",
			query: `MATCH (n:Item) WHERE NOT (n)-->() RETURN n.name ORDER BY n.name`,
			want:  []string{"orphan", "peer"},
		},
		{
			name:  "bracketed undirected -[r]- matches connected and peer",
			query: `MATCH (n:Item) WHERE (n)-[:CONTAINS]-() RETURN n.name ORDER BY n.name`,
			want:  []string{"connected", "peer"},
		},
		{
			name:  "labeled variable bare undirected",
			query: `MATCH (n:Item) WHERE NOT (n:Item)--() RETURN n.name ORDER BY n.name`,
			want:  []string{"orphan"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec, _ := setupRelExistenceFixture(t)
			result, err := exec.Execute(context.Background(), tt.query, nil)
			require.NoError(t, err, "query should execute without error")
			got := namesFromRows(result.Rows)
			assert.ElementsMatch(t, tt.want, got, "query: %s", tt.query)
		})
	}
}

func TestBug_CountSubqueryBareBody(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{
			name:  "bare COUNT = 0 matches only orphan",
			query: `MATCH (n:Item) WHERE COUNT { (n)--() } = 0 RETURN n.name ORDER BY n.name`,
			want:  []string{"orphan"},
		},
		{
			name:  "bare COUNT >= 1 matches connected and peer",
			query: `MATCH (n:Item) WHERE COUNT { (n)--() } >= 1 RETURN n.name ORDER BY n.name`,
			want:  []string{"connected", "peer"},
		},
		{
			name:  "bare directed COUNT matches only connected",
			query: `MATCH (n:Item) WHERE COUNT { (n)-->() } = 1 RETURN n.name ORDER BY n.name`,
			want:  []string{"connected"},
		},
		{
			name:  "MATCH-prefixed COUNT agrees with bare form",
			query: `MATCH (n:Item) WHERE COUNT { MATCH (n)-->() } = 1 RETURN n.name ORDER BY n.name`,
			want:  []string{"connected"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec, _ := setupRelExistenceFixture(t)
			result, err := exec.Execute(context.Background(), tt.query, nil)
			require.NoError(t, err, "query should execute without error")
			got := namesFromRows(result.Rows)
			assert.ElementsMatch(t, tt.want, got, "query: %s", tt.query)
		})
	}
}

func TestBug_ExistsSubqueryBareBody(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{
			name:  "bare EXISTS matches connected and peer",
			query: `MATCH (n:Item) WHERE EXISTS { (n)--() } RETURN n.name ORDER BY n.name`,
			want:  []string{"connected", "peer"},
		},
		{
			name:  "bare NOT EXISTS matches only orphan",
			query: `MATCH (n:Item) WHERE NOT EXISTS { (n)--() } RETURN n.name ORDER BY n.name`,
			want:  []string{"orphan"},
		},
		{
			name:  "bare directed EXISTS matches only connected",
			query: `MATCH (n:Item) WHERE EXISTS { (n)-->() } RETURN n.name ORDER BY n.name`,
			want:  []string{"connected"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec, _ := setupRelExistenceFixture(t)
			result, err := exec.Execute(context.Background(), tt.query, nil)
			require.NoError(t, err, "query should execute without error")
			got := namesFromRows(result.Rows)
			assert.ElementsMatch(t, tt.want, got, "query: %s", tt.query)
		})
	}
}

// ------------------------------------------------------------------------
// Direct unit tests for the new helpers.
// ------------------------------------------------------------------------

func TestContainsRelExistencePattern(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		want    bool
	}{
		{"bare undirected", "(n)--()", true},
		{"bare outgoing", "(n)-->()", true},
		{"bare incoming", "(n)<--()", true},
		{"bracketed outgoing", "(n)-[:TYPE]->()", true},
		{"bracketed incoming", "(n)<-[:TYPE]-()", true},
		{"bracketed undirected", "(n)-[r]-()", true},
		{"bracketed undirected no type", "(n)-[]-()", true},
		{"negative: property arithmetic", "n.a - n.b", false},
		{"negative: array index arithmetic", "n.arr[0]-1", false},
		{"negative: plain comparison", "n.age > 10", false},
		{"negative: empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, containsRelExistencePattern(tt.pattern))
		})
	}
}

func TestBareRelDirection(t *testing.T) {
	tests := []struct {
		name         string
		pattern      string
		variable     string
		wantIncoming bool
		wantOutgoing bool
		wantOK       bool
	}{
		{"n outgoing prefix: (n)-->()", "(n)-->()", "n", false, true, true},
		{"n incoming prefix: (n)<--()", "(n)<--()", "n", true, false, true},
		{"n undirected prefix: (n)--()", "(n)--()", "n", true, true, true},
		{"n incoming suffix: ()-->(n)", "()-->(n)", "n", true, false, true},
		{"n outgoing suffix: ()<--(n)", "()<--(n)", "n", false, true, true},
		{"n undirected suffix: ()--(n)", "()--(n)", "n", true, true, true},
		{"labeled variable: (n:Item)-->()", "(n:Item)-->()", "n", false, true, true},
		{"labeled variable suffix: ()<--(n:Item)", "()<--(n:Item)", "n", false, true, true},
		{"bracketed pattern rejected", "(n)-[:TYPE]->()", "n", false, false, false},
		{"variable not present", "(m)-->()", "n", false, false, false},
		{"no arrow at all", "(n)", "n", false, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			incoming, outgoing, ok := bareRelDirection(tt.pattern, tt.variable)
			assert.Equal(t, tt.wantOK, ok, "ok mismatch")
			if tt.wantOK {
				assert.Equal(t, tt.wantIncoming, incoming, "incoming mismatch")
				assert.Equal(t, tt.wantOutgoing, outgoing, "outgoing mismatch")
			}
		})
	}
}

func TestEvaluateRelationshipPatternInWhere_BareDirected(t *testing.T) {
	exec, base := setupRelExistenceFixture(t)
	_ = base
	ctx := context.Background()

	getNodeByName := func(name string) *storage.Node {
		result, err := exec.Execute(ctx, `MATCH (n:Item {name: $name}) RETURN n`, map[string]interface{}{"name": name})
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		node, ok := result.Rows[0][0].(*storage.Node)
		require.True(t, ok, "expected *storage.Node, got %T", result.Rows[0][0])
		return node
	}

	connected := getNodeByName("connected")
	orphan := getNodeByName("orphan")
	peer := getNodeByName("peer")

	assert.True(t, exec.evaluateRelationshipPatternInWhere(connected, "n", "(n)-->()"))
	assert.False(t, exec.evaluateRelationshipPatternInWhere(orphan, "n", "(n)-->()"))
	assert.False(t, exec.evaluateRelationshipPatternInWhere(peer, "n", "(n)-->()"))

	assert.True(t, exec.evaluateRelationshipPatternInWhere(peer, "n", "(n)<--()"))
	assert.False(t, exec.evaluateRelationshipPatternInWhere(connected, "n", "(n)<--()"))

	assert.True(t, exec.evaluateRelationshipPatternInWhere(connected, "n", "(n)--()"))
	assert.True(t, exec.evaluateRelationshipPatternInWhere(peer, "n", "(n)--()"))
	assert.False(t, exec.evaluateRelationshipPatternInWhere(orphan, "n", "(n)--()"))
}

func TestCheckSubqueryMatch_BareBody(t *testing.T) {
	exec, _ := setupRelExistenceFixture(t)
	ctx := context.Background()

	getNodeByName := func(name string) *storage.Node {
		result, err := exec.Execute(ctx, `MATCH (n:Item {name: $name}) RETURN n`, map[string]interface{}{"name": name})
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		node, ok := result.Rows[0][0].(*storage.Node)
		require.True(t, ok)
		return node
	}

	connected := getNodeByName("connected")
	orphan := getNodeByName("orphan")

	assert.True(t, exec.checkSubqueryMatch(ctx, connected, "n", "(n)-->()"), "bare directed body should match connected")
	assert.False(t, exec.checkSubqueryMatch(ctx, orphan, "n", "(n)-->()"), "bare directed body should not match orphan")
	assert.True(t, exec.checkSubqueryMatch(ctx, connected, "n", "(n)--()"), "bare undirected body should match connected")
	assert.False(t, exec.checkSubqueryMatch(ctx, orphan, "n", "(n)--()"), "bare undirected body should not match orphan")
}

func TestCountSubqueryMatches_BareBody(t *testing.T) {
	exec, _ := setupRelExistenceFixture(t)
	ctx := context.Background()

	getNodeByName := func(name string) *storage.Node {
		result, err := exec.Execute(ctx, `MATCH (n:Item {name: $name}) RETURN n`, map[string]interface{}{"name": name})
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		node, ok := result.Rows[0][0].(*storage.Node)
		require.True(t, ok)
		return node
	}

	connected := getNodeByName("connected")
	orphan := getNodeByName("orphan")

	assert.Equal(t, int64(1), exec.countSubqueryMatches(connected, "n", "(n)-->()"))
	assert.Equal(t, int64(0), exec.countSubqueryMatches(orphan, "n", "(n)-->()"))
	assert.Equal(t, int64(1), exec.countSubqueryMatches(connected, "n", "(n)--()"))
	assert.Equal(t, int64(0), exec.countSubqueryMatches(orphan, "n", "(n)--()"))
	// MATCH-prefixed body should agree with the bare form.
	assert.Equal(t, int64(1), exec.countSubqueryMatches(connected, "n", "MATCH (n)-->()"))
}
