package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/internal/contextslot"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The SET statements here run after the same statement's DELETE, which is
// what makes the deleted entity's read/write controls apply (#907). Every
// expectation comes from Neo4j 5.26.30 reading the same statement shape.

// deletedSetFixture builds an executor over its own empty graph, so a case's
// store assertions see only the entities it seeded.
func deletedSetFixture(t *testing.T) (*StorageExecutor, context.Context) {
	t.Helper()
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "deleted_set")), context.Background()
}

// requireCount runs a single-count query and requires the count, which pins
// that SET stored nothing after the statement deleted the entity.
func requireCount(t *testing.T, exec *StorageExecutor, ctx context.Context, query string, want int64) {
	t.Helper()
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err, query)
	require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
}

// A SET that runs after the statement's own DELETE assigns properties Neo4j
// counts and stores nothing (#907): the deleted entity keeps the empty view
// the DELETE left in the row, so n {.*} is {} and no phantom property is
// visible or published to storage. It holds for the node and relationship
// forms, for every SET form, and through an alias.
func TestDeletedEntitySetWritesLeaveTheDeletedEntityEmpty(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		for _, testCase := range []struct {
			name       string
			setup      string
			query      string
			storeQuery string
			want       map[string]interface{}
			properties int
		}{
			// Node, every SET form.
			{
				name:       "node property",
				setup:      "CREATE (:X {p: 1})",
				query:      "MATCH (n:X {p: 1}) DELETE n SET n.q = 1 RETURN n {.*} AS v",
				storeQuery: "MATCH (n:X) RETURN count(n) AS c",
				want:       map[string]interface{}{},
				properties: 1,
			},
			{
				name:       "node dynamic key",
				setup:      "CREATE (:X {p: 1})",
				query:      "MATCH (n:X {p: 1}) WITH n, {k: 'q'} AS m DELETE n SET n[m.k] = 1 RETURN n {.*} AS v",
				storeQuery: "MATCH (n:X) RETURN count(n) AS c",
				want:       map[string]interface{}{},
				properties: 1,
			},
			{
				name:       "node map merge",
				setup:      "CREATE (:X {p: 1})",
				query:      "MATCH (n:X {p: 1}) DELETE n SET n += {q: 1} RETURN n {.*} AS v",
				storeQuery: "MATCH (n:X) RETURN count(n) AS c",
				want:       map[string]interface{}{},
				properties: 1,
			},
			{
				name:       "node map replacement",
				setup:      "CREATE (:X {p: 1})",
				query:      "MATCH (n:X {p: 1}) DELETE n SET n = {q: 1} RETURN n {.*} AS v",
				storeQuery: "MATCH (n:X) RETURN count(n) AS c",
				want:       map[string]interface{}{},
				properties: 1,
			},
			{
				name:       "node alias",
				setup:      "CREATE (:X {p: 1})",
				query:      "MATCH (n:X {p: 1}) WITH n AS m DELETE m SET m.q = 1 RETURN m {.*} AS v",
				storeQuery: "MATCH (n:X) RETURN count(n) AS c",
				want:       map[string]interface{}{},
				properties: 1,
			},
			// Relationship, every SET form.
			{
				name:       "relationship property",
				setup:      "CREATE (:A)-[:R {w: 1}]->(:B)",
				query:      "MATCH ()-[r:R]->() DELETE r SET r.q = 1 RETURN r {.*} AS v",
				storeQuery: "MATCH ()-[r:R]->() RETURN count(r) AS c",
				want:       map[string]interface{}{},
				properties: 1,
			},
			{
				name:       "relationship dynamic key",
				setup:      "CREATE (:A)-[:R {w: 1}]->(:B)",
				query:      "MATCH ()-[r:R]->() WITH r, {k: 'q'} AS m DELETE r SET r[m.k] = 1 RETURN r {.*} AS v",
				storeQuery: "MATCH ()-[r:R]->() RETURN count(r) AS c",
				want:       map[string]interface{}{},
				properties: 1,
			},
			{
				name:       "relationship map merge",
				setup:      "CREATE (:A)-[:R {w: 1}]->(:B)",
				query:      "MATCH ()-[r:R]->() DELETE r SET r += {q: 1} RETURN r {.*} AS v",
				storeQuery: "MATCH ()-[r:R]->() RETURN count(r) AS c",
				want:       map[string]interface{}{},
				properties: 1,
			},
			{
				name:       "relationship map replacement",
				setup:      "CREATE (:A)-[:R {w: 1}]->(:B)",
				query:      "MATCH ()-[r:R]->() DELETE r SET r = {q: 1} RETURN r {.*} AS v",
				storeQuery: "MATCH ()-[r:R]->() RETURN count(r) AS c",
				want:       map[string]interface{}{},
				properties: 1,
			},
			{
				name:       "relationship alias",
				setup:      "CREATE (:A)-[:R {w: 1}]->(:B)",
				query:      "MATCH ()-[r:R]->() WITH r AS e DELETE e SET e.q = 1 RETURN e {.*} AS v",
				storeQuery: "MATCH ()-[r:R]->() RETURN count(r) AS c",
				want:       map[string]interface{}{},
				properties: 1,
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				exec, ctx := deletedSetFixture(t)
				_, err := exec.Execute(ctx, testCase.setup, nil)
				require.NoError(t, err)
				result, err := exec.Execute(ctx, testCase.query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{testCase.want}}, result.Rows, testCase.query)
				require.Equal(t, testCase.properties, result.Stats.PropertiesSet, testCase.query)
				requireCount(t, exec, ctx, testCase.storeQuery, 0)
			})
		}
	})
}

// properties_set counts a deleted target's assignments the way it counts a
// live entity's, though nothing is stored (#907): the reported candidate
// CREATE (n:X {p: 1}) DELETE n SET n.q = 1 counts 2 (the CREATE's p and the
// SET's q), a seeded graph counts the SET's 1, and a null right-hand side adds
// no property count. The counters are the applicator's write count, so they
// are never skipped or zeroed for a deleted target.
func TestDeletedEntitySetWriteCountersCountWrites(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		t.Run("reported candidate", func(t *testing.T) {
			exec, ctx := deletedSetFixture(t)
			result, err := exec.Execute(ctx, "CREATE (n:X {p: 1}) DELETE n SET n.q = 1 RETURN n {.*} AS v", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{map[string]interface{}{}}}, result.Rows)
			require.Equal(t, 2, result.Stats.PropertiesSet)
			requireCount(t, exec, ctx, "MATCH (n:X) RETURN count(n) AS c", 0)
		})
		for _, testCase := range []struct {
			name  string
			setup string
			query string
			want  int
		}{
			{
				name:  "node property",
				setup: "CREATE (:X {p: 1})",
				query: "MATCH (n:X {p: 1}) DELETE n SET n.q = 1",
				want:  1,
			},
			{
				name:  "node property null",
				setup: "CREATE (:X {p: 1})",
				query: "MATCH (n:X {p: 1}) DELETE n SET n.q = null",
				want:  0,
			},
			{
				name:  "node dynamic key",
				setup: "CREATE (:X {p: 1})",
				query: "MATCH (n:X {p: 1}) WITH n, {k: 'q'} AS m DELETE n SET n[m.k] = 1",
				want:  1,
			},
			{
				name:  "node map merge",
				setup: "CREATE (:X {p: 1})",
				query: "MATCH (n:X {p: 1}) DELETE n SET n += {q: 1}",
				want:  1,
			},
			{
				name:  "node map replacement",
				setup: "CREATE (:X {p: 1})",
				query: "MATCH (n:X {p: 1}) DELETE n SET n = {q: 1}",
				want:  1,
			},
			{
				name:  "relationship property",
				setup: "CREATE (:A)-[:R {w: 1}]->(:B)",
				query: "MATCH ()-[r:R]->() DELETE r SET r.q = 1",
				want:  1,
			},
			{
				name:  "relationship property null",
				setup: "CREATE (:A)-[:R {w: 1}]->(:B)",
				query: "MATCH ()-[r:R]->() DELETE r SET r.q = null",
				want:  0,
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				exec, ctx := deletedSetFixture(t)
				_, err := exec.Execute(ctx, testCase.setup, nil)
				require.NoError(t, err)
				result, err := exec.Execute(ctx, testCase.query, nil)
				require.NoError(t, err)
				require.Equal(t, testCase.want, result.Stats.PropertiesSet, testCase.query)
			})
		}
	})
}

// A label added to a deleted node is the deleted-entity read error (#907):
// adding a label reads the entity's labels, and Neo4j answers EntityNotFound
// for a deleted node. An empty dynamic label list names no label, so it reads
// nothing, succeeds and adds nothing.
func TestDeletedEntitySetLabelAdditionFails(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		for _, testCase := range []struct {
			name   string
			query  string
			params map[string]interface{}
		}{
			{"static label", "MATCH (n:X {p: 1}) DELETE n SET n:Extra", nil},
			{"static label chain", "MATCH (n:X {p: 1}) DELETE n SET n:Extra:More", nil},
			{"dynamic label", "MATCH (n:X {p: 1}) WITH n, {l: 'Extra'} AS m DELETE n SET n:$(m.l)", nil},
			{"dynamic label parameter", "MATCH (n:X {p: 1}) DELETE n SET n:$($l)", map[string]interface{}{"l": "Extra"}},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				exec, ctx := deletedSetFixture(t)
				_, err := exec.Execute(ctx, "CREATE (:X {p: 1})", nil)
				require.NoError(t, err)
				_, err = exec.Execute(ctx, testCase.query, testCase.params)
				requireDeletedEntityError(t, err)
			})
		}
		t.Run("empty dynamic label list adds nothing", func(t *testing.T) {
			exec, ctx := deletedSetFixture(t)
			_, err := exec.Execute(ctx, "CREATE (:X {p: 1})", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, "MATCH (n:X {p: 1}) WITH n, {l: []} AS m DELETE n SET n:$(m.l) RETURN n {.*} AS v", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{map[string]interface{}{}}}, result.Rows)
			require.Equal(t, 0, result.Stats.LabelsAdded)
			requireCount(t, exec, ctx, "MATCH (n:X) RETURN count(n) AS c", 0)
		})
	})
}

// A deleted target does not change the error its assignment raises (#907):
// the right-hand side and the dynamic key/label values are evaluated and
// validated in Neo4j's order, so an ArithmeticError, a TypeError or a
// TokenNameError is that error, and a deleted entity's property read is
// EntityNotFound. The key is read before the value, so a null key is the key's
// TypeError even when the value divides by zero.
func TestDeletedEntitySetErrorControlsMatchNeo4j(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		const setup = "CREATE (:X {p: 1})"
		for _, testCase := range []struct {
			name   string
			query  string
			params map[string]interface{}
			code   string
		}{
			{
				name:  "value arithmetic error",
				query: "MATCH (n:X {p: 1}) DELETE n SET n.q = 1 / 0",
				code:  "Neo.ClientError.Statement.ArithmeticError",
			},
			{
				name:  "value null map merge",
				query: "MATCH (n:X {p: 1}) DELETE n SET n += null",
				code:  "Neo.ClientError.Statement.TypeError",
			},
			{
				name:  "value null map replacement",
				query: "MATCH (n:X {p: 1}) DELETE n SET n = null",
				code:  "Neo.ClientError.Statement.TypeError",
			},
			{
				name:  "nested map value in merge",
				query: "MATCH (n:X {p: 1}) DELETE n SET n += {q: {x: 1}}",
				code:  "Neo.ClientError.Statement.TypeError",
			},
			{
				name:  "nested map value in replacement",
				query: "MATCH (n:X {p: 1}) DELETE n SET n = {q: {x: 1}}",
				code:  "Neo.ClientError.Statement.TypeError",
			},
			{
				name:   "null dynamic key",
				query:  "MATCH (n:X {p: 1}) DELETE n SET n[$k] = 1",
				params: map[string]interface{}{"k": nil},
				code:   "Neo.ClientError.Statement.TypeError",
			},
			{
				name:   "null dynamic key before the value's arithmetic error",
				query:  "MATCH (n:X {p: 1}) DELETE n SET n[$k] = 1 / 0",
				params: map[string]interface{}{"k": nil},
				code:   "Neo.ClientError.Statement.TypeError",
			},
			{
				name:   "null label value",
				query:  "MATCH (n:X {p: 1}) DELETE n SET n:$($l)",
				params: map[string]interface{}{"l": nil},
				code:   "Neo.ClientError.Statement.TypeError",
			},
			{
				name:   "label list holding null",
				query:  "MATCH (n:X {p: 1}) DELETE n SET n:$($l)",
				params: map[string]interface{}{"l": []interface{}{nil}},
				code:   "Neo.ClientError.Statement.TypeError",
			},
			{
				name:   "empty label name",
				query:  "MATCH (n:X {p: 1}) DELETE n SET n:$($l)",
				params: map[string]interface{}{"l": ""},
				code:   "Neo.ClientError.Schema.TokenNameError",
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				exec, ctx := deletedSetFixture(t)
				_, err := exec.Execute(ctx, setup, nil)
				require.NoError(t, err)
				_, err = exec.Execute(ctx, testCase.query, testCase.params)
				require.Error(t, err, testCase.query)
				code, message := nornicerrors.Neo4jStatus(err)
				require.Equal(t, testCase.code, code, "%s: %v", message, err)
			})
		}
		// A dynamic key or label whose value the request supplies (a parameter)
		// is typed when the statement is admitted, so Neo4j 5.26.30's
		// SyntaxError is reported before a row runs: the live target and the
		// target the same statement deleted report it alike, because the
		// deletion is not part of admission. A value a row computes keeps its
		// run-time classification (TestDynamicLabelsAndKeysInWrites).
		for _, testCase := range []struct {
			name    string
			live    string
			deleted string
			params  map[string]interface{}
		}{
			{
				name:    "integer parameter dynamic key",
				live:    "MATCH (n:X {p: 1}) SET n[$k] = 1",
				deleted: "MATCH (n:X {p: 1}) DELETE n SET n[$k] = 1",
				params:  map[string]interface{}{"k": int64(1)},
			},
			{
				name:    "integer parameter label",
				live:    "MATCH (n:X {p: 1}) SET n:$($l)",
				deleted: "MATCH (n:X {p: 1}) DELETE n SET n:$($l)",
				params:  map[string]interface{}{"l": int64(1)},
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				reported := make([]string, 0, 2)
				for _, query := range []string{testCase.live, testCase.deleted} {
					exec, ctx := deletedSetFixture(t)
					_, err := exec.Execute(ctx, setup, nil)
					require.NoError(t, err)
					_, err = exec.Execute(ctx, query, testCase.params)
					require.Error(t, err, query)
					code, message := nornicerrors.Neo4jStatus(err)
					require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, "%s: %v", query, err)
					reported = append(reported, code+" ("+message+")")
				}
				require.Equal(t, reported[0], reported[1], "the deleted target changed the dynamic token's error")
			})
		}
		// A value that reads the deleted entity's own property is the
		// deleted-entity read error, whatever the read's shape is.
		for _, query := range []string{
			"MATCH (n:X {p: 1}) DELETE n SET n.q = n.p",
			"MATCH (n:X {p: 1}) DELETE n WITH n, {k: 'q'} AS m DELETE n SET n[m.k] = n.p",
			"MATCH (n:X {p: 1}) DELETE n SET n:$(n.p)",
		} {
			t.Run(query, func(t *testing.T) {
				exec, ctx := deletedSetFixture(t)
				_, err := exec.Execute(ctx, setup, nil)
				require.NoError(t, err)
				_, err = exec.Execute(ctx, query, nil)
				requireDeletedEntityError(t, err)
			})
		}
		t.Run("empty foreach evaluates no right-hand side", func(t *testing.T) {
			exec, ctx := deletedSetFixture(t)
			_, err := exec.Execute(ctx, setup, nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, "MATCH (n:X {p: 1}) DELETE n FOREACH (x IN [] | SET x.q = 1 / 0) RETURN 1 AS v", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		})
	})
}

// errStoredSetWrite fails an update, so a SET that publishes through storage
// reports it while a SET the statement's own deletion drops does not.
var errStoredSetWrite = errors.New("SET published to storage")

// A SET whose target the statement did not delete still publishes through
// storage, so a peer's delete of that entity stays the write's own failure:
// only the statement's own deletion is dropped (setTargetDeletedByStatement).
// A statement's writes run inside the statement's own implicit transaction,
// so the peer's delete is represented here at the canonical owner
// (persistSetEntities) by a store whose update reports storage.ErrNotFound,
// with the statement's registry holding a different entity.
func TestDeletedEntitySetKeepsPeerDeletionConflicts(t *testing.T) {
	newState := func(edge bool) setEntityState {
		if edge {
			relationship := &storage.Edge{ID: "e1", Type: "R", StartNode: "n1", EndNode: "n2"}
			relationship.Properties = map[string]interface{}{"w": int64(1), "q": int64(1)}
			return setEntityState{
				variable:     "r",
				relationship: relationship,
				properties:   map[string]interface{}{"w": int64(1)},
				written:      1,
			}
		}
		node := &storage.Node{ID: "n1"}
		node.Properties = map[string]interface{}{"p": int64(1), "q": int64(1)}
		return setEntityState{
			variable:   "n",
			node:       node,
			properties: map[string]interface{}{"p": int64(1)},
			labels:     []string{"X"},
			written:    1,
		}
	}
	wrap := func(_ setEntityState, err error) error { return err }
	for _, testCase := range []struct {
		name string
		edge bool
	}{
		{"node", false},
		{"relationship", true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			state := newState(testCase.edge)
			failing := &updateErrorEngine{
				Engine:  storage.NewNamespacedEngine(newTestMemoryEngine(t), "deleted_set_peer"),
				nodeErr: storage.ErrNotFound,
				edgeErr: storage.ErrNotFound,
			}
			// The statement deleted an entity of its own: not this target.
			ctx := contextslot.Ensure[deletedEntities](t.Context())
			contextslot.Get[deletedEntities](ctx).add(
				[]storage.NodeID{storage.NodeID("deleted_set_peer:deleted-by-statement")},
				map[storage.EdgeID]struct{}{"deleted_set_peer:deleted-edge": {}},
			)
			stats := &QueryStats{}
			err := NewStorageExecutor(failing).persistSetEntities(ctx, failing, []setEntityState{state}, stats, wrap)
			require.Error(t, err)
			require.True(t, errors.Is(err, storage.ErrNotFound), "the write still reaches storage: %v", err)
			require.Equal(t, 1, stats.PropertiesSet)
		})
	}
	t.Run("the statement's own deletion is not published", func(t *testing.T) {
		for _, edge := range []bool{false, true} {
			state := newState(edge)
			skipping := &updateErrorEngine{
				Engine:  storage.NewNamespacedEngine(newTestMemoryEngine(t), "deleted_set_own"),
				nodeErr: errStoredSetWrite,
				edgeErr: errStoredSetWrite,
			}
			ctx := contextslot.Ensure[deletedEntities](t.Context())
			if edge {
				contextslot.Get[deletedEntities](ctx).add(nil, map[storage.EdgeID]struct{}{storage.EdgeID(state.relationship.IdentityKey()): {}})
			} else {
				contextslot.Get[deletedEntities](ctx).add([]storage.NodeID{storage.NodeID(state.node.IdentityKey())}, nil)
			}
			stats := &QueryStats{}
			err := NewStorageExecutor(skipping).persistSetEntities(ctx, skipping, []setEntityState{state}, stats, wrap)
			require.NoError(t, err)
			require.Equal(t, 1, stats.PropertiesSet)
		}
	})
}
