package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	dbconfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// These adapters exercise the actual client batch consumer against Execute;
// they do not claim Bolt transport or live Neo4j source coverage.
type migrationSession struct {
	neo4j.SessionWithContext
	run func(context.Context, string, map[string]any) (neo4j.ResultWithContext, error)
}

func (s migrationSession) Run(ctx context.Context, q string, p map[string]any, _ ...func(*neo4j.TransactionConfig)) (neo4j.ResultWithContext, error) {
	return s.run(ctx, q, p)
}

type migrationResult struct {
	neo4j.ResultWithContext
	record                *neo4j.Record
	singleErr, consumeErr error
	consumed              bool
}

func (r *migrationResult) Single(context.Context) (*neo4j.Record, error) {
	return r.record, r.singleErr
}
func (r *migrationResult) Consume(context.Context) (neo4j.ResultSummary, error) {
	r.consumed = true
	return nil, r.consumeErr
}

func TestMigrationCompletedGraph(t *testing.T) {
	for _, parser := range []string{dbconfig.ParserTypeNornic, dbconfig.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := dbconfig.GetParserType()
			dbconfig.SetParserType(parser)
			t.Cleanup(func() { dbconfig.SetParserType(previous) })
			raw := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			t.Cleanup(func() { _ = raw.Close() })
			store := storage.NewNamespacedEngine(raw, "migration")
			exec := cypher.NewStorageExecutor(store)
			ctx := context.Background()
			run := func(q string, p map[string]any) *cypher.ExecuteResult {
				r, err := exec.Execute(ctx, q, p)
				require.NoError(t, err, q)
				return r
			}
			target := migrationSession{run: func(_ context.Context, q string, p map[string]any) (neo4j.ResultWithContext, error) {
				r := run(q, p)
				require.Len(t, r.Rows, 1)
				return &migrationResult{record: &neo4j.Record{Keys: r.Columns, Values: r.Rows[0]}}, nil
			}}
			nodes := []map[string]any{
				{"id": "n1", "labels": []any{"Person", "Employee"}, "props": map[string]any{"name": "Alice", "team": "platform"}},
				{"id": "n2", "labels": []any{"Person"}, "props": map[string]any{"name": "Bob"}},
			}
			edges := []map[string]any{
				{"id": "r1", "startId": "n1", "endId": "n2", "props": map[string]any{"since": int64(2020)}},
				{"id": "r2", "startId": "n1", "endId": "n2", "props": map[string]any{}},
				{"id": "r3", "startId": "n1", "endId": "n1", "props": map[string]any{"since": int64(2021)}},
			}
			doc, err := os.ReadFile("README.md")
			require.NoError(t, err)
			blocks := strings.Split(string(doc), "```cypher\n")
			require.GreaterOrEqual(t, len(blocks), 3)
			promotion := strings.Split(blocks[1], "```")[0]
			cleanup := strings.Split(blocks[2], "```")[0]
			skill, err := os.ReadFile("../../../docs/skills/neo4j-migration.skill.md")
			require.NoError(t, err)
			require.Contains(t, string(skill), promotion)
			require.Contains(t, string(skill), cleanup)
			for attempt := 0; attempt < 2; attempt++ {
				nodes[0]["props"].(map[string]any)["revision"] = int64(attempt)
				edges[0]["props"].(map[string]any)["since"] = int64(2020 + attempt)
				applied, err := applyBatch(ctx, target, nodeUpsertCypher, nodes)
				require.NoError(t, err)
				require.Equal(t, 2, applied)
				applied, err = applyBatch(ctx, target, edgeUpsertCypher("KNOWS"), edges)
				require.NoError(t, err)
				require.Equal(t, 3, applied)
				require.Equal(t, [][]any{{int64(2)}}, run(promotion, nil).Rows)
				require.Equal(t, [][]any{{int64(1)}}, run(strings.ReplaceAll(promotion, "Person", "Employee"), nil).Rows)
				run(cleanup, nil)
				require.Equal(t, [][]any{{"Alice"}, {"Bob"}}, run("MATCH (n:Person) RETURN n.name ORDER BY n.name", nil).Rows)
				require.Equal(t, [][]any{{"Alice", "platform", int64(attempt)}}, run("MATCH (n:Employee) RETURN n.name,n.team,n.revision", nil).Rows)
				require.Equal(t, [][]any{{"r1", "Alice", "Bob", int64(2020 + attempt)}, {"r2", "Alice", "Bob", nil}, {"r3", "Alice", "Alice", int64(2021)}}, run("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r._neo4j_id,a.name,b.name,r.since ORDER BY r._neo4j_id", nil).Rows)
				// Whole graph counts and identities pin post-promotion rerun behavior.
				allNodes, err := store.AllNodes(ctx)
				require.NoError(t, err)
				require.Len(t, allNodes, 2)
				allEdges, err := store.AllEdges(ctx)
				require.NoError(t, err)
				require.Len(t, allEdges, 3)
				for _, node := range allNodes {
					require.Contains(t, node.Labels, "Neo4jMigrated")
					require.NotEmpty(t, node.Properties["_neo4j_id"])
					require.NotContains(t, node.Properties, "_neo4j_labels")
				}
			}
			missing := []map[string]any{{"id": "missing-edge", "startId": "n1", "endId": "absent", "props": map[string]any{}}}
			applied, err := applyBatch(ctx, target, edgeUpsertCypher("KNOWS"), missing)
			require.Zero(t, applied)
			require.ErrorContains(t, err, "target applied 0 of 1")
			allEdges, err := store.AllEdges(ctx)
			require.NoError(t, err)
			require.Len(t, allEdges, 3)
			run("CREATE (:_internal {name:'system'})", nil)
			require.Empty(t, run("MATCH (n:_internal) RETURN n", nil).Rows)
			require.Equal(t, [][]any{{int64(2)}}, run("MATCH (n) RETURN count(n)", nil).Rows)
		})
	}
}

func TestMigrationBatchResultErrors(t *testing.T) {
	failure := errors.New("driver failure")
	for _, tc := range []struct {
		name     string
		result   migrationResult
		runErr   error
		want     string
		consumed bool
	}{
		{name: "missing count", result: migrationResult{record: &neo4j.Record{}}, want: "target applied <nil>", consumed: true},
		{name: "partial count", result: migrationResult{record: &neo4j.Record{Keys: []string{"applied"}, Values: []any{int64(0)}}}, want: "target applied 0", consumed: true},
		{name: "wrong count type", result: migrationResult{record: &neo4j.Record{Keys: []string{"applied"}, Values: []any{"1"}}}, want: "incomplete", consumed: true},
		{name: "run failure", runErr: failure, want: "driver failure"},
		{name: "single failure", result: migrationResult{singleErr: failure}, want: "driver failure"},
		{name: "consume failure", result: migrationResult{record: &neo4j.Record{}, consumeErr: failure}, want: "driver failure", consumed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.result
			target := migrationSession{run: func(_ context.Context, q string, p map[string]any) (neo4j.ResultWithContext, error) {
				require.Equal(t, nodeUpsertCypher, q)
				require.Len(t, p["rows"], 1)
				return &result, tc.runErr
			}}
			count, err := applyBatch(context.Background(), target, nodeUpsertCypher, []map[string]any{{"id": "n1"}})
			require.Zero(t, count)
			require.ErrorContains(t, err, tc.want)
			require.Equal(t, tc.consumed, result.consumed)
		})
	}
}
