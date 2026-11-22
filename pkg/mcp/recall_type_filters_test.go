package mcp

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestRecallTypeUnionPreservesOtherFilters(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			props := func(date string, tags []string) map[string]interface{} {
				return map[string]interface{}{"created_at": date, "tags": tags}
			}
			db, ids := seedMCPDiscoverGraph(t, []mcpDiscoverNodeSeed{
				{label: "Memory", props: props("2026-03-01T00:00:00Z", []string{"alpha", "beta"})},
				{label: "Task", props: props("2026-03-01T00:00:00Z", []string{"alpha"})},
				{label: "Task", props: props("2025-01-01T00:00:00Z", []string{"alpha", "beta"})},
				{label: "Noise", props: props("2026-03-01T00:00:00Z", []string{"alpha", "beta"})},
			}, nil)
			result, err := db.GetCypherExecutor().Execute(context.Background(),
				"CREATE (n:Memory:Task {created_at: $date, tags: $tags}) RETURN elementId(n)",
				map[string]interface{}{"date": "2026-03-01T00:00:00Z", "tags": []string{"alpha", "beta"}})
			require.NoError(t, err)
			multiID := result.Rows[0][0].(string)
			server := NewServer(db, nil)
			for _, transport := range nor100RecallTransports() {
				t.Run(transport.name, func(t *testing.T) {
					args := map[string]interface{}{
						"type": []string{"Bogus", "Memory", "Task", "Memory"},
						"tags": []string{"alpha", "beta"}, "since": "2026-01-01T00:00:00Z", "limit": 10,
					}
					recall := nor100DecodeRecall(t, transport.call(t, server, args))
					require.Equal(t, 2, recall.Count)
					require.ElementsMatch(t, []string{ids[0], multiID}, nor100RecallIDs(recall), "duplicate requested labels and multi-label nodes must not duplicate results")
					args["limit"] = 1
					recall = nor100DecodeRecall(t, transport.call(t, server, args))
					require.Equal(t, 1, recall.Count)
					require.Len(t, recall.Nodes, 1)
					require.Contains(t, []string{ids[0], multiID}, recall.Nodes[0].ID, "limit applies after all filters")
					recall = nor100DecodeRecall(t, transport.call(t, server, map[string]interface{}{"type": []string{"memory", "Task"}, "limit": 10}))
					require.Equal(t, 3, recall.Count)
					require.ElementsMatch(t, []string{ids[1], ids[2], multiID}, nor100RecallIDs(recall), "label matching stays case-sensitive")
				})
			}
		})
	}
}
