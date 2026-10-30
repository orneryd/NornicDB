package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Relationship search's keyword index reads its own property allowlist,
// apart from the nodes': a relationship with none of the listed properties
// isn't found, and its term matches come from the listed ones; unset, the
// node list applies.
func TestRelationshipSearchReadsItsOwnPropertyList(t *testing.T) {
	ctx := context.Background()
	engine := relationshipTestEngine(t)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "opted", StartNode: "a", EndNode: "c", Type: "LINK", Properties: map[string]any{"search_text": "harbour explanation", "text": "user note harbour"}}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "plain", StartNode: "c", EndNode: "a", Type: "LINK", Properties: map[string]any{"text": "harbour note"}}))
	svc := startRelationshipService(t, engine, nil, func(s *Service) {
		s.SetFulltextProperties([]string{"text", "search_text"})
		s.SetRelationshipFulltextProperties([]string{" search_text ", "search_text"})
	})
	require.Equal(t, []string{"text", "search_text"}, svc.FulltextProperties())
	require.Equal(t, []string{"search_text"}, svc.RelationshipFulltextProperties())

	response, err := svc.SearchRelationships(ctx, "harbour", nil, &SearchOptions{Limit: 5})
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	require.Equal(t, "opted", response.Results[0].ID)
	for _, match := range response.Results[0].Matches {
		require.Equal(t, "search_text", match.Property)
	}

	// Unset, the node list applies.
	fallback := startRelationshipService(t, engine, nil, func(s *Service) {
		s.SetFulltextProperties([]string{"text"})
	})
	require.Equal(t, []string{"text"}, fallback.RelationshipFulltextProperties())
	response, err = fallback.SearchRelationships(ctx, "harbour", nil, &SearchOptions{Limit: 5})
	require.NoError(t, err)
	require.Len(t, response.Results, 2)
}
