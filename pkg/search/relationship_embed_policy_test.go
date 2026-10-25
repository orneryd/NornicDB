package search

import (
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Saved relationship vectors are reused under the embedding policy they
// were made with, and embedded again under another (a changed property
// list).
func TestRelationshipVectorsFollowTheEmbedPolicy(t *testing.T) {
	engine := relationshipTestEngine(t)
	path := filepath.Join(t.TempDir(), "bm25")
	persisted := func(policy string, embed *countingEmbed) *Service {
		return startRelationshipService(t, engine, embed.embed, func(s *Service) {
			s.SetPersistenceEnabled(true)
			s.SetFulltextIndexPath(path)
			s.EnableRelationshipSearch(embed.embed)
			s.SetRelationshipEmbedPolicy(policy)
		})
	}
	first := &countingEmbed{}
	firstService := persisted("include=search_text", first)
	waitRelationshipVectors(t, firstService, 2, relationshipSnapshotPublished)
	require.EqualValues(t, 2, first.calls.Load())

	same := &countingEmbed{}
	sameService := persisted("include=search_text", same)
	waitRelationshipVectors(t, sameService, 2, relationshipSnapshotPublished)
	require.EqualValues(t, 0, same.calls.Load(), "the saved vectors are reused")

	changed := &countingEmbed{}
	changedService := persisted("include=text", changed)
	waitRelationshipVectors(t, changedService, 2, relationshipSnapshotPublished)
	require.EqualValues(t, 2, changed.calls.Load(), "a new policy embeds again")

	var nilService *Service
	nilService.SetRelationshipEmbedPolicy("x")
	require.NotEqual(t, relationshipSource(&storage.Edge{Type: "A"}), (&relationshipSearch{policy: 7}).source(&storage.Edge{Type: "A"}))
}
