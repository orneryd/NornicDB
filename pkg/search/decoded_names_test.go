package search

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDecodedNameFulltextSelectors(t *testing.T) {
	for _, name := range []string{" ", "  ", "\t"} {
		service := NewService(newTestMemoryEngine(t), DefaultVectorDimensions, nil)
		service.SetFulltextLabels([]string{"", name, name})
		service.SetFulltextProperties([]string{"", name, name})
		service.SetRelationshipFulltextProperties([]string{"", name, name})
		require.Equal(t, []string{name}, service.FulltextLabels())
		require.Equal(t, []string{name}, service.FulltextProperties())
		require.Equal(t, []string{name}, service.RelationshipFulltextProperties())
		listed := &storage.Node{ID: "exact", Labels: []string{name}, Properties: map[string]interface{}{name: "zephyr report"}}
		other := &storage.Node{ID: "other", Labels: []string{"Other"}, Properties: map[string]interface{}{name: "zephyr report"}}
		require.NoError(t, service.IndexNode(listed))
		require.NoError(t, service.IndexNode(other))
		hits := service.fulltext().Search("zephyr", 10)
		require.Len(t, hits, 1)
		require.Equal(t, "exact", hits[0].ID)
	}
}
