package cypher

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func seedTranslationQueryFamilyData(t *testing.T, store storage.Engine, idPrefix string, frenchCount, secondaryCount int) {
	t.Helper()
	for i := 0; i < frenchCount; i++ {
		origID := storage.NodeID(fmt.Sprintf("%sorig-fr-%02d", idPrefix, i))
		trID := storage.NodeID(fmt.Sprintf("%str-fr-%02d", idPrefix, i))
		_, err := store.CreateNode(&storage.Node{
			ID:     origID,
			Labels: []string{"OriginalText"},
			Properties: map[string]interface{}{
				"textKey":      fmt.Sprintf("fr-%02d", i),
				"originalText": fmt.Sprintf("source-fr-%02d", i),
			},
		})
		require.NoError(t, err)
		_, err = store.CreateNode(&storage.Node{
			ID:     trID,
			Labels: []string{"TranslatedText"},
			Properties: map[string]interface{}{
				"language":       "fr",
				"translatedText": fmt.Sprintf("bonjour-%02d", i),
				"createdAt":      fmt.Sprintf("2026-04-%02dT12:00:00Z", i+1),
			},
		})
		require.NoError(t, err)
		require.NoError(t, store.CreateEdge(&storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("%sedge-fr-%02d", idPrefix, i)),
			Type:      "TRANSLATES_TO",
			StartNode: origID,
			EndNode:   trID,
		}))
	}

	for i := 0; i < secondaryCount; i++ {
		origID := storage.NodeID(fmt.Sprintf("%sorig-es-%02d", idPrefix, i))
		trID := storage.NodeID(fmt.Sprintf("%str-es-%02d", idPrefix, i))
		_, err := store.CreateNode(&storage.Node{
			ID:     origID,
			Labels: []string{"OriginalText"},
			Properties: map[string]interface{}{
				"textKey":      fmt.Sprintf("es-%02d", i),
				"originalText": fmt.Sprintf("source-es-%02d", i),
			},
		})
		require.NoError(t, err)
		_, err = store.CreateNode(&storage.Node{
			ID:     trID,
			Labels: []string{"TranslatedText"},
			Properties: map[string]interface{}{
				"language":       "es",
				"translatedText": fmt.Sprintf("hola-%02d", i),
				"createdAt":      fmt.Sprintf("2026-05-%02dT12:00:00Z", i+1),
			},
		})
		require.NoError(t, err)
		require.NoError(t, store.CreateEdge(&storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("%sedge-es-%02d", idPrefix, i)),
			Type:      "TRANSLATES_TO",
			StartNode: origID,
			EndNode:   trID,
		}))
	}
}
