package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type relationshipScanGuardEngine struct {
	*storage.MemoryEngine
	forbidLabelScan bool
	outgoingCalls   int
}

func (e *relationshipScanGuardEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if e.forbidLabelScan {
		return nil, fmt.Errorf("GetNodesByLabel should not be called for indexed relationship start-node pruning")
	}
	return e.MemoryEngine.GetNodesByLabel(label)
}

func (e *relationshipScanGuardEngine) AllNodes() ([]*storage.Node, error) {
	if e.forbidLabelScan {
		return nil, fmt.Errorf("AllNodes should not be called for indexed relationship start-node pruning")
	}
	return e.MemoryEngine.AllNodes()
}

func (e *relationshipScanGuardEngine) GetOutgoingEdges(nodeID storage.NodeID) ([]*storage.Edge, error) {
	e.outgoingCalls++
	return e.MemoryEngine.GetOutgoingEdges(nodeID)
}

func seedRelationshipLimitData(t *testing.T, eng *relationshipScanGuardEngine, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		origID := storage.NodeID(fmt.Sprintf("nornic:orig-%03d", i))
		trID := storage.NodeID(fmt.Sprintf("nornic:tr-%03d", i))
		_, err := eng.CreateNode(&storage.Node{
			ID:     origID,
			Labels: []string{"OriginalText"},
			Properties: map[string]interface{}{
				"originalText": fmt.Sprintf("src-%03d", i),
			},
		})
		require.NoError(t, err)
		_, err = eng.CreateNode(&storage.Node{
			ID:     trID,
			Labels: []string{"TranslatedText"},
			Properties: map[string]interface{}{
				"language":       "es",
				"translatedText": fmt.Sprintf("dst-%03d", i),
			},
		})
		require.NoError(t, err)
		require.NoError(t, eng.CreateEdge(&storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("nornic:edge-%03d", i)),
			Type:      "TRANSLATES_TO",
			StartNode: origID,
			EndNode:   trID,
		}))
	}
}

func TestRelationshipMatchLimitShortCircuitsTraversal(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &relationshipScanGuardEngine{MemoryEngine: base}
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	seedRelationshipLimitData(t, eng, 50)

	res, err := exec.Execute(ctx, `
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
RETURN o.originalText AS originalText
LIMIT 1
`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.LessOrEqual(t, eng.outgoingCalls, 2, "LIMIT 1 should stop traversal after the first start-node match")
}

func TestRelationshipMatchUsesIndexForIsNotNullStartNodeFilter(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &relationshipScanGuardEngine{MemoryEngine: base}
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	seedRelationshipLimitData(t, eng, 8)
	_, err := exec.Execute(ctx, "CREATE INDEX idx_original_text FOR (o:OriginalText) ON (o.originalText)", nil)
	require.NoError(t, err)
	eqWhere, err := exec.Execute(ctx, "MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText) WHERE o.originalText = 'src-000' RETURN o.originalText AS originalText LIMIT 1", nil)
	require.NoError(t, err)
	require.Len(t, eqWhere.Rows, 1)

	warmup, err := exec.Execute(ctx, `
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
WHERE o.originalText IS NOT NULL
RETURN
  o.originalText AS originalText,
  t.language AS language,
  t.translatedText AS translatedText
ORDER BY t.language
LIMIT 1
`, nil)
	require.NoError(t, err)
	require.Len(t, warmup.Rows, 1)

	eng.forbidLabelScan = true

	res, err := exec.Execute(ctx, `
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
WHERE o.originalText IS NOT NULL
RETURN
  o.originalText AS originalText,
  t.language AS language,
  t.translatedText AS translatedText
ORDER BY t.language
LIMIT 1
`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 3)
}

func TestRelationshipMatchTranslationQueryFamily_EndNodeFilterOrderLimit(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &relationshipScanGuardEngine{MemoryEngine: base}
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	seedTranslationQueryFamilyData(t, eng, "nornic:", 15, 4)
	_, err := exec.Execute(ctx, "CREATE INDEX idx_translated_created_at FOR (t:TranslatedText) ON (t.createdAt)", nil)
	require.NoError(t, err)

	// This filter permits missing createdAt values. A partial non-null index
	// window must allow normal selection to include those potential matches.

	res, err := exec.Execute(ctx, `
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
WHERE t.language = 'fr'
RETURN o, t, t.createdAt
ORDER BY t.createdAt DESC
LIMIT 10
`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"o", "t", "t.createdAt"}, res.Columns)
	require.Len(t, res.Rows, 10)
	require.False(t, exec.LastHotPathTrace().TraversalEndSeedTopK)

	for i, row := range res.Rows {
		require.Len(t, row, 3)
		orig, ok := row[0].(*storage.Node)
		require.True(t, ok)
		tr, ok := row[1].(*storage.Node)
		require.True(t, ok)
		require.Equal(t, "fr", tr.Properties["language"])
		require.Equal(t, tr.Properties["createdAt"], row[2])
		require.Equal(t, fmt.Sprintf("fr-%02d", 14-i), orig.Properties["textKey"])
	}

	withoutProjectedSortKey, err := exec.Execute(ctx, `
MATCH (o:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText)
WHERE t.language = 'fr'
RETURN o.textKey AS textKey, t
ORDER BY t.createdAt DESC
LIMIT 3
`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"textKey", "t"}, withoutProjectedSortKey.Columns)
	require.Len(t, withoutProjectedSortKey.Rows, 3)
	require.Equal(t, "fr-14", withoutProjectedSortKey.Rows[0][0])
	require.Equal(t, "fr-13", withoutProjectedSortKey.Rows[1][0])
	require.Equal(t, "fr-12", withoutProjectedSortKey.Rows[2][0])
}

func TestRelationshipMatchEndSeededTopKSupportsFixedMultiHop(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	exec := NewStorageExecutor(base)
	ctx := context.Background()

	for i := 0; i < 6; i++ {
		origID := storage.NodeID(fmt.Sprintf("nornic:o-hop-%d", i))
		midID := storage.NodeID(fmt.Sprintf("nornic:m-hop-%d", i))
		targetID := storage.NodeID(fmt.Sprintf("nornic:t-hop-%d", i))
		_, err := base.CreateNode(&storage.Node{ID: origID, Labels: []string{"OriginalText"}, Properties: map[string]interface{}{"textKey": fmt.Sprintf("hop-%d", i)}})
		require.NoError(t, err)
		_, err = base.CreateNode(&storage.Node{ID: midID, Labels: []string{"Bridge"}, Properties: map[string]interface{}{"rank": i}})
		require.NoError(t, err)
		_, err = base.CreateNode(&storage.Node{ID: targetID, Labels: []string{"TranslatedText"}, Properties: map[string]interface{}{"language": "fr", "createdAt": fmt.Sprintf("2026-04-%02dT12:00:00Z", i+1)}})
		require.NoError(t, err)
		require.NoError(t, base.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("nornic:e-hop-a-%d", i)), Type: "STEP", StartNode: origID, EndNode: midID}))
		require.NoError(t, base.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("nornic:e-hop-b-%d", i)), Type: "STEP", StartNode: midID, EndNode: targetID}))
	}

	_, err := exec.Execute(ctx, "CREATE INDEX idx_hop_target_created_at FOR (t:TranslatedText) ON (t.createdAt)", nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `
MATCH (o:OriginalText)-[:STEP*2]->(t:TranslatedText)
WHERE t.language = 'fr'
RETURN o.textKey AS textKey, t.createdAt AS createdAt
ORDER BY t.createdAt DESC
LIMIT 2
`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)
	require.Equal(t, []interface{}{"hop-5", "2026-04-06T12:00:00Z"}, res.Rows[0])
	require.Equal(t, []interface{}{"hop-4", "2026-04-05T12:00:00Z"}, res.Rows[1])
	require.False(t, exec.LastHotPathTrace().TraversalEndSeedTopK)
}
