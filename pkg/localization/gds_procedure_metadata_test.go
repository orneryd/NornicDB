package localization

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGDSGraphExistsMetadata(t *testing.T) {
	require.Equal(t, "Checks if a graph exists in the catalog.", CypherProcedureMetadataGdsGraphExists().Fallback)
	require.Equal(t, CypherProcedureMetadataGdsGraphExists().ID, CypherProcedureMetadata("gds.graph.exists").ID)
}

func TestGDSNodeSimilarityMetadata(t *testing.T) {
	require.Contains(t, CypherProcedureMetadataGdsNodeSimilarityStream().Fallback, "Node Similarity algorithm")
	require.Contains(t, CypherProcedureMetadataGdsNodeSimilarityFilteredStream().Fallback, "Filtered Node Similarity")
}

func TestGDSLouvainMetadata(t *testing.T) {
	require.Equal(t, "The Louvain method for community detection is an algorithm for detecting communities in networks.", CypherProcedureMetadataGdsLouvainStream().Fallback)
}
