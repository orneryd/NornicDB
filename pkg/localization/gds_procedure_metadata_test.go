package localization

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGDSGraphExistsMetadata(t *testing.T) {
	require.Equal(t, "Checks if a graph exists in the catalog.", CypherProcedureMetadataGdsGraphExists().Fallback)
	require.Equal(t, CypherProcedureMetadataGdsGraphExists().ID, CypherProcedureMetadata("gds.graph.exists").ID)
}
