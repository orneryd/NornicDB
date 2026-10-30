package localization

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestQdrantDeletePointFailureCatalog(t *testing.T) {
	message := QdrantDeletePointFailed("point-7")
	require.Equal(t, MessageQdrantDeletePointFailed, message.ID)
	require.Equal(t, map[string]any{"PointID": "point-7"}, message.Data)
	require.Equal(t, "failed to delete point point-7", message.Fallback)
}
