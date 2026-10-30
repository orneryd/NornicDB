package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeLegacyNode_ErrorBranches(t *testing.T) {
	t.Run("malformed serialization header version", func(t *testing.T) {
		data := append([]byte(serializationMagic), byte(99), serializerIDMsgpack)
		_, err := decodeLegacyNode(data)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported serialization version")
	})

	t.Run("header payload decode failure", func(t *testing.T) {
		// Valid header envelope with invalid msgpack payload for legacy node.
		data := append([]byte(serializationMagic), serializationVersion, serializerIDMsgpack)
		data = append(data, []byte("not-msgpack-legacy-node")...)
		_, err := decodeLegacyNode(data)
		require.Error(t, err)
	})
}
