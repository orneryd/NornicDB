// Author: NOR-292 lane (Codex), 2026-10-29.
package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestSemanticMetadataNilPreservesPropertyEncodingContract(t *testing.T) {
	metadata, err := msgpack.Marshal(semanticMetadataValues{})
	require.NoError(t, err)
	require.Equal(t, []byte{0xc0}, metadata, "absent metadata is MessagePack nil")
	var decodedMetadata semanticMetadataValues
	require.NoError(t, msgpack.Unmarshal(metadata, &decodedMetadata))
	require.Nil(t, decodedMetadata.values)
	properties, err := msgpack.Marshal(semanticPropertyValues{})
	require.NoError(t, err)
	require.Equal(t, []byte{0x80}, properties, "TJ's nil property map remains an empty map")
	var decodedProperties semanticPropertyValues
	require.NoError(t, msgpack.Unmarshal(properties, &decodedProperties))
	require.NotNil(t, decodedProperties.values)
	require.Empty(t, decodedProperties.values)
	for _, values := range []map[string]any{{}, {"model": "kept", "nested": map[string]any{}, "list": []any{"kept", int64(3)}}} {
		propertyRules := semanticPropertyValues{values: values}
		want, err := msgpack.Marshal(propertyRules)
		require.NoError(t, err)
		got, err := msgpack.Marshal(semanticMetadataValues{propertyRules})
		require.NoError(t, err)
		// MessagePack maps have no field-order contract. Compare each encoded
		// value byte-for-byte, keyed by field name, so independent Go map
		// iteration cannot make this delegation oracle nondeterministic.
		var wantFields, gotFields map[string]msgpack.RawMessage
		require.NoError(t, msgpack.Unmarshal(want, &wantFields))
		require.NoError(t, msgpack.Unmarshal(got, &gotFields))
		require.Equal(t, wantFields, gotFields, "non-nil metadata delegates unchanged value encoding")
	}
	// The six-field public accepted/live/cold image matrix separately proves
	// all other five collection fields, nil/empty/populated, remain unchanged.
}
