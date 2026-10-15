package bolt

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRemoteEntityValuePreservesOrdinaryMapEncoding(t *testing.T) {
	remote := storage.RemoteEntityValue{"elementId": "4:remote:n", "labels": []interface{}{"Doc"}, "properties": map[string]interface{}{"name": "Remote"}}
	plain := map[string]interface{}(remote)
	for _, utc := range []bool{false, true} {
		wire := encodeRecordValueInto(nil, plain, utc, true, "local")
		require.Equal(t, byte(0xa3), wire[0])
		require.Equal(t, byte(0x4e), encodeRecordValueInto(nil, remote, utc, true, "local")[1])
	}
}

// Map key order is intentionally unspecified on the Bolt wire. Compare the
// decoded values and map/list tags, rather than two randomized Go iterations.
func assertRemoteBoltValueEqual(t *testing.T, expected, actual []byte) {
	t.Helper()
	require.Equal(t, expected[0], actual[0])
	expectedValue, expectedUsed, err := decodePackStreamValue(expected, 0)
	require.NoError(t, err)
	require.Equal(t, len(expected), expectedUsed)
	actualValue, actualUsed, err := decodePackStreamValue(actual, 0)
	require.NoError(t, err)
	require.Equal(t, len(actual), actualUsed)
	require.Equal(t, expectedValue, actualValue)
}
