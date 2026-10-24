package storage

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// Supply a typed named after-image from the fixture's logical source. Merely
// wrapping arbitrary decoded legacy JSON would not restore its missing types.
func typedPropertyRecordFixture(t *testing.T, data WALBufferedCommitData, properties map[string]any) []byte {
	t.Helper()
	data.Ops = append([]Operation(nil), data.Ops...)
	for i := range data.Ops {
		if data.Ops[i].Node != nil {
			node := *data.Ops[i].Node
			node.Properties = properties
			data.Ops[i].Node = &node
		}
	}
	transformRecoveryEntities(&data, prepareRecoveryNode, prepareRecoveryEdge)
	typed, err := encodeValue(&data)
	require.NoError(t, err)
	payload, err := json.Marshal(walRecoverySidecar{Encoding: walRecoveryEncoding, Data: typed})
	require.NoError(t, err)
	return payload
}

func TestWALUnstampedForeignCannotBorrowPriorRecord(t *testing.T) {
	_, data := r2BindingData(t, 1)
	dest := reviewBindingEngine(t, t.TempDir())
	_, err := dest.CreateNode(&Node{ID: "perf:sentinel", Properties: map[string]any{"other": int64(0)}})
	require.NoError(t, err)
	payload, err := json.Marshal(data)
	require.NoError(t, err)
	require.NoError(t, ReplayWALEntry(dest, WALEntry{Operation: OpBufferedCommit, Data: payload}))
	data.TxID = "unstamped-suffix"
	data.PropertyBindings, data.PropKeys, data.PropertyDictionaryID = nil, nil, ""
	payload, err = json.Marshal(data)
	require.NoError(t, err)
	require.ErrorContains(t, ReplayWALEntry(dest, WALEntry{Operation: OpBufferedCommit, Data: payload}), "missing property name binding")
	node, err := dest.GetNode("perf:a")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"property_000": int64(0)}, node.Properties)
	assertPropertyDictionaryBijection(t, dest)
}
