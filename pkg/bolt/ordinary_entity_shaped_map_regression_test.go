package bolt

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOrdinaryEntityShapedMapKeepsBoltMapIdentity(t *testing.T) {
	object := map[string]interface{}{"_nodeId": "n1", "labels": []interface{}{"L"}}
	variants := [][]byte{encodePackStreamValue(object)}
	for _, utc := range []bool{false, true} {
		recordWire, encodeErr := encodeRecordValueInto(t.Context(), nil, object, utc, true, "test")
		require.NoError(t, encodeErr)
		variants = append(variants, encodePackStreamValueIntoWithUTC(nil, object, utc), recordWire)
	}
	for _, wire := range variants {
		require.Equal(t, byte(0xA2), wire[0], "ordinary map must keep map tag")
		got, used, err := decodePackStreamValue(wire, 0)
		require.NoError(t, err)
		require.Equal(t, len(wire), used)
		require.Equal(t, object, got)
	}
}
