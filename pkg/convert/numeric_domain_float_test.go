package convert

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNumericDomainFloat32Text(t *testing.T) {
	for _, value := range []interface{}{float32(1.2), map[string]interface{}{"v": float32(1.2)}, []interface{}{float32(1.2)}, struct {
		V interface{} `json:"v"`
	}{float32(1.2)}} {
		data, err := PropertyJSON.Marshal(value)
		require.NoError(t, err)
		require.NotContains(t, string(data), "1.2000000476837158")
		require.Contains(t, string(data), "1.2")
	}
}

func TestNumericDomainCodecContracts(t *testing.T) {
	const document = `{"u":18446744073709551615,"whole":2.0}`
	var property, input map[string]interface{}
	require.NoError(t, PropertyJSON.Unmarshal([]byte(document), &property))
	require.Equal(t, uint64(math.MaxUint64), property["u"])
	require.Equal(t, float64(2), property["whole"])
	// External Cypher input follows Neo4j: an INTEGER outside int64 is rejected.
	var unsupported *UnsupportedNumberError
	require.ErrorAs(t, CypherInputJSON.Unmarshal([]byte(document), &input), &unsupported)
	var whole map[string]interface{}
	require.NoError(t, CypherInputJSON.Unmarshal([]byte(`{"whole":2.0}`), &whole))
	require.Equal(t, float64(2), whole["whole"])
	for _, marshal := range []func(interface{}) ([]byte, error){CypherInputJSON.Marshal, func(value interface{}) ([]byte, error) { return CypherInputJSON.MarshalIndent(value, "", "  ") }} {
		data, err := marshal(map[string]interface{}{"v": float64(2)})
		require.NoError(t, err)
		var decoded map[string]interface{}
		require.NoError(t, CypherInputJSON.Unmarshal(data, &decoded))
		require.Equal(t, float64(2), decoded["v"])
	}
	var out bytes.Buffer
	require.NoError(t, CypherInputJSON.NewEncoder(&out).Encode(map[string]interface{}{"v": float64(2)}))
	require.Contains(t, out.String(), "2.0")
}
