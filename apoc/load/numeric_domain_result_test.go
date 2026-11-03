package load

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNumericDomainResultReaders(t *testing.T) {
	const document = `{"u":18446744073709551615,"i":9007199254740993,"f":2.0,"nested":[9223372036854775808,5e-324]}`
	ctx := numberDocumentContext(document)
	for name, read := range map[string]func() (interface{}, error){
		"graphql": func() (interface{}, error) {
			return GraphQL(ctx, "https://example.com/graphql", "{ properties }", map[string]interface{}{"v": float64(2)})
		},
		"rest":            func() (interface{}, error) { return Rest(ctx, "https://example.com/data.json", nil) },
		"import document": func() (interface{}, error) { return PropertyDocument(ctx, "https://example.com/data.json") },
	} {
		t.Run(name, func(t *testing.T) {
			value, err := read()
			require.NoError(t, err)
			require.Equal(t, map[string]interface{}{"u": uint64(math.MaxUint64), "i": int64(9007199254740993), "f": float64(2), "nested": []interface{}{uint64(math.MaxInt64) + 1, math.SmallestNonzeroFloat64}}, value)
		})
	}
	// Raw load.json is an explicit external Cypher conversion: it follows the
	// one external-number policy (Neo4j 5.26 rejects an INTEGER outside the
	// signed 64-bit range, reference probe 1c), like apoc.convert.fromJson*.
	// A result/document reader has a different path.
	_, err := Json(ctx, "https://example.com/data.json")
	require.ErrorContains(t, err, "Unsupported type of Number")
}
