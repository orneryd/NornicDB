package load

import (
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNumericDomainResultReaders(t *testing.T) {
	const document = `{"u":18446744073709551615,"i":9007199254740993,"f":2.0,"nested":[9223372036854775808,5e-324]}`
	host := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/graphql" {
			_, _ = writer.Write([]byte(`{"data":` + document + `}`))
		} else {
			_, _ = writer.Write([]byte(document))
		}
	}))
	defer host.Close()
	for name, read := range map[string]func() (interface{}, error){
		"graphql": func() (interface{}, error) {
			return GraphQL(host.URL+"/graphql", "{ properties }", map[string]interface{}{"v": float64(2)})
		},
		"rest": func() (interface{}, error) { return Rest(host.URL, nil) },
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
	_, err := Json(host.URL)
	require.ErrorContains(t, err, "Unsupported type of Number")
}
