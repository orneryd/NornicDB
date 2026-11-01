package load

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/apocio"
	"github.com/stretchr/testify/require"
)

// numberDocumentContext serves document for any https://example.com path, and
// wraps it in {"data": ...} under /graphql, through the APOC capability.
func numberDocumentContext(document string) context.Context {
	policy := apocio.NewPolicy(apocio.Config{
		AllowImportFileAccess: true,
		AllowRemoteURLAccess:  true,
		RemoteURLAllowlist:    []string{"example.com"},
		HostResolver:          publicResolver,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			body := document
			if req.URL.Path == "/graphql" {
				body = `{"data":` + document + `}`
			}
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header), Request: req}, nil
		})},
	})
	return apocio.WithCapability(context.Background(), policy.Capability())
}

func TestJSONIngestionLibraryLoad(t *testing.T) {
	const document = `{"v":9007199254740993,"nested":[-9007199254740993,2.0]}`
	ctx := numberDocumentContext(document)
	path := jsonItemsFile(t, document)
	for _, source := range []string{path, "https://example.com/data.json"} {
		value, err := Json(ctx, source)
		require.NoError(t, err)
		require.Equal(t, map[string]interface{}{"v": int64(9007199254740993), "nested": []interface{}{int64(-9007199254740993), float64(2)}}, value)
	}
	for _, load := range []func() (interface{}, error){
		func() (interface{}, error) { return JsonParams(ctx, "https://example.com/data.json", nil, "GET") },
		func() (interface{}, error) { return GraphQL(ctx, "https://example.com/graphql", "{ values }", nil) },
		func() (interface{}, error) { return Rest(ctx, "https://example.com/data.json", nil) },
	} {
		value, err := load()
		require.NoError(t, err)
		require.Equal(t, map[string]interface{}{"v": int64(9007199254740993), "nested": []interface{}{int64(-9007199254740993), float64(2)}}, value)
	}
}
