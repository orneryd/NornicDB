package load

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJSONIngestionLibraryLoad(t *testing.T) {
	const document = `{"v":9007199254740993,"nested":[-9007199254740993,2.0]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/graphql" {
			_, _ = w.Write([]byte(`{"data":` + document + `}`))
			return
		}
		_, _ = w.Write([]byte(document))
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "numbers.json")
	require.NoError(t, os.WriteFile(path, []byte(document), 0600))
	for _, source := range []string{path, server.URL} {
		value, err := Json(source)
		require.NoError(t, err)
		require.Equal(t, map[string]interface{}{"v": int64(9007199254740993), "nested": []interface{}{int64(-9007199254740993), float64(2)}}, value)
	}
	for _, load := range []func() (interface{}, error){
		func() (interface{}, error) { return JsonParams(server.URL, nil, "GET") },
		func() (interface{}, error) { return GraphQL(server.URL+"/graphql", "{ values }", nil) },
		func() (interface{}, error) { return Rest(server.URL, nil) },
	} {
		value, err := load()
		require.NoError(t, err)
		require.Equal(t, map[string]interface{}{"v": int64(9007199254740993), "nested": []interface{}{int64(-9007199254740993), float64(2)}}, value)
	}
}
