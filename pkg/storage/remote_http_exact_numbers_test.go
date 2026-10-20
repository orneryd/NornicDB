package storage

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoteHTTPExactNumericRoundTrip(t *testing.T) {
	for _, v := range []int64{9007199254740993, 9223372036854775807, -9007199254740993, -9223372036854775808} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			body := fmt.Sprintf(`{"results":[{"columns":["v","nested","n"],"data":[{"row":[%d,{"values":[%d]},{"v":%d}],"meta":[null,null,{"elementId":"n","type":"node"}],"graph":{"nodes":[{"id":"1","elementId":"n","labels":["N"],"properties":{"v":%d}}],"relationships":[]}}]}],"errors":[]}`, v, v, v, v)
			engine := makeEngineWithTransport(t, "http://remote.example", func(req *http.Request) (*http.Response, error) {
				if strings.HasSuffix(req.URL.Path, "/tx") {
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"results":[],"errors":[],"commit":"http://remote.example/db/remote_db/tx/42/commit"}`))}, nil
				}
				payload, err := io.ReadAll(req.Body)
				require.NoError(t, err)
				require.Contains(t, string(payload), fmt.Sprint(v))
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
			})
			for _, route := range []string{"auto_commit", "explicit_transaction"} {
				t.Run(route, func(t *testing.T) {
					query := engine.QueryCypher
					if route == "explicit_transaction" {
						tx, err := engine.BeginCypherTx(context.Background())
						require.NoError(t, err)
						query = tx.QueryCypher
					}
					cols, rows, err := query(context.Background(), "RETURN $v AS v", map[string]interface{}{"v": v})
					require.NoError(t, err)
					require.Equal(t, []string{"v", "nested", "n"}, cols)
					require.Len(t, rows, 1)
					want, ok := numericIndexKey(v)
					require.True(t, ok)
					for _, value := range []interface{}{rows[0][0], rows[0][1].(map[string]interface{})["values"].([]interface{})[0], rows[0][2].(RemoteEntityValue)["properties"].(map[string]interface{})["v"]} {
						key, ok := numericIndexKey(value)
						require.True(t, ok)
						require.Equal(t, want, key, "HTTP changed integer %d into %T(%v)", v, value, value)
					}
				})
			}
		})
	}
}

func TestRemoteHTTPExactNumericPlainValues(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want interface{}
	}{
		{`9007199254740993`, uint64(9007199254740993)},
		{`18446744073709551615`, uint64(math.MaxUint64)},
		{`-9007199254740993`, int64(-9007199254740993)},
		{`-0.0`, float64(0)},
		{`0`, float64(0)},
		{`1.5`, float64(1.5)},
		{`1e2`, float64(100)},
		{`{"v":[9007199254740993,-0.0]}`, map[string]interface{}{"v": []interface{}{uint64(9007199254740993), float64(0)}}},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			index := 0
			got, err := decodeRemoteHTTPValue([]byte(tc.raw), nil, &index, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			if f, ok := got.(float64); ok && f == 0 {
				require.False(t, math.Signbit(f))
			}
		})
	}
	for _, raw := range []string{`1e999`, `{"nested":[1e999]}`, `1 2`, `{`} {
		index := 0
		_, err := decodeRemoteHTTPValue([]byte(raw), nil, &index, nil)
		require.Error(t, err, raw)
	}
}

func TestRemoteHTTPExactNumericRESTEntity(t *testing.T) {
	properties := map[string]interface{}{"v": uint64(9007199254740993)}
	entity := map[string]interface{}{"elementId": "n", "properties": properties}
	index := 0
	value, err := decodeRemoteHTTPValueWithREST([]byte(`{"v":9007199254740993}`), []byte(`{"metadata":{"elementId":"n"},"self":"http://remote/node/1","data":{"v":9007199254740993}}`), []interface{}{map[string]interface{}{"elementId": "n", "type": "node"}}, &index, map[string]interface{}{"n": entity})
	require.NoError(t, err)
	require.Equal(t, RemoteEntityValue(entity), value)
}

func TestPublicExactNumericOwner(t *testing.T) {
	for _, tc := range []struct {
		left, right interface{}
		order       int
	}{
		{int64(9007199254740993), float64(9007199254740992), 1},
		{int64(math.MaxInt64), uint64(math.MaxInt64) + 1, -1},
		{int64(0), math.Copysign(0, -1), 0},
	} {
		order, numeric, ordered := CompareNumericValues(tc.left, tc.right)
		require.True(t, numeric)
		require.True(t, ordered)
		require.Equal(t, tc.order, order)
	}
	_, numeric, ordered := CompareNumericValues(math.NaN(), int64(0))
	require.True(t, numeric)
	require.False(t, ordered)
	_, numeric, _ = CompareNumericValues(nil, "x")
	require.False(t, numeric)
	left, ok := PropertyValueKey([]int64{0, 9007199254740992})
	require.True(t, ok)
	right, ok := PropertyValueKey([]interface{}{math.Copysign(0, -1), float64(9007199254740992)})
	require.True(t, ok)
	require.Equal(t, left, right)
}
