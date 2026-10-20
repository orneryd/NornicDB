package storage

import (
	"context"
	"fmt"
	"io"
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
				payload, err := io.ReadAll(req.Body)
				require.NoError(t, err)
				require.Contains(t, string(payload), fmt.Sprint(v))
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
			})
			cols, rows, err := engine.QueryCypher(context.Background(), "RETURN $v AS v", map[string]interface{}{"v": v})
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
}
