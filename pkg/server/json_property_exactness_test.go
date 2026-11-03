package server

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJSONIngestionHTTPPropertyInputs(t *testing.T) {
	server := &Server{config: &Config{MaxRequestSize: 1 << 20}}
	request := httptest.NewRequest("POST", "/", strings.NewReader(`{"properties":{"v":9007199254740993,"d":1.25,"nested":[-9007199254740993,2.0]}}`))
	var body struct {
		Properties map[string]interface{} `json:"properties"`
	}
	require.NoError(t, server.readJSON(request, &body))
	require.Equal(t, map[string]interface{}{"v": int64(9007199254740993), "d": 1.25, "nested": []interface{}{int64(-9007199254740993), float64(2)}}, body.Properties)
	var transaction TransactionRequest
	require.NoError(t, decodeTransactionRequest(strings.NewReader(`{"statements":[{"statement":"RETURN $v","parameters":{"v":9007199254740993,"d":2.0}}]}`), &transaction, transactionRequestLimit{}))
	require.Equal(t, int64(9007199254740993), transaction.Statements[0].Parameters["v"])
	require.Equal(t, float64(2), transaction.Statements[0].Parameters["d"])
}
