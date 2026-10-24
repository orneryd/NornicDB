package storage

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A remote transaction opened for reading asks the remote for a read
// transaction: over HTTP the access-mode READ header, which a write
// transaction doesn't send; over Bolt a read session.
func TestRemoteEngineReadCypherTxAsksForReadAccess(t *testing.T) {
	var modes []string
	engine := makeEngineWithTransport(t, "http://remote.example", func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/db/remote_db/tx") {
			modes = append(modes, req.Header.Get("access-mode"))
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(
			`{"results":[],"errors":[],"commit":"http://remote.example/db/remote_db/tx/1/commit"}`))}, nil
	})
	_, err := engine.BeginReadCypherTx(context.Background())
	require.NoError(t, err)
	_, err = engine.BeginCypherTx(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"READ", ""}, modes)

	bolt := newUnreachableBoltTransport(t)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = bolt.beginCypherTx(ctx, false)
	require.Error(t, err, "a read session on an unreachable server")
}
