package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher/testutil"
	"github.com/stretchr/testify/require"
)

// The client statement shapes over HTTP: the request canceled a moment after
// it starts, and how long the server's statement then works on (the handler
// returns when it ends: PARSESCALE route=http).
func TestHTTPStatementTextParseScaling(t *testing.T) {
	sizes := os.Getenv("NORNICDB_PARSE_SIZES")
	if sizes == "" {
		t.Skip("NORNICDB_PARSE_SIZES lists the statement sizes to measure")
	}
	server, authenticator := setupTestServer(t)
	authorization := "Bearer " + getAuthToken(t, authenticator, "admin")
	router := server.buildRouter()
	stopped := map[string]bool{}
	for _, field := range strings.Split(sizes, ",") {
		size, err := strconv.Atoi(strings.TrimSpace(field))
		require.NoError(t, err)
		for _, shape := range testutil.StatementShapes(size) {
			if stopped[shape.Name] {
				continue
			}
			body, err := json.Marshal(map[string]interface{}{"statements": []map[string]interface{}{{"statement": shape.Text}}})
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			request := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", bytes.NewReader(body)).WithContext(ctx)
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", authorization)
			recorder := httptest.NewRecorder()
			finished := make(chan struct{})
			go func() {
				router.ServeHTTP(recorder, request)
				close(finished)
			}()
			time.Sleep(20 * time.Millisecond)
			canceledAt := time.Now()
			cancel()
			<-finished
			late := time.Since(canceledAt)
			t.Logf("PARSESCALE shape=%s route=http bytes=%d after_cancel=%v status=%d", shape.Name, len(shape.Text), late, recorder.Code)
			if late > 20*time.Second {
				stopped[shape.Name] = true
			}
		}
	}
}
