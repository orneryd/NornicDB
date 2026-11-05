package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
)

func TestHTTPRequestMetadataAdmissionPrecedesDecoding(t *testing.T) {
	server, client, token := startMemoryHTTPServer(t)
	policy := memory.NewPolicy(128<<10, 0, time.Minute, func() (memory.Sample, error) { return memory.Sample{Used: 1}, nil }, time.Now)
	require.NoError(t, policy.Refresh())
	server.memoryPolicy = policy
	for _, endpoint := range []struct{ method, path string }{
		{http.MethodGet, "/graphql"},
		{http.MethodPost, "/graphql"},
		{http.MethodGet, "/auth/config"},
		{http.MethodPost, "/db/nornic/tx/commit"},
		{http.MethodGet, "/health"},
	} {
		t.Run(endpoint.method+endpoint.path, func(t *testing.T) {
			// Security would return 401 only after URL.Query has unescaped the
			// whole payload. Admission must reject before that decoder runs.
			query := "?token=%3C&variables=" + strings.Repeat("%78", 64<<10)
			request, err := http.NewRequest(endpoint.method, client.base+endpoint.path+query, nil)
			require.NoError(t, err)
			request.Header.Set("Authorization", token)
			response, err := client.client.Do(request)
			require.NoError(t, err)
			body, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			require.NoError(t, err)
			require.Equal(t, http.StatusServiceUnavailable, response.StatusCode, string(body))
			require.Contains(t, string(body), "MemoryPoolOutOfMemoryError")
			require.Zero(t, policy.Reserved())
		})
	}
}

func TestHTTPRequestMetadataRejectsBeforeQueryAllocation(t *testing.T) {
	server, _, _ := startMemoryHTTPServer(t)
	policy := memory.NewPolicy(128<<10, 0, time.Minute, func() (memory.Sample, error) { return memory.Sample{Used: 1}, nil }, time.Now)
	require.NoError(t, policy.Refresh())
	server.memoryPolicy = policy
	router := server.buildRouter()
	request := httptest.NewRequest(http.MethodGet, "/graphql?token=%3C"+strings.Repeat("&x=%78", 8192), nil)
	var response *httptest.ResponseRecorder
	allocations := testing.AllocsPerRun(3, func() {
		response = httptest.NewRecorder()
		router.ServeHTTP(response, request)
	})
	t.Logf("rejected request allocations: %.0f", allocations)
	require.Less(t, allocations, float64(256), "admission must not unescape 8192 query values before rejecting")
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Zero(t, policy.Reserved())
}

func TestHTTPRequestMetadataControlAvailability(t *testing.T) {
	for _, action := range []string{"health", "rollback", "commit"} {
		t.Run(action, func(t *testing.T) {
			server, client, token := startMemoryHTTPServer(t)
			var used atomic.Int64
			used.Store(1)
			policy := memory.NewPolicy(1<<20, 0, time.Minute, func() (memory.Sample, error) { return memory.Sample{Used: used.Load()}, nil }, time.Now)
			require.NoError(t, policy.Refresh())
			server.memoryPolicy = policy
			method, path := http.MethodGet, client.base+"/health"
			if action != "health" {
				status, transaction, _ := client.post("/db/nornic/tx", "CREATE (:MetadataControl)")
				require.Equal(t, http.StatusCreated, status)
				require.Empty(t, transaction.Errors)
				path, method = transaction.Commit, http.MethodPost
				if action == "rollback" {
					path, method = strings.TrimSuffix(path, "/commit"), http.MethodDelete
				}
			}
			used.Store(1 << 20)
			require.NoError(t, policy.Refresh())
			for _, variant := range []string{"query", "body", "bare"} {
				var body io.Reader
				url := path
				if variant == "query" {
					url += "?x=1"
				}
				if variant == "body" {
					body = strings.NewReader("{}")
				}
				request, err := http.NewRequest(method, url, body)
				require.NoError(t, err)
				request.Header.Set("Authorization", token)
				response, err := client.client.Do(request)
				require.NoError(t, err)
				data, err := io.ReadAll(response.Body)
				_ = response.Body.Close()
				require.NoError(t, err)
				if variant == "bare" {
					require.Equal(t, http.StatusOK, response.StatusCode, string(data))
					if action != "health" {
						var transaction TransactionResponse
						require.NoError(t, json.Unmarshal(data, &transaction))
						require.Empty(t, transaction.Errors)
					}
				} else {
					require.Equal(t, http.StatusServiceUnavailable, response.StatusCode, string(data))
					require.Contains(t, string(data), "MemoryPoolOutOfMemoryError")
				}
				require.Zero(t, policy.Reserved())
			}
			if action != "health" {
				used.Store(1)
				require.NoError(t, policy.Refresh())
				txPath := strings.TrimPrefix(strings.TrimSuffix(path, "/commit"), client.base)
				status, _, _ := client.post(txPath)
				require.Equal(t, http.StatusNotFound, status, "control operation must close its transaction")
			}
		})
	}
}

func TestHTTPRequestMetadataSecurityRejections(t *testing.T) {
	server, client, _ := startMemoryHTTPServer(t)
	policy := memory.NewPolicy(8<<20, 0, time.Minute, func() (memory.Sample, error) { return memory.Sample{Used: 1}, nil }, time.Now)
	require.NoError(t, policy.Refresh())
	server.memoryPolicy = policy
	for _, rejection := range []struct {
		query  string
		status int
	}{
		{"token=%3C", http.StatusUnauthorized},
		{"callback=javascript%3Aalert%281%29", http.StatusBadRequest},
		{"redirect=javascript%3Aalert%281%29", http.StatusBadRequest},
		{"redirect_uri=javascript%3Aalert%281%29", http.StatusBadRequest},
		{"url=javascript%3Aalert%281%29", http.StatusBadRequest},
		{"webhook=javascript%3Aalert%281%29", http.StatusBadRequest},
	} {
		t.Run(rejection.query, func(t *testing.T) {
			response, err := client.client.Get(client.base + "/graphql?" + rejection.query)
			require.NoError(t, err)
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			require.Equal(t, rejection.status, response.StatusCode)
			require.Zero(t, policy.Reserved())
		})
	}
}

func TestHTTPRequestMetadataTimeoutRetainsWorker(t *testing.T) {
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "15ms")
	server, _ := setupTestServer(t)
	policy := memory.NewPolicy(1<<20, 0, time.Minute, func() (memory.Sample, error) { return memory.Sample{Used: 1}, nil }, time.Now)
	require.NoError(t, policy.Refresh())
	server.memoryPolicy = policy
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	defer unblock()
	finished := make(chan error, 1)
	handler := server.wrapWithMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-resume
		_, err := io.ReadAll(r.Body)
		finished <- err
	}))
	request := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", strings.NewReader("{}"))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Positive(t, policy.Reserved(), "timeout response must retain the live worker's credit")
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("timeout worker never started")
	}
	unblock()
	select {
	case err := <-finished:
		require.NoError(t, err, "body reads must still use the retained owner after timeout")
	case <-time.After(time.Second):
		t.Fatal("worker did not return")
	}
	require.Eventually(t, func() bool { return policy.Reserved() == 0 }, time.Second, time.Millisecond)
}
