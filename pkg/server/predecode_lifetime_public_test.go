package server

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
)

func TestPredecodeReservationLifetime(t *testing.T) {
	for _, consumer := range []string{"transaction", "auth", "search", "graphql-post", "graphql-post-invalid", "graphql-get", "graphql-get-large", "headers", "query-and-body", "chunked-body", "body-limit", "unauthorized", "drop-body", "pressure-during-body", "pressure"} {
		t.Run(consumer, func(t *testing.T) {
			server, client, token := startMemoryHTTPServer(t)
			server.config.MaxRequestSize = 16 << 10
			limit := int64(64 << 20)
			var used atomic.Int64
			used.Store(1)
			if consumer == "graphql-get-large" || consumer == "headers" || consumer == "query-and-body" {
				limit = 128 << 10
			}
			if consumer == "pressure" {
				used.Store(limit)
			}
			policy := memory.NewPolicy(limit, 0, time.Minute, func() (memory.Sample, error) { return memory.Sample{Used: used.Load()}, nil }, time.Now)
			require.NoError(t, policy.Refresh())
			server.memoryPolicy = policy
			if consumer == "drop-body" || consumer == "pressure-during-body" {
				address := strings.TrimPrefix(client.base, "http://")
				conn, err := net.Dial("tcp", address)
				require.NoError(t, err)
				defer conn.Close()
				prefix := `{"statements":[{"statement":"`
				tail := `RETURN 1"}]}`
				_, err = io.WriteString(conn, "POST /db/nornic/tx/commit HTTP/1.1\r\nHost: "+address+"\r\nAuthorization: "+token+"\r\nContent-Type: application/json\r\nContent-Length: "+strconv.Itoa(len(prefix)+len(tail))+"\r\n\r\n"+prefix)
				require.NoError(t, err)
				require.Eventually(t, func() bool { return policy.Reserved() > 0 }, time.Second, time.Millisecond)
				if consumer == "pressure-during-body" {
					used.Store(limit)
					require.NoError(t, policy.Refresh())
					_, err = io.WriteString(conn, tail)
					require.NoError(t, err)
					require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
					response, err := http.ReadResponse(bufio.NewReader(conn), nil)
					require.NoError(t, err)
					payload, err := io.ReadAll(response.Body)
					_ = response.Body.Close()
					require.NoError(t, err)
					require.Equal(t, http.StatusServiceUnavailable, response.StatusCode, string(payload))
					require.Contains(t, string(payload), "MemoryPoolOutOfMemoryError")
				}
				_ = conn.Close()
			} else {
				method, path, body := http.MethodPost, "/db/nornic/tx/commit", "{"
				switch consumer {
				case "auth":
					path = "/auth/token"
				case "search":
					path = "/nornicdb/search"
				case "graphql-post", "headers":
					path, body = "/graphql", `{"query":"{__typename}"}`
				case "graphql-post-invalid":
					path = "/graphql"
				case "query-and-body":
					path += "?bulk=" + strings.Repeat("x", 100<<10)
				case "body-limit":
					body = strings.Repeat(" ", 16<<10) + "{"
				case "graphql-get", "graphql-get-large":
					method, path, body = http.MethodGet, "/graphql?query=%7B__typename%7D&variables=%7Binvalid", ""
					if consumer == "graphql-get-large" {
						query := url.Values{"query": {"{__typename}"}, "variables": {`{"bulk":"` + strings.Repeat("x", 100<<10) + `"}`}}.Encode()
						path = "/graphql?" + query
						lease, err := policy.ReserveWireRequest(int64(len(query)), int64(len(query)))
						lease.Release()
						require.ErrorIs(t, err, memory.ErrLimitExceeded)
					}
				}
				r, err := http.NewRequest(method, client.base+path, strings.NewReader(body))
				require.NoError(t, err)
				r.Header.Set("Authorization", token)
				r.Header.Set("Content-Type", "application/json")
				if consumer == "unauthorized" {
					r.Header.Del("Authorization")
				}
				if consumer == "headers" {
					for i := 0; i < 30; i++ {
						r.Header.Set("X-Payload-"+strings.Repeat("a", i+1), strings.Repeat("x", 3500))
					}
				}
				if consumer == "chunked-body" {
					r.ContentLength = -1
					r.Body = io.NopCloser(bufio.NewReader(strings.NewReader(body)))
				}
				response, err := client.client.Do(r)
				require.NoError(t, err)
				payload, err := io.ReadAll(response.Body)
				_ = response.Body.Close()
				require.NoError(t, err)
				if consumer == "pressure" || consumer == "graphql-get-large" || consumer == "headers" || consumer == "query-and-body" {
					require.Equal(t, http.StatusServiceUnavailable, response.StatusCode, string(payload))
				} else if consumer == "body-limit" {
					require.Equal(t, http.StatusBadRequest, response.StatusCode, string(payload))
				} else if consumer == "transaction" || consumer == "chunked-body" {
					require.Equal(t, http.StatusOK, response.StatusCode)
					require.Contains(t, string(payload), `"errors":[{`)
				} else if consumer != "graphql-post" {
					require.GreaterOrEqual(t, response.StatusCode, 400, string(payload))
				}
			}
			require.Eventually(t, func() bool { return policy.Reserved() == 0 }, time.Second, time.Millisecond,
				"completed/failed/disconnected HTTP request must release its reservation")
		})
	}
}
