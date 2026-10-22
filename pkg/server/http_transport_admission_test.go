package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

func TestHTTPTransportUpgradeBodyAdmission(t *testing.T) {
	const largeBodyBytes = 16 << 20
	smallBody := `{"statements":[{"statement":"RETURN 1 AS n"}]}`
	for _, test := range []struct {
		name    string
		limit   int64
		chunked bool
		upgrade bool
		large   bool
		status  int
	}{
		{"policy-content-length", 4 << 10, false, true, true, http.StatusServiceUnavailable},
		{"policy-chunked", 4 << 10, true, true, true, http.StatusServiceUnavailable},
		{"request-cap-content-length", 256 << 20, false, true, true, http.StatusBadRequest},
		{"request-cap-chunked", 256 << 20, true, true, true, http.StatusBadRequest},
		{"small-upgrade-body", 8 << 20, false, true, false, http.StatusOK},
		// Unknown-length bodies are charged at MaxRequestSize, as for any HTTP/1 request.
		{"small-chunked-upgrade-body", 256 << 20, true, true, false, http.StatusOK},
		{"ordinary-http-body", 8 << 20, false, false, false, http.StatusOK},
		{"policy-disabled-upgrade-body", 0, false, true, false, http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, authenticator := setupTestServer(t)
			server.config.Address = "127.0.0.1"
			var policy *memory.Policy
			if test.limit != 0 {
				policy = memory.NewPolicy(test.limit, 0, time.Minute, func() (memory.Sample, error) { return memory.Sample{Used: 1}, nil }, time.Now)
				require.NoError(t, policy.Refresh())
			}
			server.memoryPolicy = policy
			token := getAuthToken(t, authenticator, "admin")
			startTestHTTPServer(t, server)
			body := smallBody
			if test.large {
				body = strings.Repeat(" ", largeBodyBytes)
				require.EqualValues(t, 10<<20, server.config.MaxRequestSize)
			}
			conn, err := net.Dial("tcp", server.Addr())
			require.NoError(t, err)
			defer conn.Close()
			require.NoError(t, conn.SetDeadline(time.Now().Add(15*time.Second)))
			headers := ""
			if test.upgrade {
				headers = "Connection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: AAIAAAAA\r\n"
			}
			framing := fmt.Sprintf("Content-Length: %d\r\n", len(body))
			if test.chunked {
				framing = "Transfer-Encoding: chunked\r\n"
			}
			_, err = fmt.Fprintf(conn, "POST /db/nornic/tx/commit HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nExpect: 100-continue\r\n%s%s\r\n", server.Addr(), token, headers, framing)
			require.NoError(t, err)
			reader := bufio.NewReader(conn)
			response, err := http.ReadResponse(reader, nil)
			require.NoError(t, err)
			firstStatus := response.StatusCode
			var written int64
			if response.StatusCode == http.StatusContinue {
				_ = response.Body.Close()
				var writer io.Writer = conn
				if test.chunked {
					writer = httputil.NewChunkedWriter(conn)
				}
				written, err = io.Copy(writer, strings.NewReader(body))
				writeErr := err
				if test.chunked {
					_ = writer.(io.Closer).Close()
					_, _ = io.WriteString(conn, "\r\n") // Empty trailer section after the final chunk.
				}
				response, err = http.ReadResponse(reader, nil)
				require.NoError(t, err, "body write: %v", writeErr)
			}
			defer response.Body.Close()
			var data []byte
			if response.StatusCode != http.StatusSwitchingProtocols {
				data, err = io.ReadAll(response.Body)
				require.NoError(t, err)
			}
			t.Logf("first=%d final=%d body-sent=%d", firstStatus, response.StatusCode, written)
			require.Equal(t, test.status, response.StatusCode, string(data))
			if test.limit == 4<<10 {
				require.Equal(t, http.StatusServiceUnavailable, firstStatus, "reject before the adapter requests or buffers body bytes")
				require.Zero(t, written)
				require.Contains(t, string(data), "MemoryPoolOutOfMemoryError")
			} else if test.large {
				// The ordinary request-size response: the decoder stops at MaxBytesReader.
				require.Contains(t, string(data), "Neo.ClientError.Request.InvalidFormat")
			} else {
				require.Contains(t, string(data), `"errors":[]`)
			}
			if policy != nil {
				require.Eventually(t, func() bool { return policy.Reserved() == 0 }, time.Second, time.Millisecond)
			}
		})
	}
}

func TestHTTPTransportH2BodylessControls(t *testing.T) {
	server, authenticator := setupTestServer(t)
	server.config.Address = "127.0.0.1"
	const limit = 8 << 20
	var used atomic.Int64
	used.Store(1)
	policy := memory.NewPolicy(limit, 0, time.Minute, func() (memory.Sample, error) { return memory.Sample{Used: used.Load()}, nil }, time.Now)
	require.NoError(t, policy.Refresh())
	server.memoryPolicy = policy
	token := getAuthToken(t, authenticator, "admin")
	startTestHTTPServer(t, server)
	transport := &http2.Transport{AllowHTTP: true, DialTLSContext: func(ctx context.Context, network, address string, _ *tls.Config) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	for _, operation := range []string{"health", "rollback", "commit"} {
		t.Run(operation, func(t *testing.T) {
			used.Store(1)
			require.NoError(t, policy.Refresh())
			method, path := http.MethodGet, "http://"+server.Addr()+"/health"
			if operation != "health" {
				request, err := http.NewRequest(http.MethodPost, "http://"+server.Addr()+"/db/nornic/tx", strings.NewReader(`{"statements":[{"statement":"CREATE (:TransportControl)"}]}`))
				require.NoError(t, err)
				request.Header.Set("Authorization", "Bearer "+token)
				response, err := client.Do(request)
				require.NoError(t, err)
				data, err := io.ReadAll(response.Body)
				_ = response.Body.Close()
				require.NoError(t, err)
				require.Equal(t, http.StatusCreated, response.StatusCode, string(data))
				var transaction TransactionResponse
				require.NoError(t, json.Unmarshal(data, &transaction))
				require.Empty(t, transaction.Errors)
				path, method = transaction.Commit, http.MethodPost
				if operation == "rollback" {
					path, method = strings.TrimSuffix(path, "/commit"), http.MethodDelete
				}
			}
			used.Store(limit)
			require.NoError(t, policy.Refresh())
			for _, variant := range []string{"query", "body", "bare"} {
				url := path
				var body io.Reader
				if variant == "query" {
					url += "?x=1"
				}
				if variant == "body" {
					body = strings.NewReader("{}")
				}
				request, err := http.NewRequest(method, url, body)
				require.NoError(t, err)
				request.Header.Set("Authorization", "Bearer "+token)
				response, err := client.Do(request)
				require.NoError(t, err)
				data, err := io.ReadAll(response.Body)
				_ = response.Body.Close()
				require.NoError(t, err)
				require.Equal(t, 2, response.ProtoMajor)
				if variant == "bare" {
					require.Equal(t, http.StatusOK, response.StatusCode, string(data))
				} else {
					require.Equal(t, http.StatusServiceUnavailable, response.StatusCode, string(data))
				}
				require.Eventually(t, func() bool { return policy.Reserved() == 0 }, time.Second, time.Millisecond)
			}
		})
	}
}

func TestHTTPTransportProtocolsRetainAdmission(t *testing.T) {
	for _, protocol := range []string{"http1", "h2c-prior-knowledge", "tls-h2", "h2c-bodyless-upgrade"} {
		t.Run(protocol, func(t *testing.T) {
			server, _ := setupTestServer(t)
			server.config.Address = "127.0.0.1"
			if protocol == "tls-h2" {
				fixture := httptest.NewTLSServer(http.NotFoundHandler())
				fixture.Close()
				certificate := fixture.TLS.Certificates[0]
				key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
				require.NoError(t, err)
				server.config.TLSCertFile = filepath.Join(t.TempDir(), "cert.pem")
				server.config.TLSKeyFile = filepath.Join(t.TempDir(), "key.pem")
				require.NoError(t, os.WriteFile(server.config.TLSCertFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]}), 0600))
				require.NoError(t, os.WriteFile(server.config.TLSKeyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600))
			}
			const limit = 8 << 20
			var used atomic.Int64
			used.Store(1)
			policy := memory.NewPolicy(limit, 0, time.Minute, func() (memory.Sample, error) { return memory.Sample{Used: used.Load()}, nil }, time.Now)
			require.NoError(t, policy.Refresh())
			server.memoryPolicy = policy
			startTestHTTPServer(t, server)
			var request func(string) int
			if protocol == "h2c-bodyless-upgrade" {
				conn, err := net.Dial("tcp", server.Addr())
				require.NoError(t, err)
				defer conn.Close()
				require.NoError(t, conn.SetDeadline(time.Now().Add(15*time.Second)))
				_, err = fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: AAIAAAAA\r\n\r\n", server.Addr())
				require.NoError(t, err)
				reader := bufio.NewReader(conn)
				response, err := http.ReadResponse(reader, nil)
				require.NoError(t, err)
				require.Equal(t, http.StatusSwitchingProtocols, response.StatusCode)
				_, err = io.WriteString(conn, http2.ClientPreface)
				require.NoError(t, err)
				framer := http2.NewFramer(conn, reader)
				framer.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
				require.NoError(t, framer.WriteSettings(http2.Setting{ID: http2.SettingEnablePush, Val: 0}))
				require.Equal(t, http.StatusOK, readTransportH2Status(t, framer, 1))
				stream := uint32(1)
				request = func(path string) int {
					stream += 2
					var block bytes.Buffer
					encoder := hpack.NewEncoder(&block)
					for _, field := range []hpack.HeaderField{{Name: ":method", Value: "GET"}, {Name: ":scheme", Value: "http"}, {Name: ":authority", Value: server.Addr()}, {Name: ":path", Value: path}} {
						require.NoError(t, encoder.WriteField(field))
					}
					require.NoError(t, framer.WriteHeaders(http2.HeadersFrameParam{StreamID: stream, BlockFragment: block.Bytes(), EndStream: true, EndHeaders: true}))
					return readTransportH2Status(t, framer, stream)
				}
			} else {
				var transport http.RoundTripper
				scheme, major := "http", 1
				if protocol == "http1" {
					tr := &http.Transport{}
					transport = tr
					t.Cleanup(tr.CloseIdleConnections)
				} else {
					tr := &http2.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} // Test-only certificate.
					transport, major = tr, 2
					if protocol == "h2c-prior-knowledge" {
						tr.AllowHTTP = true
						tr.DialTLSContext = func(ctx context.Context, network, address string, _ *tls.Config) (net.Conn, error) {
							return (&net.Dialer{}).DialContext(ctx, network, address)
						}
					} else {
						scheme = "https"
					}
					t.Cleanup(tr.CloseIdleConnections)
				}
				client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
				request = func(path string) int {
					response, err := client.Get(scheme + "://" + server.Addr() + path)
					require.NoError(t, err)
					_, err = io.Copy(io.Discard, response.Body)
					_ = response.Body.Close()
					require.NoError(t, err)
					require.Equal(t, major, response.ProtoMajor)
					return response.StatusCode
				}
				require.Equal(t, http.StatusOK, request("/"))
			}
			// Credit belongs to a request/stream, never an entire h2 connection.
			require.Eventually(t, func() bool { return policy.Reserved() == 0 }, time.Second, time.Millisecond)
			used.Store(limit)
			require.NoError(t, policy.Refresh())
			require.Equal(t, http.StatusServiceUnavailable, request("/"))
			require.Equal(t, http.StatusOK, request("/health"))
			require.Zero(t, policy.Reserved())
		})
	}
}

func readTransportH2Status(t *testing.T, framer *http2.Framer, stream uint32) int {
	t.Helper()
	status := 0
	for i := 0; i < 32; i++ {
		frame, err := framer.ReadFrame()
		require.NoError(t, err)
		switch frame := frame.(type) {
		case *http2.SettingsFrame:
			if !frame.IsAck() {
				require.NoError(t, framer.WriteSettingsAck())
			}
		case *http2.MetaHeadersFrame:
			if frame.StreamID == stream {
				for _, field := range frame.Fields {
					if field.Name == ":status" {
						status, err = strconv.Atoi(field.Value)
						require.NoError(t, err)
					}
				}
				if frame.StreamEnded() {
					return status
				}
			}
		case *http2.DataFrame:
			if frame.StreamID == stream && frame.StreamEnded() {
				return status
			}
		}
	}
	t.Fatal("HTTP/2 response did not end within 32 frames")
	return 0
}
