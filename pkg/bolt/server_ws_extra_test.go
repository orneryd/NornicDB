package bolt

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// startTLSBoltServer is a TLS-fronted twin of startWSBoltServer. The
// returned cfg.TLSConfig is the server-side config; the corresponding
// client-side *tls.Config (with InsecureSkipVerify) is also returned so
// tests can dial bolt+s:// and wss:// against the same listener.
func startTLSBoltServer(t *testing.T, modify func(*Config)) (port int, clientTLS *tls.Config) {
	t.Helper()
	srvTLS, cliTLS := mustGenSelfSignedTLSConfig(t)

	cfg := DefaultConfig()
	cfg.Port = 0
	cfg.MaxConnections = 10
	cfg.ReadBufferSize = 8192
	cfg.WriteBufferSize = 8192
	cfg.TLSConfig = srvTLS
	if modify != nil {
		modify(cfg)
	}

	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "wstls-test")
	executor := &cypherQueryExecutor{executor: cypher.NewStorageExecutor(store)}
	server := New(cfg, executor)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	requireNoError(t, err)
	server.listener = listener
	requireNoError(t, server.startDiscoveryRefresher())
	port = listener.Addr().(*net.TCPAddr).Port

	errCh := make(chan error, 1)
	go func() { errCh <- server.serve() }()
	t.Cleanup(func() {
		_ = server.Close()
		<-errCh
	})
	return port, cliTLS
}

// runHelloRunPull is a one-shot Bolt smoke that drives the full HELLO +
// RUN(RETURN 1 AS x) + PULL flow against an authenticated session and
// asserts exactly one RECORD comes back. Used by the TLS happy-path tests
// and the mixed-mode test below.
func runHelloRunPull(t *testing.T, conn net.Conn) {
	t.Helper()
	requireNoError(t, PerformHandshakeWithTesting(t, conn))
	requireNoError(t, SendHello(t, conn, nil))
	requireNoError(t, ReadSuccess(t, conn))
	requireNoError(t, SendRun(t, conn, "RETURN 1 AS x", nil, nil))
	requireNoError(t, ReadSuccess(t, conn))
	requireNoError(t, SendPull(t, conn, nil))
	gotRecords := 0
	for {
		mt, data, err := ReadMessage(conn)
		requireNoError(t, err)
		switch mt {
		case MsgRecord:
			gotRecords++
		case MsgSuccess:
			if gotRecords != 1 {
				t.Fatalf("expected 1 RECORD, got %d", gotRecords)
			}
			return
		case MsgFailure:
			t.Fatalf("unexpected FAILURE during PULL: %s", string(data))
		default:
			t.Fatalf("unexpected message 0x%02X during PULL", mt)
		}
	}
}

// W2 — bolt+s:// happy path. tls.Dial straight to the Bolt port, then
// run a regular Bolt session over the encrypted stream. Exercises the
// peekTransport TLS branch + recursion that lands on transportRaw.
func TestWSBolt_TLS_RawHappyPath(t *testing.T) {
	port, cliTLS := startTLSBoltServer(t, nil)
	conn, err := tls.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port), cliTLS)
	requireNoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	runHelloRunPull(t, conn)
}

// W4 — wss:// happy path. tls.Dial first, then upgrade with
// gorilla.Dialer using NetDial that returns the already-wrapped tls.Conn.
// The server sees TLS first byte, recurses, sees "GET ", upgrades to WS.
func TestWSBolt_TLS_WSHappyPath(t *testing.T) {
	port, cliTLS := startTLSBoltServer(t, nil)
	dialer := websocket.Dialer{
		HandshakeTimeout: 5 * time.Second,
		TLSClientConfig:  cliTLS,
	}
	u := url.URL{Scheme: "wss", Host: fmt.Sprintf("127.0.0.1:%d", port), Path: "/"}
	ws, _, err := dialer.Dial(u.String(), nil)
	requireNoError(t, err)
	t.Cleanup(func() { _ = ws.Close() })
	conn := &wsConnAdapter{ws: ws}
	runHelloRunPull(t, conn)
}

// S2 — mixed-mode: bolt:// + bolt+s:// + ws:// + wss:// all completing
// concurrently against the same listener. Each session must see its own
// query result; no cross-talk. The TLS-fronted listener accepts all four.
func TestWSBolt_MixedMode_AllFourTransports(t *testing.T) {
	port, cliTLS := startTLSBoltServer(t, nil)

	dialFns := []struct {
		name string
		dial func() net.Conn
	}{
		{
			name: "bolt://",
			dial: func() net.Conn {
				c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
				requireNoError(t, err)
				return c
			},
		},
		{
			name: "bolt+s://",
			dial: func() net.Conn {
				c, err := tls.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port), cliTLS)
				requireNoError(t, err)
				return c
			},
		},
		{
			name: "ws://",
			dial: func() net.Conn {
				u := url.URL{Scheme: "ws", Host: fmt.Sprintf("127.0.0.1:%d", port), Path: "/"}
				ws, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
				if err != nil {
					// The TLS branch sniffs first byte; ws:// (no TLS)
					// is still served because tls.Server only fires when
					// the byte is 0x16. So the listener accepts plain ws://
					// alongside wss://. This dial should succeed.
					t.Fatalf("ws:// dial failed against TLS-enabled listener: %v", err)
				}
				return &wsConnAdapter{ws: ws}
			},
		},
		{
			name: "wss://",
			dial: func() net.Conn {
				dialer := websocket.Dialer{TLSClientConfig: cliTLS}
				u := url.URL{Scheme: "wss", Host: fmt.Sprintf("127.0.0.1:%d", port), Path: "/"}
				ws, _, err := dialer.Dial(u.String(), nil)
				requireNoError(t, err)
				return &wsConnAdapter{ws: ws}
			},
		},
	}

	var wg sync.WaitGroup
	var errs sync.Map
	for _, df := range dialFns {
		wg.Add(1)
		go func(df struct {
			name string
			dial func() net.Conn
		}) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errs.Store(df.name, fmt.Errorf("panic: %v", r))
				}
			}()
			conn := df.dial()
			defer conn.Close()
			runHelloRunPull(t, conn)
		}(df)
	}
	wg.Wait()

	errs.Range(func(k, v any) bool {
		t.Errorf("transport %v failed: %v", k, v)
		return true
	})
}

// S7 — reconnect: a client opens, runs a query, closes; opens again on
// the same listener. Both sessions must complete cleanly. Asserts that
// session-state cleanup doesn't leave server.activeConnections leaking.
func TestWSBolt_Reconnect(t *testing.T) {
	srv, port := startWSBoltServer(t, DefaultConfig())

	for i := 0; i < 3; i++ {
		ws := dialWS(t, port)
		conn := &wsConnAdapter{ws: ws}
		runHelloRunPull(t, conn)
		_ = ws.Close()
	}

	// Give server time to drain. activeConnections must return to zero;
	// any residual count means handleConnection's defer chain leaked.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if srv.activeConnections.Load() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("activeConnections did not drain to 0 after reconnects: %d",
		srv.activeConnections.Load())
}

// S8 — handleRoute advertises correct address. A WS client issues ROUTE;
// the server's response should carry the listener's TCP host:port from
// the synthetic *net.TCPAddr on wsConn, NOT a hard-coded localhost:7687
// (which would happen if the type assertion at handleRoute fell through).
func TestWSBolt_RouteAdvertisesListenerAddress(t *testing.T) {
	srv, port := startWSBoltServer(t, DefaultConfig())
	expectedAddr := fmt.Sprintf("%s:%d", "127.0.0.1", port)

	// Use the listener's actual port. handleRoute advertises whatever
	// wsConn.LocalAddr().(*net.TCPAddr) returns, mapped to host:port.
	_ = srv

	ws := dialWS(t, port)
	conn := &wsConnAdapter{ws: ws}
	requireNoError(t, PerformHandshakeWithTesting(t, conn))
	requireNoError(t, SendHello(t, conn, nil))
	requireNoError(t, ReadSuccess(t, conn))

	// Bolt 4.3+ ROUTE: struct(3) + signature 0x66 + (routing_context,
	// bookmarks, db). SendMessage adds the chunk framing; the bytes
	// here are just the unframed PackStream structure.
	routeMsg := []byte{
		0xB3, MsgRoute, // struct(3) + ROUTE signature
		0xA0, // routing_context: empty map
		0x90, // bookmarks: empty list
		0xC0, // db: null
	}
	requireNoError(t, SendMessage(conn, routeMsg))

	mt, data, err := ReadMessage(conn)
	requireNoError(t, err)
	if mt != MsgSuccess {
		t.Fatalf("ROUTE got 0x%02X (expected SUCCESS); body=%q", mt, string(data))
	}
	// data is the Bolt-encoded SUCCESS metadata containing rt.servers.
	// Parsing PackStream by hand here would be brittle; assert the
	// expected host:port substring appears in the raw bytes (the
	// addresses field is stringified inside the PackStream payload).
	if !strings.Contains(string(data), expectedAddr) {
		t.Fatalf("ROUTE response missing %q; body=%q", expectedAddr, string(data))
	}
}

// S9 — oversized message metric. Set MaxMessageSize tiny; client sends
// a huge BinaryMessage; server closes the WS with 1009 and increments
// websocket_oversized_total.
func TestWSBolt_OversizedMessage_IncrementsMetric(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WebSocketMaxMessageSize = 64 // tiny limit
	srv, port := startWSBoltServer(t, cfg)

	// Wire metrics so the bag exists and the counter can be observed.
	reg := prometheus.NewRegistry()
	bag := observability.NewBoltMetrics(reg)
	srv.SetBoltMetrics(bag)

	ws := dialWS(t, port)
	t.Cleanup(func() { _ = ws.Close() })

	// Send a huge BinaryMessage well past the 64-byte cap.
	huge := make([]byte, 1024)
	for i := range huge {
		huge[i] = byte(i)
	}
	if err := ws.WriteMessage(websocket.BinaryMessage, huge); err != nil {
		// Some platforms surface the close synchronously; that's fine.
		t.Logf("WriteMessage returned (may be normal): %v", err)
	}

	// The server closes us with 1009. Read the close frame; gorilla
	// surfaces it as an error from NextReader.
	_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		_, _, err := ws.NextReader()
		if err != nil {
			break
		}
	}

	// Poll the counter for up to 1s. wsConn.Read fires onOversize on
	// the read-side detection of "read limit exceeded".
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := counterValue(t, reg, "nornicdb_bolt_websocket_oversized_total"); got >= 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("websocket_oversized_total did not reach 1 within 2s")
}

// S-AuthorizationHeaderHonored — third-party clients carrying a JWT in
// `Authorization: Bearer …` (no cookie) reach Bolt-over-WS via the
// implicit-bearer path. HELLO scheme=none promotes the session.
func TestWSBolt_AuthorizationHeaderHonored(t *testing.T) {
	port, adminToken, _ := startAuthEnabledWSBoltServer(t)
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	u := url.URL{Scheme: "ws", Host: fmt.Sprintf("127.0.0.1:%d", port), Path: "/"}
	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+adminToken)
	ws, _, err := dialer.Dial(u.String(), hdr)
	requireNoError(t, err)
	t.Cleanup(func() { _ = ws.Close() })

	conn := &wsConnAdapter{ws: ws}
	requireNoError(t, PerformHandshakeWithTesting(t, conn))
	requireNoError(t, SendHello(t, conn, nil)) // scheme=none, header is the credential
	requireNoError(t, ReadSuccess(t, conn))

	// Real round-trip proves the session has full admin DB access.
	requireNoError(t, SendRun(t, conn, "RETURN 1 AS x", nil, nil))
	requireNoError(t, ReadSuccess(t, conn))
	requireNoError(t, SendPull(t, conn, nil))
	gotRecords := 0
	for {
		mt, data, err := ReadMessage(conn)
		requireNoError(t, err)
		switch mt {
		case MsgRecord:
			gotRecords++
		case MsgSuccess:
			if gotRecords != 1 {
				t.Fatalf("expected 1 RECORD, got %d", gotRecords)
			}
			return
		case MsgFailure:
			t.Fatalf("PULL failed: %s", string(data))
		default:
			t.Fatalf("unexpected msg 0x%02X", mt)
		}
	}
}

// S-CookieWinsOverHeader — when both a cookie and an Authorization
// header are present, the cookie wins (it's first-party-set).
//
// The test gives the cookie's user (alice) admin and the header's
// user (bob) viewer-only, then wires a DatabaseAccessModeResolver that
// allows admin and denies viewer. If the cookie path wins, the
// follow-up RUN/PULL completes; if the header path were chosen
// instead, the per-DB access check would surface
// Neo.ClientError.Security.Forbidden mid-stream. Both auth attempts
// validate successfully on their own, so the role check is the only
// observable distinguisher.
func TestWSBolt_CookieWinsOverHeader(t *testing.T) {
	authCfg := auth.DefaultAuthConfig()
	authCfg.JWTSecret = []byte("test-secret-key-for-jwt-signing!!")
	authCfg.SecurityEnabled = true
	mem := storage.NewMemoryEngine()
	authenticator, err := auth.NewAuthenticator(authCfg, mem)
	requireNoError(t, err)
	_, err = authenticator.CreateUser("alice", "alice-password", []auth.Role{auth.RoleAdmin})
	requireNoError(t, err)
	_, err = authenticator.CreateUser("bob", "bob-password", []auth.Role{auth.RoleViewer})
	requireNoError(t, err)
	aliceResp, _, err := authenticator.Authenticate("alice", "alice-password", "127.0.0.1", "test")
	requireNoError(t, err)
	bobResp, _, err := authenticator.Authenticate("bob", "bob-password", "127.0.0.1", "test")
	requireNoError(t, err)

	cfg := DefaultConfig()
	cfg.RequireAuth = true
	cfg.Authenticator = NewAuthenticatorAdapter(authenticator)
	srv, port := startWSBoltServer(t, cfg)
	// Admin reaches every database; viewer is denied. The asymmetry is
	// what makes the cookie-vs-header outcome observable downstream.
	srv.SetDatabaseAccessModeResolver(func(roles []string) auth.DatabaseAccessMode {
		for _, r := range roles {
			if r == string(auth.RoleAdmin) {
				return auth.FullDatabaseAccessMode
			}
		}
		return auth.DenyAllDatabaseAccessMode
	})

	// Cookie = alice (admin); Authorization = bob (viewer). Cookie must win.
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	u := url.URL{Scheme: "ws", Host: fmt.Sprintf("127.0.0.1:%d", port), Path: "/"}
	hdr := http.Header{}
	hdr.Set("Cookie", "nornicdb_token="+aliceResp.AccessToken)
	hdr.Set("Authorization", "Bearer "+bobResp.AccessToken)
	ws, _, err := dialer.Dial(u.String(), hdr)
	requireNoError(t, err)
	t.Cleanup(func() { _ = ws.Close() })

	conn := &wsConnAdapter{ws: ws}
	requireNoError(t, PerformHandshakeWithTesting(t, conn))
	requireNoError(t, SendHello(t, conn, nil))
	requireNoError(t, ReadSuccess(t, conn))
	requireNoError(t, SendRun(t, conn, "RETURN 1 AS x", nil, nil))
	if mt, data, err := ReadMessage(conn); err != nil {
		t.Fatalf("read after RUN: %v", err)
	} else if mt == MsgFailure {
		// If the header path had won, this is exactly where the test
		// would diverge: bob's viewer role would trigger
		// Neo.ClientError.Security.Forbidden on the access check.
		t.Fatalf("RUN failed (header path likely won): %s", string(data))
	} else if mt != MsgSuccess {
		t.Fatalf("RUN got 0x%02X (expected SUCCESS)", mt)
	}
	requireNoError(t, SendPull(t, conn, nil))
	gotRecords := 0
	for {
		mt, data, err := ReadMessage(conn)
		requireNoError(t, err)
		switch mt {
		case MsgRecord:
			gotRecords++
		case MsgSuccess:
			if gotRecords != 1 {
				t.Fatalf("expected 1 RECORD, got %d", gotRecords)
			}
			return
		case MsgFailure:
			t.Fatalf("PULL failed: %s", string(data))
		default:
			t.Fatalf("unexpected msg 0x%02X", mt)
		}
	}
}

// S-StrayHeaderIgnoredUnderHELLO — when the HELLO carries scheme=bearer
// with a valid token, the server uses it and never consults the
// Authorization header (whose value here is invalid).
func TestWSBolt_StrayHeaderIgnoredUnderHELLOBearer(t *testing.T) {
	port, adminToken, badToken := startAuthEnabledWSBoltServer(t)
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	u := url.URL{Scheme: "ws", Host: fmt.Sprintf("127.0.0.1:%d", port), Path: "/"}
	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+badToken)
	ws, _, err := dialer.Dial(u.String(), hdr)
	requireNoError(t, err)
	t.Cleanup(func() { _ = ws.Close() })

	conn := &wsConnAdapter{ws: ws}
	requireNoError(t, PerformHandshakeWithTesting(t, conn))
	requireNoError(t, SendHello(t, conn, map[string]string{
		"scheme":      "bearer",
		"credentials": adminToken,
	}))
	mt, _, err := ReadMessage(conn)
	requireNoError(t, err)
	if mt != MsgSuccess {
		t.Fatalf("HELLO bearer should win over invalid Authorization header (got 0x%02X)", mt)
	}
}

// S-OnlyAuthorizationAndCookieHonored — X-API-Key, ?token=, and any
// other HTTP-API credential surfaces are NOT honored on WS upgrades.
func TestWSBolt_OtherHTTPAuthSurfacesIgnored(t *testing.T) {
	port, adminToken, _ := startAuthEnabledWSBoltServer(t)
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	u := url.URL{
		Scheme:   "ws",
		Host:     fmt.Sprintf("127.0.0.1:%d", port),
		Path:     "/",
		RawQuery: "token=" + adminToken, // attempt URL-query auth (must be ignored)
	}
	hdr := http.Header{}
	hdr.Set("X-API-Key", adminToken) // attempt API-key auth (must be ignored)
	ws, _, err := dialer.Dial(u.String(), hdr)
	requireNoError(t, err)
	t.Cleanup(func() { _ = ws.Close() })

	conn := &wsConnAdapter{ws: ws}
	requireNoError(t, PerformHandshakeWithTesting(t, conn))
	requireNoError(t, SendHello(t, conn, nil)) // scheme=none, no cookie, no Authorization
	mt, _, err := ReadMessage(conn)
	requireNoError(t, err)
	if mt != MsgFailure {
		t.Fatalf("X-API-Key / URL-query token must NOT grant access (got 0x%02X)", mt)
	}
}

// Path-not-/ returns 404. A WS upgrade aimed at /bolt or /api or anything
// other than / should be rejected with HTTP 404 (driver misconfiguration,
// not a probe).
func TestWSBolt_PathNotRoot_Returns404(t *testing.T) {
	_, port := startWSBoltServer(t, DefaultConfig())

	// Speak HTTP directly so we can issue a non-/ Upgrade request.
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	requireNoError(t, err)
	defer conn.Close()
	_, _ = io.WriteString(conn,
		"GET /bolt HTTP/1.1\r\nHost: localhost\r\n"+
			"Upgrade: websocket\r\nConnection: upgrade\r\n"+
			"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGVzdA==\r\n\r\n")

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	requireNoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for non-root WS path, got %d", resp.StatusCode)
	}
}

// counterValue gathers reg, finds the named single-series counter (no
// labels), and returns its value. Returns 0 if the counter family
// hasn't been touched yet (some metric backends omit zero-value
// families from Gather). Test helper only.
func counterValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		if len(mf.Metric) == 0 {
			return 0
		}
		if mf.GetType() != dto.MetricType_COUNTER {
			t.Fatalf("metric %q is not a counter", name)
		}
		var total float64
		for _, m := range mf.Metric {
			total += m.GetCounter().GetValue()
		}
		return total
	}
	return 0
}

// _ ensures the atomic import is referenced even if a future refactor
// removes the only direct usage. The activeConnections.Load call in
// TestWSBolt_Reconnect is the current site.
var _ = atomic.Int64{}
