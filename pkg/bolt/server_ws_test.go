package bolt

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// startWSBoltServer starts a Bolt server bound to a random port. Returns
// the server and listening port. Cleanup closes the server.
func startWSBoltServer(t *testing.T, cfg *Config) (*Server, int) {
	t.Helper()
	if cfg == nil {
		cfg = DefaultConfig()
	}
	cfg.Port = 0
	if cfg.MaxConnections == 0 {
		cfg.MaxConnections = 10
	}
	if cfg.ReadBufferSize == 0 {
		cfg.ReadBufferSize = 8192
	}
	if cfg.WriteBufferSize == 0 {
		cfg.WriteBufferSize = 8192
	}

	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "wstest")
	executor := &cypherQueryExecutor{executor: cypher.NewStorageExecutor(store)}
	server := New(cfg, executor)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	requireNoError(t, err)
	server.listener = listener
	if err := server.startDiscoveryRefresher(); err != nil {
		t.Fatalf("startDiscoveryRefresher: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port

	errCh := make(chan error, 1)
	go func() { errCh <- server.serve() }()
	t.Cleanup(func() {
		_ = server.Close()
		<-errCh
	})

	return server, port
}

// dialWS opens a WebSocket client connection against the Bolt port and
// returns it.
func dialWS(t *testing.T, port int) *websocket.Conn {
	t.Helper()
	u := url.URL{Scheme: "ws", Host: fmt.Sprintf("127.0.0.1:%d", port), Path: "/"}
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	ws, _, err := dialer.Dial(u.String(), nil)
	requireNoError(t, err)
	t.Cleanup(func() { _ = ws.Close() })
	return ws
}

// wsConnAdapter wraps a *websocket.Conn so it can be used as net.Conn by
// the existing testutil helpers (SendHello, ReadSuccess, etc).
type wsConnAdapter struct {
	ws            *websocket.Conn
	currentReader io.Reader
}

func (a *wsConnAdapter) Read(p []byte) (int, error) {
	for {
		if a.currentReader == nil {
			mt, r, err := a.ws.NextReader()
			if err != nil {
				return 0, err
			}
			if mt != websocket.BinaryMessage {
				continue
			}
			a.currentReader = r
		}
		n, err := a.currentReader.Read(p)
		if err == io.EOF {
			a.currentReader = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}
func (a *wsConnAdapter) Write(p []byte) (int, error) {
	if err := a.ws.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}
func (a *wsConnAdapter) Close() error                       { return a.ws.Close() }
func (a *wsConnAdapter) LocalAddr() net.Addr                { return a.ws.LocalAddr() }
func (a *wsConnAdapter) RemoteAddr() net.Addr               { return a.ws.RemoteAddr() }
func (a *wsConnAdapter) SetDeadline(t time.Time) error      { return a.ws.SetReadDeadline(t) }
func (a *wsConnAdapter) SetReadDeadline(t time.Time) error  { return a.ws.SetReadDeadline(t) }
func (a *wsConnAdapter) SetWriteDeadline(t time.Time) error { return a.ws.SetWriteDeadline(t) }

// TestWSBolt_HappyPath asserts the W3 wire-compatibility scenario from
// the plan: bolt://host:port/ → upgrade succeeds → HELLO/RUN/PULL/BYE
// completes. Buffering inside the bolt server folds 4 RECORD messages
// plus surrounding SUCCESS into a single binary frame, so the
// wsConnAdapter.Read path must coalesce frames correctly.
func TestWSBolt_HappyPath(t *testing.T) {
	_, port := startWSBoltServer(t, DefaultConfig())

	ws := dialWS(t, port)
	conn := &wsConnAdapter{ws: ws}

	requireNoError(t, PerformHandshakeWithTesting(t, conn))
	requireNoError(t, SendHello(t, conn, nil))
	requireNoError(t, ReadSuccess(t, conn))

	requireNoError(t, SendRun(t, conn, "RETURN 1 AS x", nil, nil))
	requireNoError(t, ReadSuccess(t, conn))
	requireNoError(t, SendPull(t, conn, nil))

	// Drain RECORD + SUCCESS
	saw := 0
	for {
		mt, _, err := ReadMessage(conn)
		requireNoError(t, err)
		if mt == MsgRecord {
			saw++
			continue
		}
		if mt == MsgSuccess {
			break
		}
		t.Fatalf("unexpected msg type 0x%02X", mt)
	}
	if saw == 0 {
		t.Fatal("expected at least one RECORD message")
	}
}

// TestWSBolt_DiscoveryProbe asserts a plain GET / (no Upgrade headers)
// returns the discovery response. Mirrors test D1 from the plan.
func TestWSBolt_DiscoveryProbe(t *testing.T) {
	_, port := startWSBoltServer(t, DefaultConfig())
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	requireNoError(t, err)
	defer conn.Close()

	_, _ = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	requireNoError(t, err)
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	for _, want := range []string{"Content-Type", "Access-Control-Allow-Origin", "Vary", "Date"} {
		if resp.Header.Get(want) == "" {
			t.Fatalf("missing %s header", want)
		}
	}
	if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("expected Access-Control-Allow-Origin: *, got %q", resp.Header.Get("Access-Control-Allow-Origin"))
	}
}

// TestWSBolt_DisabledReturns426 asserts that with WebSocketEnabled=false,
// a real WS upgrade attempt receives 426 Upgrade Required (test
// S-WSDisabled-426 in the plan).
func TestWSBolt_DisabledReturns426(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WebSocketEnabled = false
	_, port := startWSBoltServer(t, cfg)

	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	u := url.URL{Scheme: "ws", Host: fmt.Sprintf("127.0.0.1:%d", port), Path: "/"}
	_, resp, err := dialer.Dial(u.String(), nil)
	if err == nil {
		t.Fatal("expected dial to fail when WebSocketEnabled=false")
	}
	if resp == nil {
		t.Fatalf("expected HTTP response with 426 status; got dial error %v", err)
	}
	if resp.StatusCode != http.StatusUpgradeRequired {
		t.Fatalf("expected 426, got %d", resp.StatusCode)
	}
}

// TestWSBolt_DisabledStillServesDiscovery asserts that a plain GET /
// still gets the 200 discovery body even when WebSocketEnabled=false
// (S-WSDisabled-DiscoveryStillWorks in the plan).
func TestWSBolt_DisabledStillServesDiscovery(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WebSocketEnabled = false
	_, port := startWSBoltServer(t, cfg)

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	requireNoError(t, err)
	defer conn.Close()

	_, _ = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	requireNoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 discovery even when WS disabled, got %d", resp.StatusCode)
	}
}

// TestWSBolt_OriginAllowlist_RejectsBogus asserts that with a non-* origin
// allowlist, a WS upgrade with a non-matching Origin header fails (S5).
func TestWSBolt_OriginAllowlist_RejectsBogus(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WebSocketAllowedOrigins = "http://allowed.example.com"
	_, port := startWSBoltServer(t, cfg)

	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	u := url.URL{Scheme: "ws", Host: fmt.Sprintf("127.0.0.1:%d", port), Path: "/"}
	hdr := http.Header{}
	hdr.Set("Origin", "http://evil.example.com")
	_, resp, err := dialer.Dial(u.String(), hdr)
	if err == nil {
		t.Fatal("expected dial to fail for non-matching origin")
	}
	if resp != nil && resp.StatusCode == 200 {
		t.Fatalf("expected 4xx, got 200 from bogus origin")
	}
}

// TestWSBolt_RequireTLS_RejectsPlaintext asserts that with RequireTLS=true,
// a plain bolt:// upgrade is rejected via the canonical Neo4j error
// (S "RequireTLS=true + WS"). The connection is closed immediately;
// the dialer surfaces the error.
func TestWSBolt_RequireTLS_RejectsPlaintext(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RequireTLS = true
	cfg.TLSConfig = &tls.Config{} // present but unused; RequireTLS gates everything
	_, port := startWSBoltServer(t, cfg)

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	requireNoError(t, err)
	defer conn.Close()
	// Send the start of a WS upgrade; the server should close us.
	_, _ = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: upgrade\r\n\r\n")
	// Reading from the conn should get EOF or a small error without 200.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 64)
	n, _ := conn.Read(buf)
	if strings.HasPrefix(string(buf[:n]), "HTTP/1.1 200") {
		t.Fatalf("expected RequireTLS to block plaintext WS, got 200 OK")
	}
}

// TestWSBolt_RawTCPStillWorks ensures the existing raw bolt:// path is
// not broken by the transport-selection refactor. Drives a TCP-level
// HELLO/RETURN 1.
func TestWSBolt_RawTCPStillWorks(t *testing.T) {
	_, port := startWSBoltServer(t, DefaultConfig())
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	requireNoError(t, err)
	defer conn.Close()
	requireNoError(t, PerformHandshakeWithTesting(t, conn))
	requireNoError(t, SendHello(t, conn, nil))
	requireNoError(t, ReadSuccess(t, conn))
	requireNoError(t, SendRun(t, conn, "RETURN 1 AS x", nil, nil))
	requireNoError(t, ReadSuccess(t, conn))
	requireNoError(t, SendPull(t, conn, nil))
	for {
		mt, _, err := ReadMessage(conn)
		requireNoError(t, err)
		if mt == MsgSuccess {
			break
		}
	}
}

// dialWSWithCookie opens a WS connection with the given Cookie header
// attached, simulating a browser sending nornicdb_token alongside the
// upgrade request.
func dialWSWithCookie(t *testing.T, port int, token string) *websocket.Conn {
	t.Helper()
	u := url.URL{Scheme: "ws", Host: fmt.Sprintf("127.0.0.1:%d", port), Path: "/"}
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	hdr := http.Header{}
	hdr.Set("Cookie", "nornicdb_token="+token)
	ws, _, err := dialer.Dial(u.String(), hdr)
	requireNoError(t, err)
	t.Cleanup(func() { _ = ws.Close() })
	return ws
}

// startAuthEnabledWSBoltServer wires a real auth.Authenticator into the
// Bolt server so HELLO scheme=bearer / cookie-implicit-bearer paths can
// be exercised end-to-end. Returns server, port, and a token bound to a
// freshly-created admin user.
func startAuthEnabledWSBoltServer(t *testing.T) (port int, adminToken string, badToken string) {
	t.Helper()
	authCfg := auth.DefaultAuthConfig()
	authCfg.JWTSecret = []byte("test-secret-key-for-jwt-signing!!")
	authCfg.SecurityEnabled = true
	mem := storage.NewMemoryEngine()
	authenticator, err := auth.NewAuthenticator(authCfg, mem)
	requireNoError(t, err)

	_, err = authenticator.CreateUser("admin", "admin-password", []auth.Role{auth.RoleAdmin})
	requireNoError(t, err)

	tokenResp, _, err := authenticator.Authenticate("admin", "admin-password", "127.0.0.1", "test")
	requireNoError(t, err)
	if tokenResp == nil || tokenResp.AccessToken == "" {
		t.Fatal("expected admin token from Authenticate")
	}
	adminToken = tokenResp.AccessToken
	badToken = adminToken + "garbage-suffix"

	cfg := DefaultConfig()
	cfg.RequireAuth = true
	cfg.Authenticator = NewAuthenticatorAdapter(authenticator)
	srv, port := startWSBoltServer(t, cfg)

	// Production wires this resolver from the HTTP server's
	// per-principal access table. For these unit tests we keep the
	// rule simple: admins reach every database, everyone else gets
	// denied (matching production's secure-by-default fallback).
	srv.SetDatabaseAccessModeResolver(func(roles []string) auth.DatabaseAccessMode {
		for _, r := range roles {
			if r == string(auth.RoleAdmin) {
				return auth.FullDatabaseAccessMode
			}
		}
		return auth.DenyAllDatabaseAccessMode
	})
	srv.SetResolvedAccessResolver(func(roles []string, _ string) auth.ResolvedAccess {
		for _, r := range roles {
			if r == string(auth.RoleAdmin) {
				return auth.ResolvedAccess{Read: true, Write: true}
			}
		}
		return auth.ResolvedAccess{}
	})
	return port, adminToken, badToken
}

// TestWSBolt_CookieImplicitBearer (S-CookieImplicitBearer) — a browser
// session that authenticated via /auth/token leaves nornicdb_token on
// the cookie jar; the WS upgrade ships it along; HELLO scheme=none
// promotes the session to the cookie's roles. Then a real RETURN 1
// query must round-trip with one RECORD and a final SUCCESS — anything
// less means the cookie path didn't actually grant the session enough
// access to drive a query.
func TestWSBolt_CookieImplicitBearer(t *testing.T) {
	port, adminToken, _ := startAuthEnabledWSBoltServer(t)
	ws := dialWSWithCookie(t, port, adminToken)
	conn := &wsConnAdapter{ws: ws}

	requireNoError(t, PerformHandshakeWithTesting(t, conn))
	requireNoError(t, SendHello(t, conn, nil)) // scheme defaults to "none"
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
				t.Fatalf("expected exactly 1 RECORD, got %d", gotRecords)
			}
			return
		case MsgFailure:
			t.Fatalf("unexpected FAILURE during PULL: %s", string(data))
		default:
			t.Fatalf("unexpected message 0x%02X during PULL", mt)
		}
	}
}

// TestWSBolt_NoCookieRequiresAuth (no implicit bearer, no anon, no
// HELLO creds) — the server requires auth, the cookie is absent, the
// HELLO is scheme=none. Expect a FAILURE.
func TestWSBolt_NoCookieRequiresAuth(t *testing.T) {
	port, _, _ := startAuthEnabledWSBoltServer(t)
	ws := dialWS(t, port) // no cookie
	conn := &wsConnAdapter{ws: ws}

	requireNoError(t, PerformHandshakeWithTesting(t, conn))
	requireNoError(t, SendHello(t, conn, nil))
	mt, _, err := ReadMessage(conn)
	requireNoError(t, err)
	if mt != MsgFailure {
		t.Fatalf("expected FAILURE without auth, got 0x%02X", mt)
	}
}

// TestWSBolt_InvalidCookieFallsThrough (S-InvalidCookieFallsThrough) —
// a tampered cookie does NOT promote the session; with RequireAuth=true
// and no fallback, HELLO scheme=none is rejected.
func TestWSBolt_InvalidCookieFallsThrough(t *testing.T) {
	port, _, badToken := startAuthEnabledWSBoltServer(t)
	ws := dialWSWithCookie(t, port, badToken)
	conn := &wsConnAdapter{ws: ws}

	requireNoError(t, PerformHandshakeWithTesting(t, conn))
	requireNoError(t, SendHello(t, conn, nil))
	mt, _, err := ReadMessage(conn)
	requireNoError(t, err)
	if mt != MsgFailure {
		t.Fatalf("expected FAILURE for invalid cookie, got 0x%02X", mt)
	}
}

// TestWSBolt_BearerOverridesCookie (S-BearerOverridesCookie) — when
// HELLO carries scheme=bearer with explicit credentials, the server
// honors them and does NOT consult the cookie. Drive an invalid cookie
// + a valid HELLO bearer; assert SUCCESS and a real RUN/PULL succeed.
func TestWSBolt_BearerOverridesCookie(t *testing.T) {
	port, adminToken, badToken := startAuthEnabledWSBoltServer(t)
	ws := dialWSWithCookie(t, port, badToken) // tampered cookie
	conn := &wsConnAdapter{ws: ws}

	requireNoError(t, PerformHandshakeWithTesting(t, conn))
	requireNoError(t, SendHello(t, conn, map[string]string{
		"scheme":      "bearer",
		"credentials": adminToken,
	}))
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
				t.Fatalf("expected exactly 1 RECORD, got %d", gotRecords)
			}
			return
		case MsgFailure:
			t.Fatalf("unexpected FAILURE during PULL: %s", string(data))
		default:
			t.Fatalf("unexpected message 0x%02X during PULL", mt)
		}
	}
}

// TestWSBolt_TCPHasNoImplicitBearer (S-CookieIgnoredOnTCP) — raw TCP
// has no HTTP layer, so even if a hypothetical client sent something
// cookie-shaped it would never reach the implicit-bearer path. With
// RequireAuth=true and HELLO scheme=none, raw bolt:// is rejected.
func TestWSBolt_TCPHasNoImplicitBearer(t *testing.T) {
	port, _, _ := startAuthEnabledWSBoltServer(t)
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	requireNoError(t, err)
	defer conn.Close()
	requireNoError(t, PerformHandshakeWithTesting(t, conn))
	requireNoError(t, SendHello(t, conn, nil))
	mt, _, err := ReadMessage(conn)
	requireNoError(t, err)
	if mt != MsgFailure {
		t.Fatalf("expected FAILURE on raw TCP without HELLO creds, got 0x%02X", mt)
	}
}

// silence unused imports in narrow test compilations.
var _ = context.Background
