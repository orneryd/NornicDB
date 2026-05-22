package bolt

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// wsAcceptResult bundles everything handleConnection needs after a
// successful WS upgrade: the wsConn adapter, a fresh bufio.Reader sitting
// on top of it for the Bolt handshake, and the implicit-bearer JWT
// extracted from either the nornicdb_token cookie or the
// Authorization: Bearer header on the upgrade request (empty if
// neither is present). The token rides through to Session.implicitBearer
// so a HELLO scheme=none promotes the session to those claims.
type wsAcceptResult struct {
	conn           *wsConn
	br             *bufio.Reader
	implicitBearer string
}

// acceptWebSocket consumes the HTTP request that began with "GET " on
// the Bolt port, then either:
//
//   - serves the discovery response and closes the conn (plain GET / with
//     no Upgrade headers; mirrors Neo4j's DiscoveryResponseHandler), OR
//   - returns 426 Upgrade Required when WebSocketEnabled is false and the
//     client sent a real Upgrade: websocket request, OR
//   - completes the WebSocket handshake and returns a *wsAcceptResult
//     that the caller uses to construct a Bolt Session.
//
// On the WS happy path, the returned *wsConn's bufio.Reader is freshly
// allocated by the caller against the wsConn; the peeked bytes from
// peekTransport are consumed by reading the HTTP request from br.
func (s *Server) acceptWebSocket(
	conn net.Conn,
	br *bufio.Reader,
	remoteAddr string,
) (*wsAcceptResult, error) {
	// Read the HTTP request from the bufio.Reader (which already holds
	// the peeked "GET " bytes plus whatever else is buffered). This is
	// the standard pattern: net/http.ReadRequest pulls a complete
	// request from a bufio.Reader.
	req, err := http.ReadRequest(br)
	if err != nil {
		s.logger().Warn("ws upgrade read request failed", "remote", remoteAddr, slog.Any("error", err))
		return nil, err
	}

	// Cap the request size to avoid memory exhaustion via oversized headers.
	if req.Body != nil {
		req.Body = http.MaxBytesReader(nil, req.Body, 65536)
	}

	// Require a path of "/" — anything else is a misconfigured driver.
	if req.URL != nil && req.URL.Path != "" && req.URL.Path != "/" {
		writeHTTPStatus(conn, http.StatusNotFound, "Not Found\n")
		if ms := s.metricsState; ms != nil && ms.bag != nil {
			incBoltConnectionsRejected(ms.bag, "ws_handshake")
		}
		return nil, fmt.Errorf("ws upgrade rejected: path %q != /", req.URL.Path)
	}

	// If this is NOT a real WS upgrade, serve the discovery response.
	if !isWebSocketUpgrade(req.Header) {
		s.serveDiscovery(conn)
		return nil, nil
	}

	// WS upgrade attempted. If WS is disabled, return 426.
	if !s.config.WebSocketEnabled {
		writeWSDisabled426(conn, s.listener.Addr())
		if ms := s.metricsState; ms != nil && ms.bag != nil {
			incBoltConnectionsRejected(ms.bag, "ws_disabled")
		}
		return nil, nil
	}

	// Implicit-bearer extraction. Two HTTP credential surfaces feed the
	// same slot; cookie wins on conflict because it's first-party-set
	// by the NornicDB HTTP server. HELLO scheme=bearer/basic always wins
	// over either of these in handleHello (this is just the "scheme=none
	// promotes via HTTP credential" path). Validation deferred to
	// handleHello so this file doesn't import pkg/auth.
	implicitBearer := ""
	hasCookie := false
	hasAuthHeader := false
	if c, err := req.Cookie("nornicdb_token"); err == nil && c != nil {
		implicitBearer = c.Value
		hasCookie = true
	}
	if implicitBearer == "" {
		if h := req.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
			implicitBearer = strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
			hasAuthHeader = true
		}
	}
	s.logger().Debug("ws upgrade credentials",
		"remote", remoteAddr,
		"has_cookie", hasCookie,
		"has_authorization_header", hasAuthHeader,
		"implicit_bearer_present", implicitBearer != "")

	// Actual WS handshake.
	upgrader := s.getUpgrader()

	// gorilla/websocket.Upgrader.Upgrade wants a real http.ResponseWriter
	// + *http.Request. We bridge by writing the HTTP response via the
	// upgrader's NewServerConn-style entry point. The cleanest path is
	// to construct a tiny ResponseWriter that writes to conn; gorilla
	// uses Hijacker to take over the connection. We wrap our raw conn
	// (and the bufio.Reader holding any post-handshake bytes) in a
	// hijackableResponseWriter so Upgrade can call Hijack().
	hrw := &hijackableResponseWriter{
		conn:   conn,
		bufrw:  bufio.NewReadWriter(br, bufio.NewWriter(conn)),
		header: http.Header{},
	}

	wsc, err := upgrader.Upgrade(hrw, req, nil)
	if err != nil {
		if ms := s.metricsState; ms != nil && ms.bag != nil {
			incBoltConnectionsRejected(ms.bag, "ws_handshake")
		}
		s.logger().Warn("ws upgrade failed", "remote", remoteAddr, slog.Any("error", err))
		return nil, err
	}

	// Apply WS read limits. Ping/pong is intentionally NOT installed
	// here — installing the pong handler before HELLO would let any
	// pong frame arriving during the pre-HELLO budget extend the read
	// deadline and bypass BoltAuthTimeout. The session loop calls
	// startWebSocketKeepalive after a successful HELLO, by which point
	// we want pong frames to keep the session alive normally.
	if max := s.config.WebSocketMaxMessageSize; max > 0 {
		wsc.SetReadLimit(max)
	}

	// Identify TLS underlying for the metric label.
	_, isTLS := conn.(*tls.Conn)

	// Synthesize *net.TCPAddr from the underlying conn so handleRoute's
	// type assertion succeeds for WS clients.
	localAddr := conn.LocalAddr()
	remoteNetAddr := conn.RemoteAddr()

	wc := newWSConn(wsc, localAddr, remoteNetAddr)
	wc.encrypted = isTLS
	if ms := s.metricsState; ms != nil && ms.bag != nil {
		bag := ms.bag
		wc.onOversize = func() { incWebSocketOversized(bag) }
	}

	// Caller constructs Session.reader as bufio.NewReaderSize(wc, ...),
	// because the WS frame stream begins fresh after the HTTP upgrade.
	wsBr := bufio.NewReaderSize(wc, s.config.ReadBufferSize)
	return &wsAcceptResult{conn: wc, br: wsBr, implicitBearer: implicitBearer}, nil
}

// startWebSocketKeepalive installs the pong handler and spawns the ping
// loop on a wsConn. Called from handleConnection AFTER the Bolt HELLO
// succeeds so the pre-HELLO BoltAuthTimeout is not overridden by a
// pong-driven deadline extension. clamps a zero pongTimeout to a sane
// default so a partial config (ping > 0, pong = 0) doesn't expire every
// WriteControl deadline immediately.
func (s *Server) startWebSocketKeepalive(wc *wsConn) {
	if wc == nil {
		return
	}
	pingInterval := s.config.WebSocketPingInterval
	if pingInterval <= 0 {
		return
	}
	pongTimeout := s.config.WebSocketPongTimeout
	if pongTimeout <= 0 {
		pongTimeout = 60 * time.Second
	}
	ws := wc.ws
	_ = ws.SetReadDeadline(time.Now().Add(pongTimeout))
	ws.SetPongHandler(func(string) error {
		_ = ws.SetReadDeadline(time.Now().Add(pongTimeout))
		return nil
	})
	go pingLoop(ws, pingInterval, pongTimeout, s.closed.Load)
}

// pingLoop periodically sends WS ping control frames. Stops when the
// shouldStop closure returns true or the underlying conn errors.
func pingLoop(ws *websocket.Conn, interval, pongTimeout time.Duration, shouldStop func() bool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		<-ticker.C
		if shouldStop != nil && shouldStop() {
			return
		}
		// WriteControl is internally synchronized inside gorilla; safe to
		// call concurrently with an outstanding WriteMessage.
		err := ws.WriteControl(
			websocket.PingMessage,
			nil,
			time.Now().Add(pongTimeout),
		)
		if err != nil {
			return
		}
	}
}

// getUpgrader returns the gorilla websocket Upgrader, lazily constructed
// the first time it's needed and cached on the Server.
func (s *Server) getUpgrader() *websocket.Upgrader {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.upgrader != nil {
		return s.upgrader
	}
	allowed := strings.TrimSpace(s.config.WebSocketAllowedOrigins)
	checkOrigin := func(*http.Request) bool { return true }
	if allowed != "" && allowed != "*" {
		allowedOrigins := splitAndTrim(allowed)
		checkOrigin = func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			if origin == "" {
				return false
			}
			for _, a := range allowedOrigins {
				if strings.EqualFold(a, origin) {
					return true
				}
			}
			return false
		}
	}
	s.upgrader = &websocket.Upgrader{
		ReadBufferSize:    256 * 1024,
		WriteBufferSize:   256 * 1024,
		WriteBufferPool:   &sync.Pool{},
		EnableCompression: false,
		Subprotocols:      nil,
		HandshakeTimeout:  10 * time.Second,
		CheckOrigin:       checkOrigin,
	}
	return s.upgrader
}

// splitAndTrim splits s on "," and trims whitespace from each element.
func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// hijackableResponseWriter is a minimal http.ResponseWriter that exists
// solely so gorilla/websocket.Upgrader.Upgrade can call Hijack() to take
// over the raw conn for the WS handshake.
type hijackableResponseWriter struct {
	conn   net.Conn
	bufrw  *bufio.ReadWriter
	header http.Header
	status int
}

func (h *hijackableResponseWriter) Header() http.Header { return h.header }
func (h *hijackableResponseWriter) Write(p []byte) (int, error) {
	return h.bufrw.Write(p)
}
func (h *hijackableResponseWriter) WriteHeader(status int) {
	h.status = status
}
func (h *hijackableResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return h.conn, h.bufrw, nil
}

// writeWSDisabled426 writes the canonical 426 Upgrade Required response
// when WebSocketEnabled=false and the client attempted a real WS upgrade.
func writeWSDisabled426(conn net.Conn, listenerAddr net.Addr) {
	hostPort := "<host>:<port>"
	if listenerAddr != nil {
		hostPort = listenerAddr.String()
	}
	body := fmt.Sprintf(
		"WebSocket transport disabled on this server. Connect via raw Bolt TCP at bolt://%s/ instead.\n",
		hostPort,
	)
	resp := fmt.Sprintf(
		"HTTP/1.1 426 Upgrade Required\r\n"+
			"Content-Type: text/plain\r\n"+
			"Connection: close\r\n"+
			"Content-Length: %d\r\n"+
			"\r\n%s",
		len(body), body,
	)
	_, _ = io.WriteString(conn, resp)
}

// writeHTTPStatus writes a minimal HTTP/1.1 response with the given status
// code and plaintext body, then closes the conn. Used for 404 etc.
func writeHTTPStatus(conn net.Conn, status int, body string) {
	statusText := http.StatusText(status)
	if statusText == "" {
		statusText = "Error"
	}
	resp := fmt.Sprintf(
		"HTTP/1.1 %d %s\r\n"+
			"Content-Type: text/plain\r\n"+
			"Connection: close\r\n"+
			"Content-Length: %d\r\n"+
			"\r\n%s",
		status, statusText, len(body), body,
	)
	_, _ = io.WriteString(conn, resp)
}
