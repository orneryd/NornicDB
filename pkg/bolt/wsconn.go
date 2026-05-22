package bolt

import (
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// wsConn adapts a *websocket.Conn to net.Conn so the existing Bolt session
// machinery (which wraps net.Conn in bufio.Reader/Writer) works unchanged
// over a WebSocket transport.
//
// Synchronization invariant (load-bearing; do not change without re-reading
// gorilla/websocket@v1.5.3/conn.go):
//
//   - writeMu serializes all data-frame writes that flow through
//     (*websocket.Conn).WriteMessage / NextWriter. gorilla panics if two
//     goroutines drive WriteMessage / NextWriter concurrently
//     (conn.go:751); this mutex is the only thing keeping that contract.
//   - WriteControl (used for ping and close frames) is internally
//     synchronized inside gorilla via a separate mu and is safe to call
//     concurrently with an in-flight WriteMessage. The ping goroutine
//     therefore does NOT acquire writeMu.
//   - Close acquires writeMu so that it cannot race with a Write in
//     progress, then sends the close frame via WriteControl, then closes
//     the underlying conn.
//   - currentReader is touched only from Read; reads are serialized by
//     the single Bolt session goroutine that owns the connection, so it
//     does not need its own mutex.
type wsConn struct {
	ws *websocket.Conn

	// currentReader is the io.Reader returned by the most recent
	// (*websocket.Conn).NextReader() call. Held until exhausted, then
	// refreshed. NO intermediate bytes.Buffer — reads stream straight
	// from the WebSocket frame's reader into the Bolt bufio.Reader.
	currentReader io.Reader

	// writeMu serializes data-frame writes (gorilla requires external
	// serialization for WriteMessage/NextWriter; control frames go
	// through WriteControl which is internally synchronized).
	writeMu sync.Mutex

	localAddr  net.Addr
	remoteAddr net.Addr

	// Deadlines stored as atomic.Pointer[time.Time] so the read path
	// can see updates without locking. A nil pointer means "no
	// deadline" (matches net.Conn semantics for a zero time).
	readDeadline  atomic.Pointer[time.Time]
	writeDeadline atomic.Pointer[time.Time]

	// closed gates Close to be idempotent.
	closed atomic.Bool

	// encrypted records whether the underlying transport was a *tls.Conn
	// at upgrade time. Used by the metric label resolver to distinguish
	// ws from ws_tls.
	encrypted bool

	// onOversize is invoked exactly once when the read loop observes
	// gorilla's "read limit exceeded" close (set via SetReadLimit when the
	// adapter is constructed by the WS upgrade pipeline). The hook lets
	// the metric layer increment websocket_oversized_total without
	// coupling this file to pkg/observability.
	onOversize func()
}

// newWSConn wraps ws in an adapter that satisfies net.Conn.
//
// The caller is expected to pass localAddr/remoteAddr already extracted
// from ws.UnderlyingConn() (so this works whether the underlying conn is
// a *net.TCPConn or a *tls.Conn). If either address is not already a
// *net.TCPAddr the constructor synthesizes one from the address's String
// representation, falling back to {IPv4zero, 0} when no usable host:port
// can be parsed. handleRoute on the Bolt server type-asserts
// LocalAddr() to *net.TCPAddr; returning anything else would silently
// route WS clients to localhost:7687.
func newWSConn(ws *websocket.Conn, localAddr, remoteAddr net.Addr) *wsConn {
	return &wsConn{
		ws:         ws,
		localAddr:  toTCPAddr(localAddr),
		remoteAddr: toTCPAddr(remoteAddr),
	}
}

// toTCPAddr coerces an arbitrary net.Addr to a *net.TCPAddr, synthesizing
// zero values when the input is nil or not a TCP address.
func toTCPAddr(a net.Addr) *net.TCPAddr {
	if a == nil {
		return &net.TCPAddr{IP: net.IPv4zero, Port: 0}
	}
	if tcp, ok := a.(*net.TCPAddr); ok {
		return tcp
	}
	// Try to resolve the string form (handles things like *net.IPAddr
	// with an embedded port-less host). If that fails, return zero.
	if tcp, err := net.ResolveTCPAddr("tcp", a.String()); err == nil && tcp != nil {
		return tcp
	}
	return &net.TCPAddr{IP: net.IPv4zero, Port: 0}
}

// Read implements net.Conn.Read. It pulls bytes from the current
// per-message reader, advancing to the next BinaryMessage frame when
// the current one is exhausted. Text frames are dropped silently
// (mirroring Neo4j's WebSocketFrameUnpackingDecoder which is typed to
// BinaryWebSocketFrame only).
func (c *wsConn) Read(p []byte) (int, error) {
	if d := c.readDeadline.Load(); d != nil {
		_ = c.ws.SetReadDeadline(*d)
	}
	for {
		if c.currentReader == nil {
			mt, r, err := c.ws.NextReader()
			if err != nil {
				if isOversizedErr(err) && c.onOversize != nil {
					c.onOversize()
				}
				return 0, err
			}
			if mt != websocket.BinaryMessage {
				continue // text frames dropped silently, mirrors Neo4j
			}
			c.currentReader = r
		}
		n, err := c.currentReader.Read(p)
		if errors.Is(err, io.EOF) {
			c.currentReader = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		if err != nil && isOversizedErr(err) && c.onOversize != nil {
			c.onOversize()
		}
		return n, err
	}
}

// isOversizedErr reports whether err signals that gorilla closed the
// connection because the peer sent a message exceeding SetReadLimit.
// gorilla returns *websocket.CloseError{Code: 1009} via NextReader and a
// plain "websocket: read limit exceeded" error on the per-message reader.
func isOversizedErr(err error) bool {
	if err == nil {
		return false
	}
	var ce *websocket.CloseError
	if errors.As(err, &ce) && ce.Code == websocket.CloseMessageTooBig {
		return true
	}
	return strings.Contains(err.Error(), "read limit exceeded")
}

// Write implements net.Conn.Write. Each Write becomes exactly one
// BinaryMessage. Holding writeMu for the full WriteMessage call satisfies
// gorilla's "no concurrent writers" requirement for data frames.
func (c *wsConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if d := c.writeDeadline.Load(); d != nil {
		_ = c.ws.SetWriteDeadline(*d)
	}
	if err := c.ws.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Close implements net.Conn.Close. It is idempotent: subsequent calls
// after the first return nil without touching the underlying conn.
func (c *wsConn) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.writeMu.Lock()
	// Best-effort close frame; errors here are not actionable because
	// we are about to drop the underlying transport anyway.
	_ = c.ws.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(time.Second),
	)
	c.writeMu.Unlock()
	return c.ws.Close()
}

// LocalAddr implements net.Conn.LocalAddr. Always returns a *net.TCPAddr
// (the constructor synthesizes one if the source addr is not TCP) so the
// Bolt server's handleRoute type assertion works.
func (c *wsConn) LocalAddr() net.Addr { return c.localAddr }

// RemoteAddr implements net.Conn.RemoteAddr. Always returns a *net.TCPAddr.
func (c *wsConn) RemoteAddr() net.Addr { return c.remoteAddr }

// SetDeadline implements net.Conn.SetDeadline. Sets both read and write
// deadlines.
func (c *wsConn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

// SetReadDeadline implements net.Conn.SetReadDeadline.
func (c *wsConn) SetReadDeadline(t time.Time) error {
	if t.IsZero() {
		c.readDeadline.Store(nil)
		return nil
	}
	tt := t
	c.readDeadline.Store(&tt)
	return nil
}

// SetWriteDeadline implements net.Conn.SetWriteDeadline.
func (c *wsConn) SetWriteDeadline(t time.Time) error {
	if t.IsZero() {
		c.writeDeadline.Store(nil)
		return nil
	}
	tt := t
	c.writeDeadline.Store(&tt)
	return nil
}
