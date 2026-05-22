package bolt

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// transportKind identifies the wire-level transport detected on the Bolt
// listener after sniffing the first bytes of a connection.
type transportKind int

const (
	// transportRaw indicates the connection began with the four-byte Bolt
	// magic preamble; the existing raw-Bolt handshake handles it.
	transportRaw transportKind = iota
	// transportWebSocket indicates the connection began with the HTTP "GET "
	// method prefix and should be passed through the WebSocket upgrade
	// pipeline before Bolt framing resumes.
	transportWebSocket
)

// boltMagic is the four-byte Bolt preamble: 0x60 0x60 0xB0 0x17.
var boltMagic = []byte{0x60, 0x60, 0xB0, 0x17}

// httpGet is the four-byte "GET " HTTP method prefix used by browser-based
// drivers issuing a WebSocket upgrade request.
var httpGet = []byte("GET ")

// defaultPeekBufferSize is the default size of the bufio.Reader that
// peekTransport allocates around an accepted connection. It is large enough
// to hold a full WebSocket handshake plus several Bolt RECORD batches.
const defaultPeekBufferSize = 256 * 1024

// boltSniffTimeout bounds how long peekTransport will wait for the first
// five bytes of a connection. Exposed as a package-level var so the server
// can override it; mirrors Neo4j's transport-sniff budget (default 5s).
var boltSniffTimeout = 5 * time.Second

// ErrUnencryptedRequired is returned when peekTransport refuses a plaintext
// connection because RequireTLS is set. The error text matches Neo4j's
// canonical message verbatim so existing driver assertions pass unchanged.
var ErrUnencryptedRequired = errors.New("An unencrypted connection attempt was made where encryption is required.")

// peekedConn re-presents bytes already buffered in a *bufio.Reader to a
// downstream reader (typically tls.Server). The bufio.Reader sits in front
// of the underlying net.Conn, so subsequent reads first drain the peek
// buffer and then continue reading from the conn transparently.
type peekedConn struct {
	net.Conn
	r io.Reader
}

func (p *peekedConn) Read(b []byte) (int, error) { return p.r.Read(b) }

// peekTransport sniffs the first 5 bytes of conn to decide which Bolt
// transport the client speaks. The four outcomes are:
//
//   - transportRaw      - bytes begin with the Bolt magic preamble.
//   - transportWebSocket - bytes begin with "GET " (HTTP upgrade request).
//   - TLS branch        - first byte is 0x16 (TLS handshake record), tlsCfg
//     is non-nil, and !isEncrypted: the conn is wrapped in tls.Server, the
//     handshake completes, and peekTransport recurses with isEncrypted=true.
//   - error             - unrecognized prefix, or RequireTLS rejected a
//     plaintext prefix.
//
// The returned *bufio.Reader is the SAME reader used to peek. Its buffer
// holds the first 5 bytes of the post-TLS stream (or the original stream
// when no TLS occurred). Callers MUST construct Session with this reader
// rather than allocating a fresh bufio.NewReaderSize over the returned
// conn, otherwise the peeked bytes are lost and the Bolt handshake fails.
func peekTransport(
	conn net.Conn,
	tlsCfg *tls.Config,
	isEncrypted bool,
	requireTLS bool,
	bufSize int,
) (kind transportKind, conn2 net.Conn, br *bufio.Reader, err error) {
	if bufSize <= 0 {
		bufSize = defaultPeekBufferSize
	}

	// TODO: pool *bufio.Reader to avoid one allocation per accept under
	// WS-storm load.
	br = bufio.NewReaderSize(conn, bufSize)

	if err = conn.SetReadDeadline(time.Now().Add(boltSniffTimeout)); err != nil {
		return 0, conn, nil, fmt.Errorf("set sniff read deadline: %w", err)
	}

	head, peekErr := br.Peek(5)
	if peekErr != nil {
		var netErr net.Error
		if errors.As(peekErr, &netErr) && netErr.Timeout() {
			return 0, conn, nil, fmt.Errorf("transport sniff timeout: %w", peekErr)
		}
		return 0, conn, nil, peekErr
	}

	// Restore an unbounded read deadline; subsequent handlers manage their
	// own deadlines (Bolt auth timeout, WS handshake timeout, etc).
	if err = conn.SetReadDeadline(time.Time{}); err != nil {
		return 0, conn, nil, fmt.Errorf("clear sniff read deadline: %w", err)
	}

	switch {
	case tlsCfg != nil && !isEncrypted && head[0] == 0x16:
		// Wrap conn in tls.Server, then complete the handshake on a
		// reader that re-presents the peeked bytes. The bufio.Reader
		// is the byte source so the TLS handshake sees everything we
		// already buffered, plus future bytes from conn.
		tlsConn := tls.Server(&peekedConn{Conn: conn, r: br}, tlsCfg)
		if err = tlsConn.Handshake(); err != nil {
			return 0, conn, nil, fmt.Errorf("tls handshake: %w", err)
		}
		// Recurse on the decrypted stream. isEncrypted=true forbids
		// nested TLS in the recursive call.
		return peekTransport(tlsConn, tlsCfg, true, requireTLS, bufSize)

	case bytes.HasPrefix(head, httpGet):
		if requireTLS && !isEncrypted {
			return 0, conn, nil, ErrUnencryptedRequired
		}
		return transportWebSocket, conn, br, nil

	case bytes.HasPrefix(head, boltMagic):
		if requireTLS && !isEncrypted {
			return 0, conn, nil, ErrUnencryptedRequired
		}
		return transportRaw, conn, br, nil

	default:
		return 0, conn, nil, fmt.Errorf("unrecognized transport prefix: %x", head[:4])
	}
}
