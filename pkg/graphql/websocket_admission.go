package graphql

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/gorilla/websocket"
	"github.com/orneryd/nornicdb/pkg/memory"
)

const websocketMemoryErrorCode = "Neo.TransientError.General.MemoryPoolOutOfMemoryError"

// policyWebsocketImplementation uses gqlgen's public transport adapter API.
// A streaming NextReader lets the shared policy admit bytes before buffering
// them, unlike gqlgen's default adapter, which reads the complete message.
type policyWebsocketImplementation struct{}

type websocketOwnerKey struct{}
type websocketRequestOwner struct {
	wire *memory.WireRequest
	conn *policyWebsocketConn
}

func withWebsocketRequestOwner(r *http.Request, owner *websocketRequestOwner) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), websocketOwnerKey{}, owner))
}

func (o *websocketRequestOwner) close() {
	if o.conn != nil {
		_ = o.conn.Close()
	}
}

var _ transport.WebsocketImplementation = policyWebsocketImplementation{}
var _ transport.WebsocketConn = (*policyWebsocketConn)(nil)
var _ transport.WebsocketReadLimiter = (*policyWebsocketConn)(nil)
var _ transport.WebsocketReadDeadliner = (*policyWebsocketConn)(nil)

func (policyWebsocketImplementation) Accept(w http.ResponseWriter, r *http.Request, options transport.WebsocketAcceptOptions) (transport.WebsocketConn, error) {
	owner, ok := r.Context().Value(websocketOwnerKey{}).(*websocketRequestOwner)
	if !ok {
		return nil, errors.New("GraphQL websocket has no request lifetime owner")
	}
	upgrader := websocket.Upgrader{Subprotocols: options.Subprotocols}
	conn, err := upgrader.Upgrade(w, r, options.ResponseHeader)
	if err != nil {
		return nil, err
	}
	c := &policyWebsocketConn{conn: conn, wire: owner.wire, metadataBytes: memory.HTTPRequestWireBytes(r)}
	c.stopCancel = context.AfterFunc(r.Context(), func() { _ = conn.Close() })
	owner.conn = c
	return c, nil
}

type policyWebsocketConn struct {
	conn          *websocket.Conn
	readMu        sync.Mutex
	writeMu       sync.Mutex
	closeOnce     sync.Once
	wire          *memory.WireRequest // handler-owned, retained through synchronous decode/setup
	metadataBytes int64
	stopCancel    func() bool
}

func (c *policyWebsocketConn) NextReader() (kind int, result io.Reader, err error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	c.wire.Reset()
	// This reads framing only. No application lease is held while an idle
	// client waits to send its next message, and payload bytes are not buffered.
	kind, reader, err := c.conn.NextReader()
	if err != nil {
		if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseNoStatusReceived) {
			err = transport.ErrWebsocketClosed
		}
		return kind, nil, err
	}
	if err = c.wire.Grow(c.metadataBytes); err != nil {
		return kind, nil, c.reject(err, reader)
	}
	limit, err := c.wire.WireLimit()
	if err != nil {
		return kind, nil, c.reject(err, reader)
	}
	var data []byte
	var scratch [32 << 10]byte
	for {
		// Probe at most one byte beyond the admitted limit in fixed scratch
		// storage, never in the frame buffer or gqlgen's JSON decoder.
		readSize := int64(len(scratch))
		if limit > 0 {
			readSize = min(readSize, limit-c.metadataBytes-int64(len(data))+1)
		}
		n, readErr := reader.Read(scratch[:int(readSize)])
		if n > 0 {
			wireSize := int64(len(data)) + int64(n)
			if limit > 0 && memory.WireSize(c.metadataBytes, wireSize) > limit {
				return kind, nil, c.reject(memory.ErrLimitExceeded, reader)
			}
			if err = c.wire.Grow(c.metadataBytes, wireSize); err != nil {
				return kind, nil, c.reject(err, reader)
			}
			data = append(data, scratch[:n]...)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return kind, nil, readErr
		}
	}
	return kind, bytes.NewReader(data), nil
}

func (c *policyWebsocketConn) reject(err error, reader io.Reader) error {
	_ = c.writeClose(websocket.CloseTryAgainLater, websocketMemoryErrorCode, reader)
	return err
}

func (c *policyWebsocketConn) WriteJSON(value any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteJSON(value)
}

func (c *policyWebsocketConn) WriteClose(code int, message string) (err error) {
	return c.writeClose(code, message, nil)
}

func (c *policyWebsocketConn) writeClose(code int, message string, rejected io.Reader) (err error) {
	c.closeOnce.Do(func() {
		err = c.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, message), time.Now().Add(time.Second))
		if rejected != nil {
			// Closing with unread TCP data can reset the socket on Windows,
			// discarding the close frame. Drain the rejected message without
			// buffering or decoding it. The existing transport read limit and
			// this deadline bound a client that keeps sending.
			_ = c.conn.SetReadDeadline(time.Now().Add(time.Second))
			_, _ = io.Copy(io.Discard, rejected)
		}
		_ = c.conn.Close()
	})
	return err
}

func (c *policyWebsocketConn) Close() error {
	if c.stopCancel != nil {
		c.stopCancel()
	}
	err := c.conn.Close()
	// Join framing, but keep decode/setup credit until the handler's Run ends.
	// gqlgen may close the transport while its synchronous consumer is live.
	c.readMu.Lock()
	c.readMu.Unlock()
	return err
}

func (c *policyWebsocketConn) Subprotocol() string      { return c.conn.Subprotocol() }
func (c *policyWebsocketConn) SetReadLimit(limit int64) { c.conn.SetReadLimit(limit) }
func (c *policyWebsocketConn) SetReadDeadline(deadline time.Time) error {
	return c.conn.SetReadDeadline(deadline)
}
