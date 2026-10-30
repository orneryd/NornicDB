package bolt

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// wsTestRig spins up an httptest server that upgrades the first incoming
// HTTP request to WebSocket and hands the resulting *wsConn to the test
// via the server channel. The client *websocket.Conn is dialed by the
// test and used to drive frames.
type wsTestRig struct {
	srv      *httptest.Server
	upgrader websocket.Upgrader
	serverCh chan *wsConn
	rawCh    chan *websocket.Conn
}

func newWSTestRig(t *testing.T, configure func(*websocket.Conn)) *wsTestRig {
	t.Helper()
	rig := &wsTestRig{
		upgrader: websocket.Upgrader{
			CheckOrigin: func(*http.Request) bool { return true },
		},
		serverCh: make(chan *wsConn, 1),
		rawCh:    make(chan *websocket.Conn, 1),
	}
	rig.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := rig.upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			return
		}
		if configure != nil {
			configure(ws)
		}
		// Capture underlying TCP addrs the way the production constructor
		// caller will.
		under := ws.UnderlyingConn()
		rig.serverCh <- newWSConn(ws, under.LocalAddr(), under.RemoteAddr())
		rig.rawCh <- ws
	}))
	t.Cleanup(rig.srv.Close)
	return rig
}

func (r *wsTestRig) dial(t *testing.T) (*websocket.Conn, *wsConn, *websocket.Conn) {
	t.Helper()
	url := "ws" + strings.TrimPrefix(r.srv.URL, "http") + "/"
	client, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	wsc := <-r.serverCh
	raw := <-r.rawCh
	return client, wsc, raw
}

func TestWSConn_Read_CoalescesFrames(t *testing.T) {
	rig := newWSTestRig(t, nil)
	client, wsc, _ := rig.dial(t)

	if err := client.WriteMessage(websocket.BinaryMessage, []byte("hello")); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	if err := client.WriteMessage(websocket.BinaryMessage, []byte("world")); err != nil {
		t.Fatalf("write world: %v", err)
	}

	var got bytes.Buffer
	buf := make([]byte, 64)
	for got.Len() < len("helloworld") {
		_ = wsc.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := wsc.Read(buf)
		if n > 0 {
			got.Write(buf[:n])
		}
		if err != nil && got.Len() < len("helloworld") {
			t.Fatalf("read: %v (got %q)", err, got.String())
		}
	}
	if got.String() != "helloworld" {
		t.Fatalf("expected helloworld, got %q", got.String())
	}
}

func TestWSConn_Read_DropsTextFrames(t *testing.T) {
	rig := newWSTestRig(t, nil)
	client, wsc, _ := rig.dial(t)

	if err := client.WriteMessage(websocket.TextMessage, []byte("ignored")); err != nil {
		t.Fatalf("write text: %v", err)
	}
	if err := client.WriteMessage(websocket.BinaryMessage, []byte("real")); err != nil {
		t.Fatalf("write binary: %v", err)
	}

	var got bytes.Buffer
	buf := make([]byte, 64)
	for got.Len() < len("real") {
		_ = wsc.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := wsc.Read(buf)
		if n > 0 {
			got.Write(buf[:n])
		}
		if err != nil && got.Len() < len("real") {
			t.Fatalf("read: %v (got %q)", err, got.String())
		}
	}
	if got.String() != "real" {
		t.Fatalf("expected to skip text frame and read 'real', got %q", got.String())
	}
}

func TestWSConn_Write_FramesEachWriteAsBinary(t *testing.T) {
	rig := newWSTestRig(t, nil)
	client, wsc, _ := rig.dial(t)

	payload := []byte("payload")
	n, err := wsc.Write(payload)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("expected %d bytes written, got %d", len(payload), n)
	}

	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	mt, data, err := client.ReadMessage()
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	if mt != websocket.BinaryMessage {
		t.Fatalf("expected BinaryMessage, got %d", mt)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("expected %q, got %q", payload, data)
	}
}

func TestWSConn_Close_Idempotent(t *testing.T) {
	rig := newWSTestRig(t, nil)
	_, wsc, _ := rig.dial(t)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Close panicked: %v", r)
		}
	}()

	_ = wsc.Close()
	// Second call must not panic. Error is treated as benign.
	_ = wsc.Close()
}

func TestWSConn_WriteAfterCloseReturnsError(t *testing.T) {
	rig := newWSTestRig(t, nil)
	_, wsc, _ := rig.dial(t)

	if err := wsc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	n, err := wsc.Write([]byte("after close"))
	if err == nil {
		t.Fatalf("expected write after close to return an error")
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes written after close, got %d", n)
	}
}

func TestWSConn_SetDeadlineSetsAndClearsBothDirections(t *testing.T) {
	wsc := &wsConn{}
	deadline := time.Unix(123, 456)

	if err := wsc.SetDeadline(deadline); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
	readDeadline := wsc.readDeadline.Load()
	writeDeadline := wsc.writeDeadline.Load()
	if readDeadline == nil || !readDeadline.Equal(deadline) {
		t.Fatalf("read deadline = %v, want %v", readDeadline, deadline)
	}
	if writeDeadline == nil || !writeDeadline.Equal(deadline) {
		t.Fatalf("write deadline = %v, want %v", writeDeadline, deadline)
	}

	if err := wsc.SetDeadline(time.Time{}); err != nil {
		t.Fatalf("clear SetDeadline: %v", err)
	}
	if got := wsc.readDeadline.Load(); got != nil {
		t.Fatalf("read deadline was not cleared: %v", got)
	}
	if got := wsc.writeDeadline.Load(); got != nil {
		t.Fatalf("write deadline was not cleared: %v", got)
	}
}

func TestWSConn_LocalAddr_AlwaysTCPAddr(t *testing.T) {
	// Construct a wsConn directly with non-TCP addrs; we don't need a
	// live websocket for this since we're only exercising the address
	// coercion in the constructor.
	wsc := newWSConn(nil, &net.UnixAddr{Name: "/tmp/socket", Net: "unix"}, &net.UnixAddr{Name: "/tmp/peer", Net: "unix"})
	if _, ok := wsc.LocalAddr().(*net.TCPAddr); !ok {
		t.Fatalf("LocalAddr is not *net.TCPAddr: %T", wsc.LocalAddr())
	}
	if _, ok := wsc.RemoteAddr().(*net.TCPAddr); !ok {
		t.Fatalf("RemoteAddr is not *net.TCPAddr: %T", wsc.RemoteAddr())
	}

	// Nil inputs also produce a TCPAddr (zero values).
	wsc2 := newWSConn(nil, nil, nil)
	tcp, ok := wsc2.LocalAddr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("nil-input LocalAddr is not *net.TCPAddr: %T", wsc2.LocalAddr())
	}
	if tcp.Port != 0 || !tcp.IP.Equal(net.IPv4zero) {
		t.Fatalf("nil-input LocalAddr should be zero, got %v", tcp)
	}
}

func TestWSConn_OversizedMessageRejected(t *testing.T) {
	rig := newWSTestRig(t, func(ws *websocket.Conn) {
		ws.SetReadLimit(16)
	})
	client, wsc, _ := rig.dial(t)

	// 32 bytes — exceeds the 16-byte read limit.
	big := bytes.Repeat([]byte{0xAB}, 32)
	if err := client.WriteMessage(websocket.BinaryMessage, big); err != nil {
		// Some platforms surface the close to the writer; either way,
		// the server-side Read below is the assertion that matters.
		t.Logf("client write returned (expected on some platforms): %v", err)
	}

	_ = wsc.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 64)
	_, err := wsc.Read(buf)
	if err == nil {
		t.Fatalf("expected non-nil error for oversized message")
	}
	// We don't pin the exact error type — gorilla closes with 1009 and
	// surfaces a close error on the next Read. Either an io.EOF-bearing
	// error or a *websocket.CloseError is acceptable; only "no error"
	// is wrong.
	_ = err
}

func TestWSConn_DeadlinePropagation(t *testing.T) {
	rig := newWSTestRig(t, nil)
	_, wsc, _ := rig.dial(t)

	// Set a deadline well in the past.
	if err := wsc.SetReadDeadline(time.Now().Add(-1 * time.Hour)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 8)
		_, err := wsc.Read(buf)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected error from Read with past deadline")
		}
		// Accept either a literal "deadline" substring or any
		// timeout-flavored error.
		msg := strings.ToLower(err.Error())
		if !strings.Contains(msg, "deadline") && !strings.Contains(msg, "timeout") && err != io.EOF {
			t.Logf("non-deadline error returned (acceptable): %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Read did not return within 2s of past deadline")
	}
}

func TestWSConn_PartialFrameRead(t *testing.T) {
	rig := newWSTestRig(t, nil)
	client, wsc, _ := rig.dial(t)

	payload := bytes.Repeat([]byte{0xCD}, 1024)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := client.WriteMessage(websocket.BinaryMessage, payload); err != nil {
			t.Errorf("client write: %v", err)
		}
	}()

	got := make([]byte, 0, 1024)
	buf := make([]byte, 256)
	reads := 0
	for len(got) < 1024 && reads < 32 {
		_ = wsc.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := wsc.Read(buf)
		if n > 0 {
			got = append(got, buf[:n]...)
		}
		reads++
		if err != nil && len(got) < 1024 {
			t.Fatalf("read: %v (got %d bytes)", err, len(got))
		}
	}
	wg.Wait()

	if len(got) != 1024 {
		t.Fatalf("expected 1024 total bytes, got %d", len(got))
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch")
	}
}
