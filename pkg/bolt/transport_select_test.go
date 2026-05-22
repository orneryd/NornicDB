package bolt

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// mustGenSelfSignedTLSConfig produces a server-side *tls.Config bearing a
// freshly minted self-signed ECDSA certificate. The matching client-side
// config is returned alongside, with InsecureSkipVerify set so tests do
// not need a CA pool. Cert validity is 1 hour.
func mustGenSelfSignedTLSConfig(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "nornicdb-test"},
		NotBefore:    time.Now().Add(-1 * time.Minute),
		NotAfter:     time.Now().Add(1 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}
	server := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	client := &tls.Config{
		InsecureSkipVerify: true, // self-signed; test-only
		MinVersion:         tls.VersionTLS12,
	}
	return server, client
}

// pipePair returns a connected pair of net.Conn via net.Pipe. Both ends
// are registered for cleanup.
func pipePair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	c1, c2 := net.Pipe()
	t.Cleanup(func() { _ = c1.Close() })
	t.Cleanup(func() { _ = c2.Close() })
	return c1, c2
}

// startTLSServer returns a listener and a goroutine-fed channel of accepted
// raw net.Conns (pre-handshake). Tests drive their own peekTransport call
// against each accepted conn.
func startTLSServer(t *testing.T) (net.Listener, <-chan net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	ch := make(chan net.Conn, 1)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				close(ch)
				return
			}
			ch <- c
		}
	}()
	return ln, ch
}

// T1: Bolt magic prefix → transportRaw, peeked bytes still readable.
func TestPeekTransport_T1_BoltMagic(t *testing.T) {
	server, client := pipePair(t)
	payload := append(append([]byte{}, boltMagic...), 0xAA)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = client.Write(payload)
	}()

	kind, conn, br, err := peekTransport(server, nil, false, false, 0)
	if err != nil {
		t.Fatalf("peekTransport: %v", err)
	}
	if kind != transportRaw {
		t.Fatalf("kind = %v, want transportRaw", kind)
	}
	if conn != server {
		t.Fatalf("returned conn differs from input")
	}
	got := make([]byte, 5)
	if _, err := io.ReadFull(br, got); err != nil {
		t.Fatalf("read peeked bytes: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("peeked bytes = %x, want %x", got, payload)
	}
	wg.Wait()
}

// T2: "GET " prefix → transportWebSocket, peeked bytes still readable.
func TestPeekTransport_T2_GET(t *testing.T) {
	server, client := pipePair(t)
	payload := []byte("GET /")

	go func() { _, _ = client.Write(payload) }()

	kind, _, br, err := peekTransport(server, nil, false, false, 0)
	if err != nil {
		t.Fatalf("peekTransport: %v", err)
	}
	if kind != transportWebSocket {
		t.Fatalf("kind = %v, want transportWebSocket", kind)
	}
	got := make([]byte, 5)
	if _, err := io.ReadFull(br, got); err != nil {
		t.Fatalf("read peeked bytes: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("peeked bytes = %x, want %x", got, payload)
	}
}

// T3: TLS-handshake byte with tlsCfg=nil → unrecognized-prefix error.
func TestPeekTransport_T3_TLSByteNoConfig(t *testing.T) {
	server, client := pipePair(t)
	payload := []byte{0x16, 0x03, 0x01, 0x00, 0x05}

	go func() { _, _ = client.Write(payload) }()

	_, _, _, err := peekTransport(server, nil, false, false, 0)
	if err == nil {
		t.Fatalf("expected unrecognized-prefix error, got nil")
	}
	if !strings.Contains(err.Error(), "unrecognized") {
		t.Fatalf("error = %q, want substring 'unrecognized'", err.Error())
	}
}

// runTLSCase drives a real TLS handshake against peekTransport and asserts
// the post-handshake transport kind based on the payload the client sends.
func runTLSCase(t *testing.T, payload []byte, want transportKind) {
	t.Helper()
	srvCfg, cliCfg := mustGenSelfSignedTLSConfig(t)
	ln, accepted := startTLSServer(t)

	addr := ln.Addr().String()
	clientErr := make(chan error, 1)
	go func() {
		conn, err := tls.Dial("tcp", addr, cliCfg)
		if err != nil {
			clientErr <- err
			return
		}
		defer conn.Close()
		if _, err := conn.Write(payload); err != nil {
			clientErr <- err
			return
		}
		clientErr <- nil
	}()

	rawConn, ok := <-accepted
	if !ok {
		t.Fatalf("listener closed before accept")
	}
	t.Cleanup(func() { _ = rawConn.Close() })

	kind, _, br, err := peekTransport(rawConn, srvCfg, false, false, 0)
	if err != nil {
		t.Fatalf("peekTransport: %v", err)
	}
	if kind != want {
		t.Fatalf("kind = %v, want %v", kind, want)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(br, got); err != nil {
		t.Fatalf("read peeked bytes after TLS recursion: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("peeked bytes = %x, want %x", got, payload)
	}
	if cerr := <-clientErr; cerr != nil {
		t.Fatalf("client side: %v", cerr)
	}
}

// T4 + T5: TLS byte with valid tlsCfg, post-handshake "GET " → transportWebSocket.
func TestPeekTransport_T4T5_TLSThenGET(t *testing.T) {
	runTLSCase(t, []byte("GET /"), transportWebSocket)
}

// T4 + T6: TLS byte with valid tlsCfg, post-handshake Bolt magic → transportRaw.
func TestPeekTransport_T4T6_TLSThenBolt(t *testing.T) {
	payload := append(append([]byte{}, boltMagic...), 0xAA)
	runTLSCase(t, payload, transportRaw)
}

// T7: nested TLS rejected. Caller passes isEncrypted=true and a TLS-prefix
// byte; the function must NOT recurse into another tls.Server, and instead
// return an unrecognized-prefix error.
func TestPeekTransport_T7_NestedTLSRejected(t *testing.T) {
	server, client := pipePair(t)
	payload := []byte{0x16, 0x03, 0x03, 0x00, 0x05}
	srvCfg, _ := mustGenSelfSignedTLSConfig(t)

	go func() { _, _ = client.Write(payload) }()

	_, _, _, err := peekTransport(server, srvCfg, true, false, 0)
	if err == nil {
		t.Fatalf("expected error on nested TLS, got nil")
	}
	if !strings.Contains(err.Error(), "unrecognized") {
		t.Fatalf("error = %q, want substring 'unrecognized'", err.Error())
	}
}

// T8: RequireTLS + plaintext "GET " → ErrUnencryptedRequired.
func TestPeekTransport_T8_RequireTLSWithGET(t *testing.T) {
	server, client := pipePair(t)
	go func() { _, _ = client.Write([]byte("GET /")) }()

	_, _, _, err := peekTransport(server, nil, false, true, 0)
	if !errors.Is(err, ErrUnencryptedRequired) {
		t.Fatalf("error = %v, want ErrUnencryptedRequired", err)
	}
	if err.Error() != "An unencrypted connection attempt was made where encryption is required." {
		t.Fatalf("message = %q, want canonical Neo4j message", err.Error())
	}
}

// T9: RequireTLS + plaintext Bolt magic → ErrUnencryptedRequired.
func TestPeekTransport_T9_RequireTLSWithBolt(t *testing.T) {
	server, client := pipePair(t)
	payload := append(append([]byte{}, boltMagic...), 0xAA)
	go func() { _, _ = client.Write(payload) }()

	_, _, _, err := peekTransport(server, nil, false, true, 0)
	if !errors.Is(err, ErrUnencryptedRequired) {
		t.Fatalf("error = %v, want ErrUnencryptedRequired", err)
	}
}

// T10: unrecognized prefix (5 zero bytes) → unrecognized-prefix error.
func TestPeekTransport_T10_UnrecognizedPrefix(t *testing.T) {
	server, client := pipePair(t)
	go func() { _, _ = client.Write([]byte{0x00, 0x01, 0x02, 0x03, 0x04}) }()

	_, _, _, err := peekTransport(server, nil, false, false, 0)
	if err == nil {
		t.Fatalf("expected unrecognized-prefix error, got nil")
	}
	if !strings.Contains(err.Error(), "unrecognized") {
		t.Fatalf("error = %q, want substring 'unrecognized'", err.Error())
	}
}
