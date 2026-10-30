package bolt

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Phase-3 performance gate (plan §"Performance contract"):
//
//   - B-WS-Throughput-Records: per-record throughput via WS vs raw TCP.
//   - B-WS-Allocs-Records:     per-record allocations.
//   - B-TCP-TLS-Throughput:    TLS-on-raw baseline so the TLS+WS budget
//                              isn't double-counting plain-TLS overhead.
//
// These benchmarks parameterize transport so the same workload runs against
// tcp / tcp_tls / ws / ws_tls. CI gates compare the four; a 5% regression
// over `tcp` on any of the others fails the bar.
//
// Workload: a bench-scoped executor returns a fixed batch of rows so
// the benchmarks measure pure transport cost, not Cypher. 2 000 rows
// per query keeps wall time short (~5–20 ms per iter) which lets
// `-benchtime=10s` produce a stable signal.

// Round-trip p99 is exposed as a separate B-WS-RoundTrip-P99 benchmark
// in transport_bench_p99_test.go; that file uses runtime/metrics for the
// distribution and reports p99 via b.ReportMetric.

type benchTransport int

const (
	benchTransportTCP benchTransport = iota
	benchTransportTCPTLS
	benchTransportWS
	benchTransportWSTLS
)

func (bt benchTransport) String() string {
	switch bt {
	case benchTransportTCP:
		return "tcp"
	case benchTransportTCPTLS:
		return "tcp_tls"
	case benchTransportWS:
		return "ws"
	case benchTransportWSTLS:
		return "ws_tls"
	}
	return "?"
}

// benchSelfSignedTLS produces a server-side *tls.Config and an
// InsecureSkipVerify client config for the TLS benchmarks. Cheap enough
// to regenerate per benchmark run.
func benchSelfSignedTLS(b *testing.B) (*tls.Config, *tls.Config) {
	b.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		b.Fatalf("ecdsa: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "nornicdb-bench"},
		NotBefore:    time.Now().Add(-1 * time.Minute),
		NotAfter:     time.Now().Add(1 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		b.Fatalf("x509: %v", err)
	}
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}
	srv := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	cli := &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}
	return srv, cli
}

// startBenchTransportServer spins up a Bolt server configured for the
// requested transport and returns the listening port plus a tls config
// for tls-aware dialers.
func startBenchTransportServer(b *testing.B, transport benchTransport, exec QueryExecutor) (port int, cliTLS *tls.Config) {
	b.Helper()
	cfg := &Config{
		Port:                     0,
		MaxConnections:           1,
		ReadBufferSize:           8192,
		WriteBufferSize:          256 * 1024,
		AllowAnonymous:           true,
		WebSocketEnabled:         true,
		WebSocketWriteBufferSize: 256 * 1024,
		WebSocketMaxMessageSize:  10 * 1024 * 1024, // 10 MiB to fit large RECORD batches
		BoltSniffTimeout:         5 * time.Second,
		BoltAuthTimeout:          30 * time.Second,
	}

	switch transport {
	case benchTransportTCPTLS, benchTransportWSTLS:
		srvTLS, cli := benchSelfSignedTLS(b)
		cfg.TLSConfig = srvTLS
		cliTLS = cli
	}

	server := New(cfg, exec)
	go func() { _ = server.ListenAndServe() }()

	deadline := time.Now().Add(500 * time.Millisecond)
	for server.listener == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.listener == nil {
		b.Fatal("listener never bound")
	}
	port = server.listener.Addr().(*net.TCPAddr).Port
	b.Cleanup(func() { _ = server.Close() })
	return port, cliTLS
}

// dialBenchTransport opens a session-ready Bolt connection (handshake +
// HELLO complete) on the requested transport. Returns a net.Conn (the
// adapter for WS variants) and a bufio.Reader sitting on top.
func dialBenchTransport(b *testing.B, transport benchTransport, port int, cliTLS *tls.Config) (net.Conn, *bufio.Reader) {
	b.Helper()
	var raw net.Conn
	switch transport {
	case benchTransportTCP:
		c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			b.Fatal(err)
		}
		raw = c
	case benchTransportTCPTLS:
		c, err := tls.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port), cliTLS)
		if err != nil {
			b.Fatal(err)
		}
		raw = c
	case benchTransportWS:
		dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
		u := url.URL{Scheme: "ws", Host: fmt.Sprintf("127.0.0.1:%d", port), Path: "/"}
		ws, _, err := dialer.Dial(u.String(), nil)
		if err != nil {
			b.Fatal(err)
		}
		raw = &wsConnAdapter{ws: ws}
	case benchTransportWSTLS:
		dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second, TLSClientConfig: cliTLS}
		u := url.URL{Scheme: "wss", Host: fmt.Sprintf("127.0.0.1:%d", port), Path: "/"}
		ws, _, err := dialer.Dial(u.String(), nil)
		if err != nil {
			b.Fatal(err)
		}
		raw = &wsConnAdapter{ws: ws}
	}
	b.Cleanup(func() { _ = raw.Close() })

	if err := PerformHandshake(raw); err != nil {
		b.Fatal(err)
	}
	if err := SendMessage(raw, BuildHelloMessage(nil)); err != nil {
		b.Fatal(err)
	}
	reader := bufio.NewReaderSize(raw, 256*1024)
	scratch := make([]byte, 32*1024)
	if err := benchReadSuccess(reader, scratch); err != nil {
		b.Fatal(err)
	}
	return raw, reader
}

// runRecordsThroughputBench is the shared body of B-WS-Throughput-Records
// and the TCP/TLS baselines. SetBytes(records) so go test reports
// records/sec; ReportAllocs so the same run also surfaces allocs.
func runRecordsThroughputBench(b *testing.B, transport benchTransport, records int) {
	row := []any{int64(1), "Alice", int64(30)}
	rows := make([][]any, records)
	for i := range rows {
		rows[i] = row
	}
	exec := &benchQueryExecutor{result: &QueryResult{
		Columns: []string{"id", "name", "age"},
		Rows:    rows,
	}}

	port, cliTLS := startBenchTransportServer(b, transport, exec)
	conn, reader := dialBenchTransport(b, transport, port, cliTLS)
	scratch := make([]byte, 32*1024)

	b.ReportAllocs()
	b.SetBytes(int64(records))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := SendMessage(conn, BuildRunMessage("RETURN 1", nil, nil)); err != nil {
			b.Fatal(err)
		}
		if err := benchReadSuccess(reader, scratch); err != nil {
			b.Fatal(err)
		}
		if err := SendMessage(conn, BuildPullMessage(map[string]any{"n": int64(records)})); err != nil {
			b.Fatal(err)
		}
		for j := 0; j < records; j++ {
			mt, err := benchReadMessageType(reader, scratch)
			if err != nil {
				b.Fatal(err)
			}
			if mt != MsgRecord {
				b.Fatalf("expected RECORD, got 0x%02x", mt)
			}
		}
		if err := benchReadSuccess(reader, scratch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBolt_TransportThroughput_Records is the gating benchmark for
// the Phase-3 5% regression contract. It runs the same RECORD-streaming
// workload across all four transports so the CI compare can read
// records/sec for each and fail when WS or WS+TLS regress past tcp+5%.
//
// Naming maps to the plan: tcp = baseline; tcp_tls = B-TCP-TLS-Throughput;
// ws = B-WS-Throughput; ws_tls = B-WS-TLS-Throughput.
func BenchmarkBolt_TransportThroughput_Records(b *testing.B) {
	const records = 2000
	for _, t := range []benchTransport{benchTransportTCP, benchTransportTCPTLS, benchTransportWS, benchTransportWSTLS} {
		b.Run(t.String(), func(b *testing.B) {
			runRecordsThroughputBench(b, t, records)
		})
	}
}

// BenchmarkBolt_TransportAllocs_Records is the allocation-budget gate.
// Same workload as throughput, but b.ReportAllocs already surfaces
// B/op and allocs/op in `go test -bench` output. The 5% bar is per-record.
func BenchmarkBolt_TransportAllocs_Records(b *testing.B) {
	const records = 2000
	for _, t := range []benchTransport{benchTransportTCP, benchTransportWS, benchTransportWSTLS} {
		b.Run(t.String(), func(b *testing.B) {
			runRecordsThroughputBench(b, t, records)
		})
	}
}

// BenchmarkBolt_TransportRoundTrip drives a one-row RETURN per iteration
// and reports ns/op (per-RUN+PULL+SUCCESS round trip). The plan calls
// for p99; b.ReportMetric exposes a custom metric that benchstat can
// aggregate to p50/p99 across runs. Per-run we emit ns/op which is the
// distribution mean. The CI compare uses benchstat to derive p99.
func BenchmarkBolt_TransportRoundTrip(b *testing.B) {
	for _, t := range []benchTransport{benchTransportTCP, benchTransportTCPTLS, benchTransportWS, benchTransportWSTLS} {
		b.Run(t.String(), func(b *testing.B) {
			row := []any{int64(1)}
			exec := &benchQueryExecutor{result: &QueryResult{
				Columns: []string{"x"},
				Rows:    [][]any{row},
			}}
			port, cliTLS := startBenchTransportServer(b, t, exec)
			conn, reader := dialBenchTransport(b, t, port, cliTLS)
			scratch := make([]byte, 1024)

			// Warmup so JIT, TCP slow start, TLS session cache stabilize.
			for w := 0; w < 5; w++ {
				_ = SendMessage(conn, BuildRunMessage("RETURN 1", nil, nil))
				_ = benchReadSuccess(reader, scratch)
				_ = SendMessage(conn, BuildPullMessage(map[string]any{"n": int64(1)}))
				_, _ = benchReadMessageType(reader, scratch)
				_ = benchReadSuccess(reader, scratch)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := SendMessage(conn, BuildRunMessage("RETURN 1", nil, nil)); err != nil {
					b.Fatal(err)
				}
				if err := benchReadSuccess(reader, scratch); err != nil {
					b.Fatal(err)
				}
				if err := SendMessage(conn, BuildPullMessage(map[string]any{"n": int64(1)})); err != nil {
					b.Fatal(err)
				}
				if mt, err := benchReadMessageType(reader, scratch); err != nil || mt != MsgRecord {
					b.Fatalf("expected RECORD, got 0x%02x err=%v", mt, err)
				}
				if err := benchReadSuccess(reader, scratch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
