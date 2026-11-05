package apocio

// Regression coverage for the remote URL address policy at connect time.
//
// The URL preflight resolves an allowlisted host and rejects prohibited
// addresses. The APOC client's dialer then resolves the same host again when it
// connects, and that second resolution is what the socket uses. These tests keep
// every fixture on loopback: the only address a dial resolution can return is
// 127.0.0.1, and a prohibited address reaching the connect phase is what the
// dial-time guard must refuse. No test dials a public address.

import (
	"context"
	"encoding/binary"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/security"
)

// rebindHost is the allowlisted name in the rebind regression. It exists only in
// the loopback DNS fixture below, which the test installs as the process
// resolver for its duration.
const rebindHost = "rebind.test"

// publicHostResolver answers the URL preflight with the public address an
// attacker-controlled name served first, so the URL policy admits the name.
func publicHostResolver(context.Context, string) ([]net.IPAddr, error) {
	return []net.IPAddr{{IP: publicTestIP}}, nil
}

// loopbackDNS is a test-owned loopback UDP resolver for rebindHost: every A
// query answers 127.0.0.1 and every AAAA query answers NODATA, so a dial-time
// resolution of the name can only produce the loopback address. It records each
// A answer, which lets a test show which addresses a dialer was offered.
type loopbackDNS struct {
	conn net.PacketConn
	wg   sync.WaitGroup

	mu       sync.Mutex
	aAnswers []string
}

func startLoopbackDNS(t *testing.T) *loopbackDNS {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err, "start loopback DNS fixture")
	fixture := &loopbackDNS{conn: conn}
	fixture.wg.Add(1)
	go fixture.serve()
	t.Cleanup(func() {
		_ = conn.Close()
		joined := make(chan struct{})
		go func() {
			fixture.wg.Wait()
			close(joined)
		}()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Errorf("loopback DNS fixture did not exit")
		}
	})
	return fixture
}

func (f *loopbackDNS) serve() {
	defer f.wg.Done()
	buffer := make([]byte, 1500)
	for {
		n, from, err := f.conn.ReadFrom(buffer)
		if err != nil {
			return
		}
		if response, ok := f.response(buffer[:n]); ok {
			if _, err := f.conn.WriteTo(response, from); err != nil {
				return
			}
		}
	}
}

func (f *loopbackDNS) response(query []byte) ([]byte, bool) {
	if len(query) < 12 || binary.BigEndian.Uint16(query[4:6]) != 1 {
		return nil, false
	}
	name, offset := readDNSName(query, 12)
	if offset < 0 || offset+4 > len(query) {
		return nil, false
	}
	if name != rebindHost+"." {
		return nil, false
	}
	queryType := binary.BigEndian.Uint16(query[offset : offset+2])
	questionEnd := offset + 4
	additional := query[questionEnd:]

	var answer []byte
	if queryType == 1 { // A
		f.recordAAnswer("127.0.0.1")
		answer = []byte{0xC0, 0x0C, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04}
		answer = append(answer, net.ParseIP("127.0.0.1").To4()...)
	}

	response := make([]byte, 0, len(query)+16)
	response = append(response, query[0], query[1]) // transaction ID
	response = append(response, 0x81, 0x80)         // QR=1, RD=1, RA=1, NOERROR
	response = binary.BigEndian.AppendUint16(response, 1)
	if len(answer) > 0 {
		response = binary.BigEndian.AppendUint16(response, 1)
	} else {
		response = binary.BigEndian.AppendUint16(response, 0)
	}
	response = binary.BigEndian.AppendUint16(response, 0)
	response = binary.BigEndian.AppendUint16(response, binary.BigEndian.Uint16(query[10:12]))
	response = append(response, query[12:questionEnd]...)
	response = append(response, answer...)
	response = append(response, additional...) // echo EDNS0 OPT so the client sees its own request
	return response, true
}

func readDNSName(message []byte, offset int) (string, int) {
	name := make([]byte, 0, 64)
	for {
		if offset >= len(message) {
			return "", -1
		}
		length := int(message[offset])
		offset++
		if length == 0 {
			break
		}
		if length&0xC0 != 0 || offset+length > len(message) {
			return "", -1
		}
		name = append(name, message[offset:offset+length]...)
		name = append(name, '.')
		offset += length
	}
	return strings.ToLower(string(name)), offset
}

func (f *loopbackDNS) recordAAnswer(address string) {
	f.mu.Lock()
	f.aAnswers = append(f.aAnswers, address)
	f.mu.Unlock()
}

func (f *loopbackDNS) aAnswerCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.aAnswers)
}

func (f *loopbackDNS) aAnswerList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.aAnswers...)
}

// useLoopbackDNS points the process resolver at the fixture for the duration of
// the test. Only the Go resolver is used, and only names the fixture serves
// resolve at all.
func useLoopbackDNS(t *testing.T, fixture *loopbackDNS) {
	t.Helper()
	saved := net.DefaultResolver
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	net.DefaultResolver = &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _ string, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "udp", fixture.conn.LocalAddr().String())
		},
	}
	t.Cleanup(func() { net.DefaultResolver = saved })
}

// loopbackTarget is a test-owned loopback TCP listener that counts accepted
// connections. It speaks no protocol: the regressions assert that the request
// never reaches it.
type loopbackTarget struct {
	listener net.Listener
	wg       sync.WaitGroup
	accepted atomic.Int32
}

func startLoopbackTarget(t *testing.T) *loopbackTarget {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "start loopback target fixture")
	target := &loopbackTarget{listener: listener}
	target.wg.Add(1)
	go func() {
		defer target.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			target.accepted.Add(1)
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		joined := make(chan struct{})
		go func() {
			target.wg.Wait()
			close(joined)
		}()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Errorf("loopback target fixture did not exit")
		}
	})
	return target
}

func (t *loopbackTarget) port() string {
	_, port, err := net.SplitHostPort(t.listener.Addr().String())
	if err != nil {
		panic(err)
	}
	return port
}

func (t *loopbackTarget) acceptedCount() int { return int(t.accepted.Load()) }

// requireAccepting proves the fixture accepts connections, so a later zero count
// means the request never connected rather than the listener being dead. The
// counter is reset for the request under test.
func (t *loopbackTarget) requireAccepting(tb testing.TB) {
	tb.Helper()
	conn, err := net.DialTimeout("tcp", t.listener.Addr().String(), 5*time.Second)
	require.NoError(tb, err, "connect to loopback target fixture")
	require.NoError(tb, conn.Close())
	deadline := time.Now().Add(5 * time.Second)
	for t.accepted.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	require.Equal(tb, 1, t.acceptedCount(), "fixture liveness connection was not accepted")
	t.accepted.Store(0)
}

// TestDefaultRemoteTransportDialGuard checks the default client keeps the APOC
// defaults and carries the dial-time address judgment.
func TestDefaultRemoteTransportDialGuard(t *testing.T) {
	require.Equal(t, 10*time.Second, defaultRemoteHTTPClient.Timeout)
	require.NotNil(t, defaultRemoteHTTPClient.CheckRedirect)
	require.ErrorIs(t, defaultRemoteHTTPClient.CheckRedirect(&http.Request{}, nil), http.ErrUseLastResponse)

	transport, ok := defaultRemoteHTTPClient.Transport.(*http.Transport)
	require.True(t, ok, "the default APOC transport is an *http.Transport")
	require.Nil(t, transport.Proxy, "the APOC client uses no ambient proxy")
	require.NotNil(t, transport.DialContext, "every default APOC dial is judged before it connects")
	require.True(t, transport.ForceAttemptHTTP2, "a custom dial function must not drop HTTP/2")

	// The dialer keeps net/http's connect settings: the process resolver, the
	// Happy Eyeballs fallback and the request context are unchanged.
	require.Equal(t, 30*time.Second, defaultRemoteDialer.Timeout)
	require.Equal(t, 30*time.Second, defaultRemoteDialer.KeepAlive)
	require.Nil(t, defaultRemoteDialer.Resolver)
	require.Nil(t, defaultRemoteDialer.Control)
	require.NotNil(t, defaultRemoteDialer.ControlContext)
}

// TestRemoteRebindGetsNoTCPConnection is the regression for the resolved-address
// gap: the URL preflight resolves the allowlisted host to an allowed public
// address, and the dial that follows resolves the same host to a prohibited
// loopback address. The dial must be refused before any TCP connection exists.
func TestRemoteRebindGetsNoTCPConnection(t *testing.T) {
	dns := startLoopbackDNS(t)
	useLoopbackDNS(t, dns)
	target := startLoopbackTarget(t)
	target.requireAccepting(t)

	capability := NewPolicy(Config{
		AllowRemoteURLAccess: true,
		RemoteURLAllowlist:   []string{rebindHost},
		HostResolver:         publicHostResolver,
	}).Capability()
	require.Same(t, defaultRemoteHTTPClient, capability.record.remoteHTTPClient(),
		"the regression must exercise the default APOC client")

	body, err := capability.Open(context.Background(), "https://"+rebindHost+":"+target.port()+"/data.json", Request{})
	if body != nil {
		_ = body.Close()
	}
	require.Error(t, err, "the rebind request must not succeed")
	require.ErrorIs(t, err, errRemoteAddressDisallowed,
		"the dial guard refused the address the socket resolved: %v", err)
	require.Equal(t, 0, target.acceptedCount(), "no TCP connection reached the prohibited address")
	require.NotZero(t, dns.aAnswerCount(), "the transport resolved the name again at dial time")
	require.Equal(t, []string{"127.0.0.1"}, dns.aAnswerList(),
		"the dial-time resolution returned only the loopback address")
}

// TestDefaultDeniedRemoteRequestResolvesNothing keeps the default-deny control:
// with remote URL access disabled the request resolves nothing and dials
// nothing.
func TestDefaultDeniedRemoteRequestResolvesNothing(t *testing.T) {
	dns := startLoopbackDNS(t)
	useLoopbackDNS(t, dns)
	target := startLoopbackTarget(t)
	target.requireAccepting(t)

	capability := NewPolicy(Config{}).Capability()
	body, err := capability.Open(context.Background(), "https://"+rebindHost+":"+target.port()+"/data.json", Request{})
	if body != nil {
		_ = body.Close()
	}
	require.ErrorIs(t, err, ErrRemoteURLAccessDisabled)
	require.Zero(t, target.acceptedCount(), "a denied request dials nothing")
	require.Zero(t, dns.aAnswerCount(), "a denied request resolves nothing")
}

// TestPreflightProhibitedAddressResolvesNothing is the preflight control: a name
// that is prohibited in the URL resolution is rejected before any dial
// resolution happens.
func TestPreflightProhibitedAddressResolvesNothing(t *testing.T) {
	dns := startLoopbackDNS(t)
	useLoopbackDNS(t, dns)
	target := startLoopbackTarget(t)
	target.requireAccepting(t)

	capability := NewPolicy(Config{
		AllowRemoteURLAccess: true,
		RemoteURLAllowlist:   []string{rebindHost},
		HostResolver: func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
		},
	}).Capability()
	body, err := capability.Open(context.Background(), "https://"+rebindHost+":"+target.port()+"/data.json", Request{})
	if body != nil {
		_ = body.Close()
	}
	require.ErrorIs(t, err, errRemoteAddressDisallowed,
		"the preflight reports the same address rejection")
	require.Zero(t, target.acceptedCount())
	require.Zero(t, dns.aAnswerCount(), "the request never reached the dial")
}

// TestRemoteAddressPolicyJudgesPreflightAndDialAlike checks the one classifier
// both parsers use: the URL preflight sees a net.IP from a resolver, the dial
// guard sees the resolved "ip:port" string, and both must judge the same
// addresses the same way.
func TestRemoteAddressPolicyJudgesPreflightAndDialAlike(t *testing.T) {
	cases := []struct {
		name       string
		ip         string
		prohibited bool
	}{
		{"ipv4 loopback", "127.0.0.1", true},
		{"ipv4 private 10/8", "10.1.2.3", true},
		{"ipv4 private 172.16/12", "172.16.0.1", true},
		{"ipv4 private 192.168/16", "192.168.1.1", true},
		{"ipv4 link-local unicast", "169.254.10.20", true},
		{"ipv4 multicast", "224.0.0.1", true},
		{"ipv4 unspecified", "0.0.0.0", true},
		{"ipv4-mapped loopback", "::ffff:127.0.0.1", true},
		{"ipv4-mapped private", "::ffff:10.0.0.1", true},
		{"ipv6 loopback", "::1", true},
		{"ipv6 unspecified", "::", true},
		{"ipv6 link-local unicast", "fe80::1", true},
		{"ipv6 multicast", "ff02::1", true},
		{"ipv6 unique-local", "fc00::1", true},
		{"public TEST-NET-3", "203.0.113.7", false},
		{"public ipv4", "93.184.216.34", false},
		{"public ipv6", "2606:2800:220:1:248:1893:25c8:1946", false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			ip := net.ParseIP(testCase.ip)
			require.NotNil(t, ip)
			require.Equal(t, testCase.prohibited, security.IsRestrictedIP(ip, remoteIPPolicy))

			dialErr := checkRemoteDialAddress(net.JoinHostPort(testCase.ip, "443"))
			capability := NewPolicy(Config{
				AllowRemoteURLAccess: true,
				RemoteURLAllowlist:   []string{"example.com"},
				HostResolver: func(context.Context, string) ([]net.IPAddr, error) {
					return []net.IPAddr{{IP: ip}}, nil
				},
				HTTPClient: &http.Client{Transport: respondWith(http.StatusOK, `{}`)},
			}).Capability()
			body, preflightErr := capability.Open(context.Background(), "https://example.com/data.json", Request{})
			if body != nil {
				_ = body.Close()
			}

			if !testCase.prohibited {
				require.NoError(t, dialErr)
				require.NoError(t, preflightErr)
				return
			}
			require.ErrorIs(t, dialErr, errRemoteAddressDisallowed)
			require.ErrorIs(t, preflightErr, errRemoteAddressDisallowed)
		})
	}
}

// TestRemoteDialGuardFailsClosedOnUnjudgeableAddress proves the guard refuses an
// address it cannot judge instead of dialing it: the resolved endpoint is always
// an IP literal, so anything else is an unexpected form.
func TestRemoteDialGuardFailsClosedOnUnjudgeableAddress(t *testing.T) {
	cases := map[string]struct {
		address  string
		rejected string
	}{
		"empty":            {"", "is not an IP:port"},
		"missing port":     {"203.0.113.7", "is not an IP:port"},
		"unbracketed ipv6": {"::1:443", "is not an IP:port"},
		"empty host":       {":443", "is not a resolved IP address"},
		"host name":        {"example.com:443", "is not a resolved IP address"},
		"zoned ipv6":       {"[fe80::1%lo0]:443", "is not a resolved IP address"},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			err := checkRemoteDialAddress(testCase.address)
			require.Error(t, err, "an address the guard cannot judge must not be dialed")
			require.ErrorContains(t, err, testCase.rejected)
		})
	}
}

// TestRemoteDialKeepsCallerContext checks the guarded dial still answers to the
// caller's context, so a canceled or expired request connects nothing.
func TestRemoteDialKeepsCallerContext(t *testing.T) {
	t.Run("guard judgment uses the resolved address", func(t *testing.T) {
		err := defaultRemoteDialer.ControlContext(context.Background(), "tcp4", net.JoinHostPort("127.0.0.1", "443"), nil)
		require.ErrorIs(t, err, errRemoteAddressDisallowed)
	})

	t.Run("canceled context reaches the caller", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := defaultRemoteDialer.DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", "1"))
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("expired deadline reaches the caller", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		_, err := defaultRemoteDialer.DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", "1"))
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("canceled request connects nothing", func(t *testing.T) {
		dns := startLoopbackDNS(t)
		useLoopbackDNS(t, dns)
		target := startLoopbackTarget(t)
		target.requireAccepting(t)

		capability := NewPolicy(Config{
			AllowRemoteURLAccess: true,
			RemoteURLAllowlist:   []string{rebindHost},
			HostResolver:         publicHostResolver,
		}).Capability()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		body, err := capability.Open(ctx, "https://"+rebindHost+":"+target.port()+"/data.json", Request{})
		if body != nil {
			_ = body.Close()
		}
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, target.acceptedCount())
	})
}

// BenchmarkRemoteDialAddressGuard measures the per-connect cost the guard adds to
// the default APOC client: one judgment of an already resolved endpoint.
func BenchmarkRemoteDialAddressGuard(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := checkRemoteDialAddress("203.0.113.7:443"); err != nil {
			b.Fatalf("guard refused a public address: %v", err)
		}
	}
}
