package cypher

// The remote APOC JSON procedures (apoc.load.json, apoc.load.jsonArray,
// apoc.import.json) must resolve the allowlisted host and issue their outbound
// request on the caller's statement context. A canceled or expired caller ends
// that work and receives that cancellation, instead of loading — or importing —
// data the caller no longer waits for.
//
// These regressions drive the public Execute path. They hold the existing
// resolver and HTTP-client seams until the work observes the caller's context.
// The hold timer only bounds work no caller cancellation reached, so a passing
// run always ends because of the caller's context, never because of the timer.

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// apocCancellationHoldLimit bounds a fixture that no caller cancellation
	// reached. It exceeds apocCancellationDeadline so a passing run always
	// ends from the caller's context, not from this timer.
	apocCancellationHoldLimit = 3 * time.Second
	apocCancellationDeadline  = 1 * time.Second
	apocCancellationWaitLimit = 60 * time.Second

	apocCancellationRemoteURL = "https://example.com/payload.json"
	apocCancellationAllowed   = "example.com"

	// apocCancellationPayload is the data a canceled caller must never receive
	// or import: one node and one relationship for apoc.import.json to write
	// if the load ran to the end.
	apocCancellationPayload = `{"nodes":[{"id":"n1","labels":["Person"],"properties":{"name":"alice"}}],` +
		`"relationships":[{"id":"e1","type":"KNOWS","startNode":"n1","endNode":"n2","properties":{"since":2020}}]}`

	apocSeamDNS  = "dns"
	apocSeamHTTP = "http"

	apocCancellationMarkerValue = "caller-statement"
)

// errAPOCCancellationNotPropagated is the fixture failure for work the caller's
// context never reached: it is deliberately not a context error.
var errAPOCCancellationNotPropagated = errors.New("APOC remote work did not observe the caller's context")

// apocCancellationMarkerKey tags the caller's statement context so a fixture
// can tell it apart from a background context.
type apocCancellationMarkerKey struct{}

var apocCancellationPublicIP = net.ParseIP("93.184.216.34")

// apocCancellationDNS is the resolveAPOCHost seam. Held lookups stay pending
// until the context they were given ends, so a caller's cancellation has to
// reach the resolution itself.
// apocCancellationGate owns held-work entry/release for both resolver and
// transport fixtures. Metrics and the operation-specific outcome stay with each.
type apocCancellationGate struct {
	hold     bool
	entered  chan struct{}
	release  chan struct{}
	stopOnce sync.Once
}

func newAPOCCancellationGate(hold bool) apocCancellationGate {
	return apocCancellationGate{hold: hold, entered: make(chan struct{}, 1), release: make(chan struct{})}
}
func (f *apocCancellationGate) stop() { f.stopOnce.Do(func() { close(f.release) }) }

type apocCancellationDNS struct {
	apocCancellationGate
	mu       sync.Mutex
	lookups  int
	contexts []context.Context
}

func (f *apocCancellationDNS) resolve(ctx context.Context, _ string) ([]net.IPAddr, error) {
	f.mu.Lock()
	f.lookups++
	f.contexts = append(f.contexts, ctx)
	f.mu.Unlock()
	if !f.hold {
		return []net.IPAddr{{IP: apocCancellationPublicIP}}, nil
	}
	select {
	case f.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.release:
		return nil, errAPOCCancellationNotPropagated
	case <-time.After(apocCancellationHoldLimit):
		return []net.IPAddr{{IP: apocCancellationPublicIP}}, nil
	}
}

func (f *apocCancellationDNS) seenContexts() []context.Context {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]context.Context(nil), f.contexts...)
}

// apocCancellationHTTP is the apocRemoteHTTPClient seam. Held requests stay
// pending until the request's own context ends, so a caller's cancellation has
// to reach the outbound request.
type apocCancellationHTTP struct {
	apocCancellationGate
	mu       sync.Mutex
	requests int
	observed error
}

func (f *apocCancellationHTTP) RoundTrip(req *http.Request) (*http.Response, error) {
	f.mu.Lock()
	f.requests++
	f.mu.Unlock()
	if !f.hold {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader(apocCancellationPayload)),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	}
	select {
	case f.entered <- struct{}{}:
	default:
	}
	var observed error
	select {
	case <-req.Context().Done():
		observed = req.Context().Err()
	case <-f.release:
		observed = errAPOCCancellationNotPropagated
	case <-time.After(apocCancellationHoldLimit):
		observed = errAPOCCancellationNotPropagated
	}
	f.mu.Lock()
	f.observed = observed
	f.mu.Unlock()
	return nil, observed
}

func (f *apocCancellationHTTP) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests
}

func (f *apocCancellationHTTP) requestContextError() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.observed
}

type apocCancellationOutcome struct {
	result *ExecuteResult
	err    error
}

// TestAPOCRemoteJSONCallerContextCancellation proves that a canceled or expired
// caller context ends both stages of a remote JSON load for every JSON load
// procedure, on both configured parsers, and leaves the graph unchanged.
func TestAPOCRemoteJSONCallerContextCancellation(t *testing.T) {
	restoreParserTypeAfter(t)

	parsers := []struct {
		name       string
		parserType string
	}{
		{"nornic_parser", config.ParserTypeNornic},
		{"antlr_parser", config.ParserTypeANTLR},
	}
	procedures := []struct {
		name  string
		query string
	}{
		{"apoc.load.json", "CALL apoc.load.json($u) YIELD value"},
		{"apoc.load.jsonArray", "CALL apoc.load.jsonArray($u) YIELD value"},
		{"apoc.import.json", "CALL apoc.import.json($u)"},
	}
	seams := []struct {
		name string
		hold string
	}{
		{"allowlist_dns_resolution", apocSeamDNS},
		{"outbound_http", apocSeamHTTP},
	}
	variants := []struct {
		name         string
		withDeadline bool
	}{
		{"cancellation", false},
		{"deadline", true},
	}

	for _, parser := range parsers {
		for _, procedure := range procedures {
			for _, seam := range seams {
				for _, variant := range variants {
					name := strings.Join([]string{parser.name, procedure.name, seam.name, variant.name}, "/")
					t.Run(name, func(t *testing.T) {
						config.SetParserType(parser.parserType)
						// Fake time advances only after actual Execute work is blocked.
						// Parser/CI scheduling cannot expire the deadline before seam entry.
						synctest.Test(t, func(t *testing.T) {
							runAPOCRemoteJSONCallerContextCase(t, procedure.query, seam.hold, variant.withDeadline)
						})
					})
				}
			}
		}
	}
}

func runAPOCRemoteJSONCallerContextCase(t *testing.T, query, hold string, withDeadline bool) {
	t.Helper()

	eng := newTestMemoryEngine(t)
	exec := NewStorageExecutor(eng)
	exec.SetAllowRemoteAPOCURLAccess(true)
	exec.SetAPOCRemoteURLAllowlist([]string{apocCancellationAllowed})

	dns := &apocCancellationDNS{apocCancellationGate: newAPOCCancellationGate(hold == apocSeamDNS)}
	remote := &apocCancellationHTTP{apocCancellationGate: newAPOCCancellationGate(hold == apocSeamHTTP)}
	exec.apocIO = exec.apocIO.WithHostResolver(dns.resolve)
	exec.apocIO = exec.apocIO.WithHTTPClient(&http.Client{Transport: remote})

	parent := context.WithValue(context.Background(), apocCancellationMarkerKey{}, apocCancellationMarkerValue)
	var ctx context.Context
	var cancel context.CancelFunc
	if withDeadline {
		ctx, cancel = context.WithTimeout(parent, apocCancellationDeadline)
	} else {
		ctx, cancel = context.WithCancel(parent)
	}
	defer cancel()

	outcome := make(chan apocCancellationOutcome, 1)
	go func() {
		result, err := exec.Execute(ctx, query, map[string]interface{}{"u": apocCancellationRemoteURL})
		outcome <- apocCancellationOutcome{result: result, err: err}
	}()

	joined := false
	t.Cleanup(func() {
		// Release and join the held work even when an assertion below stops the
		// case early; a fixture never outlives its test.
		dns.stop()
		remote.stop()
		if joined {
			return
		}
		select {
		case <-outcome:
		case <-time.After(apocCancellationWaitLimit):
			t.Errorf("APOC remote statement did not finish after its fixtures were released")
		}
	})

	entered := dns.entered
	if hold == apocSeamHTTP {
		entered = remote.entered
	}
	select {
	case <-entered:
	case <-time.After(apocCancellationWaitLimit):
		t.Fatalf("held %s work never started", hold)
	}
	if !withDeadline {
		// The held work is running: canceling the caller must end it.
		cancel()
	}

	wantErr := context.Canceled
	if withDeadline {
		wantErr = context.DeadlineExceeded
	}

	var got apocCancellationOutcome
	select {
	case got = <-outcome:
		joined = true
	case <-time.After(apocCancellationWaitLimit):
		t.Fatalf("APOC remote statement did not return within %s of the caller's %v", apocCancellationWaitLimit, wantErr)
	}

	require.Error(t, got.err, "a canceled APOC remote JSON statement must fail")
	require.Nil(t, got.result, "a canceled APOC remote JSON statement must not return rows")
	require.ErrorIs(t, got.err, wantErr)

	if hold == apocSeamDNS {
		seen := dns.seenContexts()
		require.Len(t, seen, 1)
		assert.Equal(t, wantErr, seen[0].Err(), "allowlist DNS resolution did not run on the caller's context")
		assert.Equal(t, apocCancellationMarkerValue, seen[0].Value(apocCancellationMarkerKey{}),
			"allowlist DNS resolution ran outside the caller's statement context")
		assert.Zero(t, remote.requestCount(), "a canceled DNS resolution must not be followed by an outbound request")
	} else {
		require.Equal(t, 1, remote.requestCount())
		assert.Equal(t, wantErr, remote.requestContextError(), "the outbound request did not carry the caller's context")
	}

	nodes, err := eng.AllNodes(parent)
	require.NoError(t, err)
	assert.Empty(t, nodes, "a canceled APOC remote JSON statement must not import nodes")
	edges, err := eng.AllEdges(parent)
	require.NoError(t, err)
	assert.Empty(t, edges, "a canceled APOC remote JSON statement must not import relationships")
}
