package apocio

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// publicTestIP is a public address the policy accepts.
var publicTestIP = net.ParseIP("93.184.216.34")

// roundTripFunc adapts a function to http.RoundTripper.
type roundTripFunc func(req *http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return fn(req) }

// publicResolver answers every lookup with publicTestIP, keeping the tests
// independent from ambient DNS.
func publicResolver(_ context.Context, _ string) ([]net.IPAddr, error) {
	return []net.IPAddr{{IP: publicTestIP}}, nil
}

func respondWith(status int, body string) roundTripFunc {
	return func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: status,
			Status:     http.StatusText(status),
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	}
}

func TestZeroCapabilityAdmitsNoIO(t *testing.T) {
	var capability Capability
	ctx := context.Background()

	_, err := capability.Open(ctx, "https://example.com/data.json", Request{})
	require.ErrorIs(t, err, ErrCapabilityMissing)

	_, err = capability.Open(ctx, "/tmp/payload.json", Request{})
	require.ErrorIs(t, err, ErrCapabilityMissing)

	_, err = capability.ImportPath("/tmp/payload.json")
	require.ErrorIs(t, err, ErrCapabilityMissing)

	_, err = capability.ExportPath("/tmp/payload.json")
	require.ErrorIs(t, err, ErrCapabilityMissing)
}

func TestCapabilityFromRequiresBinding(t *testing.T) {
	_, err := CapabilityFrom(context.Background())
	require.ErrorIs(t, err, ErrCapabilityMissing)

	// Binding an unbound capability does not manufacture admission.
	ctx := WithCapability(context.Background(), Capability{})
	_, err = CapabilityFrom(ctx)
	require.ErrorIs(t, err, ErrCapabilityMissing)

	policy := NewPolicy(Config{})
	ctx = WithCapability(context.Background(), policy.Capability())
	capability, err := CapabilityFrom(ctx)
	require.NoError(t, err)
	// The bound capability is still denied by default.
	_, err = capability.Open(ctx, "https://example.com/data.json", Request{})
	require.ErrorIs(t, err, ErrRemoteURLAccessDisabled)
}

func TestDefaultDeny(t *testing.T) {
	policy := NewPolicy(Config{})
	capability := policy.Capability()

	_, err := capability.Open(context.Background(), "https://example.com/data.json", Request{})
	require.ErrorIs(t, err, ErrRemoteURLAccessDisabled)

	_, err = capability.ImportPath("/data/payload.json")
	require.ErrorIs(t, err, ErrLocalImportFileAccessDisabled)

	_, err = capability.ExportPath("/data/out.json")
	require.ErrorIs(t, err, ErrLocalExportFileAccessDisabled)
}

func TestImportFileAccessisIndependentOfExportFileAccess(t *testing.T) {
	dir := t.TempDir()
	payload := filepath.Join(dir, "payload.json")
	require.NoError(t, os.WriteFile(payload, []byte(`{"k":"v"}`), 0o600))

	capability := NewPolicy(Config{AllowImportFileAccess: true}).Capability()
	body, err := capability.Open(context.Background(), payload, Request{})
	require.NoError(t, err)
	contents, err := io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.Equal(t, `{"k":"v"}`, string(contents))

	_, err = capability.ExportPath(filepath.Join(dir, "out.json"))
	require.ErrorIs(t, err, ErrLocalExportFileAccessDisabled)
}

func TestRemoteURLPolicy(t *testing.T) {
	var requests []*http.Request
	var bodies []string
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests = append(requests, req)
		recorded := ""
		if req.Body != nil {
			data, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			recorded = string(data)
		}
		bodies = append(bodies, recorded)
		return respondWith(http.StatusOK, `{"ok":true}`)(req)
	})}

	base := Config{
		AllowRemoteURLAccess: true,
		RemoteURLAllowlist:   []string{"Example.COM", " *.*.example.com "},
		HTTPClient:           client,
		HostResolver:         publicResolver,
	}
	capability := NewPolicy(base).Capability()

	body, err := capability.Open(context.Background(), "https://example.com/data.json", Request{})
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.Len(t, requests, 1)
	require.Equal(t, http.MethodGet, requests[0].Method)

	// The request method, headers and body reach the outbound request.
	body, err = capability.Open(context.Background(), "https://example.com/graphql", Request{
		Method:  http.MethodPost,
		Headers: map[string]string{"X-Probe": "one", "Content-Type": "application/json"},
		Body:    []byte(`{"query":"x"}`),
	})
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.Len(t, requests, 2)
	require.Equal(t, http.MethodPost, requests[1].Method)
	require.Equal(t, "one", requests[1].Header.Get("X-Probe"))
	require.Equal(t, "application/json", requests[1].Header.Get("Content-Type"))
	require.Equal(t, `{"query":"x"}`, bodies[1])
}

func TestRemoteURLAccessStillRequiresAllowlistHost(t *testing.T) {
	capability := NewPolicy(Config{AllowRemoteURLAccess: true, HTTPClient: &http.Client{Transport: respondWith(http.StatusOK, `{}`)}, HostResolver: publicResolver}).Capability()
	_, err := capability.Open(context.Background(), "https://example.com/data.json", Request{})
	require.ErrorIs(t, err, ErrRemoteURLHostNotAllowed)

	// Wildcard allowlists match a suffix, not the bare parent domain.
	capability = NewPolicy(Config{
		AllowRemoteURLAccess: true,
		RemoteURLAllowlist:   []string{"*.example.com"},
		HTTPClient:           &http.Client{Transport: respondWith(http.StatusOK, `{}`)},
		HostResolver:         publicResolver,
	}).Capability()
	_, err = capability.Open(context.Background(), "https://example.com/data.json", Request{})
	require.ErrorIs(t, err, ErrRemoteURLHostNotAllowed)
	body, err := capability.Open(context.Background(), "https://api.example.com/data.json", Request{})
	require.NoError(t, err)
	require.NoError(t, body.Close())
}

func TestRemoteURLAddressPolicy(t *testing.T) {
	capability := NewPolicy(Config{
		AllowRemoteURLAccess: true,
		RemoteURLAllowlist:   []string{"loopback.example.com", "internal.example.com"},
		HTTPClient:           &http.Client{Transport: respondWith(http.StatusOK, `{}`)},
		HostResolver: func(_ context.Context, host string) ([]net.IPAddr, error) {
			if host == "loopback.example.com" {
				return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
			}
			return []net.IPAddr{{IP: net.ParseIP("10.1.2.3")}}, nil
		},
	}).Capability()

	_, err := capability.Open(context.Background(), "https://loopback.example.com/data.json", Request{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "resolves to a disallowed address")

	_, err = capability.Open(context.Background(), "https://internal.example.com/data.json", Request{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "resolves to a disallowed address")
}

func TestRemoteURLRejectsInvalidTargets(t *testing.T) {
	capability := NewPolicy(Config{
		AllowRemoteURLAccess: true,
		RemoteURLAllowlist:   []string{"example.com"},
		HTTPClient:           &http.Client{Transport: respondWith(http.StatusOK, `{}`)},
		HostResolver:         publicResolver,
	}).Capability()

	for name, source := range map[string]string{
		"userinfo": "https://user:pass@example.com/data.json",
		"fragment": "https://example.com/data.json#frag",
		"scheme":   "ftp://example.com/data.json",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := capability.Open(context.Background(), source, Request{})
			require.Error(t, err)
		})
	}
}

func TestRemoteResponseMustBeOK(t *testing.T) {
	capability := NewPolicy(Config{
		AllowRemoteURLAccess: true,
		RemoteURLAllowlist:   []string{"example.com"},
		HTTPClient:           &http.Client{Transport: respondWith(http.StatusServiceUnavailable, "later")},
		HostResolver:         publicResolver,
	}).Capability()

	_, err := capability.Open(context.Background(), "https://example.com/data.json", Request{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "HTTP error")
}

func TestRedirectsAreNotFollowed(t *testing.T) {
	var targets []string
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			targets = append(targets, req.URL.String())
			return respondWith(http.StatusFound, "")(req)
		}),
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	capability := NewPolicy(Config{
		AllowRemoteURLAccess: true,
		RemoteURLAllowlist:   []string{"example.com"},
		HTTPClient:           client,
		HostResolver:         publicResolver,
	}).Capability()

	_, err := capability.Open(context.Background(), "https://example.com/data.json", Request{})
	require.Error(t, err)
	require.Len(t, targets, 1, "a redirect is not followed to a second target")
}

func TestDefaultRemoteHTTPClientPolicy(t *testing.T) {
	require.NotNil(t, defaultRemoteHTTPClient)
	require.Equal(t, 10*time.Second, defaultRemoteHTTPClient.Timeout)
	require.NotNil(t, defaultRemoteHTTPClient.CheckRedirect)
	require.ErrorIs(t, defaultRemoteHTTPClient.CheckRedirect(&http.Request{}, nil), http.ErrUseLastResponse)
	transport, ok := defaultRemoteHTTPClient.Transport.(*http.Transport)
	require.True(t, ok)
	require.Nil(t, transport.Proxy, "the APOC client uses no ambient proxy")

	// A policy without a configured client uses exactly those defaults.
	require.Same(t, defaultRemoteHTTPClient, NewPolicy(Config{}).Capability().record.remoteHTTPClient())
}

func TestZeroCapabilityShadowsOuterAccess(t *testing.T) {
	granted := NewPolicy(Config{AllowImportFileAccess: true}).Capability()
	payload := filepath.Join(t.TempDir(), "payload.json")
	require.NoError(t, os.WriteFile(payload, []byte(`{}`), 0o600))

	// The outer context is explicitly privileged and reads the file.
	privileged := WithCapability(context.Background(), granted)
	body, err := granted.Open(privileged, payload, Request{})
	require.NoError(t, err)
	require.NoError(t, body.Close())

	// Binding the zero capability against that privileged context must not
	// retain the outer capability: an unbound capability only ever denies.
	shadowed := WithCapability(privileged, Capability{})
	_, err = CapabilityFrom(shadowed)
	require.ErrorIs(t, err, ErrCapabilityMissing,
		"a zero capability binds an explicit missing capability instead of leaving the outer one")
	// apoc/load.TestLoadersRequireBoundCapability proves an I/O caller that
	// resolves its capability from such a context performs no I/O.

	// The outer context keeps its own capability.
	inner, err := CapabilityFrom(privileged)
	require.NoError(t, err)
	body, err = inner.Open(privileged, payload, Request{})
	require.NoError(t, err)
	require.NoError(t, body.Close())
}

func TestCapabilityDerivationSharesOnePolicy(t *testing.T) {
	policy := NewPolicy(Config{AllowImportFileAccess: true})
	require.Same(t, policy.policyRecord(), policy.Capability().record,
		"a capability names the policy record instead of copying its configuration")

	updated := policy.WithImportFileAccess(false)
	require.NotSame(t, policy.policyRecord(), updated.policyRecord(),
		"a With* call publishes a modified copy")
	_, err := updated.Capability().ImportPath("/data/payload.json")
	require.ErrorIs(t, err, ErrLocalImportFileAccessDisabled)
	resolved, err := policy.Capability().ImportPath("/data/payload.json")
	require.NoError(t, err)
	require.Equal(t, filepath.Clean("/data/payload.json"), resolved)
}

func TestLocalFileURLRules(t *testing.T) {
	root := t.TempDir()
	capability := NewPolicy(Config{AllowImportFileAccess: true, FileAccessRoot: root}).Capability()

	resolved, err := capability.ImportPath("file:///../safe/payload.json")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(root, "safe", "payload.json"), resolved)

	resolved, err = capability.ImportPath("/tmp/../safe/payload.json")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(root, "safe", "payload.json"), resolved)

	_, err = capability.ImportPath("file://localhost/safe/payload.json")
	require.ErrorIs(t, err, ErrFileAuthorityNotAllowed)
	_, err = capability.ImportPath("file:///safe/payload.json?x=1")
	require.ErrorIs(t, err, ErrFileQueryNotAllowed)
	_, err = capability.ImportPath("file:///safe/payload.json#frag")
	require.ErrorIs(t, err, ErrFileFragmentNotAllowed)
	_, err = capability.ImportPath("https://example.com/payload.json")
	require.ErrorContains(t, err, "expected local APOC file source")
	_, err = capability.ImportPath("s3://bucket/payload.json")
	require.ErrorContains(t, err, "unsupported APOC file URL scheme")
}

func TestImportRootContainsReads(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "payload.json"), []byte(`[1]`), 0o600))
	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "secret.json"), []byte(`[2]`), 0o600))

	capability := NewPolicy(Config{AllowImportFileAccess: true, FileAccessRoot: root}).Capability()

	body, err := capability.Open(context.Background(), "file:///payload.json", Request{})
	require.NoError(t, err)
	contents, err := io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.Equal(t, "[1]", string(contents))

	// A path outside the root resolves under the root, never outside it.
	_, err = capability.Open(context.Background(), filepath.Join(outside, "secret.json"), Request{})
	require.Error(t, err)
}

func TestLocalSourceRejectsRequestOptions(t *testing.T) {
	dir := t.TempDir()
	payload := filepath.Join(dir, "payload.json")
	require.NoError(t, os.WriteFile(payload, []byte(`{}`), 0o600))
	capability := NewPolicy(Config{AllowImportFileAccess: true}).Capability()

	_, err := capability.Open(context.Background(), payload, Request{Method: http.MethodPost})
	require.ErrorContains(t, err, "expected local APOC file source, got POST request")

	_, err = capability.Open(context.Background(), payload, Request{Headers: map[string]string{"X": "1"}})
	require.ErrorContains(t, err, "expected local APOC file source")

	// A plain GET is a local read, not a rejected option set.
	body, err := capability.Open(context.Background(), payload, Request{Method: http.MethodGet})
	require.NoError(t, err)
	require.NoError(t, body.Close())
}

func TestPolicyCopiesItsInputs(t *testing.T) {
	hosts := []string{"example.com"}
	policy := NewPolicy(Config{AllowRemoteURLAccess: true, RemoteURLAllowlist: hosts, HostResolver: publicResolver, HTTPClient: &http.Client{Transport: respondWith(http.StatusOK, `{}`)}})
	hosts[0] = "attacker.example.com"

	capability := policy.Capability()
	body, err := capability.Open(context.Background(), "https://example.com/data.json", Request{})
	require.NoError(t, err)
	require.NoError(t, body.Close())

	_, err = capability.Open(context.Background(), "https://attacker.example.com/data.json", Request{})
	require.ErrorIs(t, err, ErrRemoteURLHostNotAllowed)

	// A With* call leaves the policy it was derived from unchanged.
	updated := policy.WithRemoteURLAccess(false)
	_, err = updated.Capability().Open(context.Background(), "https://example.com/data.json", Request{})
	require.ErrorIs(t, err, ErrRemoteURLAccessDisabled)
	body, err = capability.Open(context.Background(), "https://example.com/data.json", Request{})
	require.NoError(t, err)
	require.NoError(t, body.Close())
}

func TestOpenUsesCallerContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	capability := NewPolicy(Config{
		AllowRemoteURLAccess: true,
		RemoteURLAllowlist:   []string{"example.com"},
		HostResolver: func(ctx context.Context, _ string) ([]net.IPAddr, error) {
			return nil, ctx.Err()
		},
	}).Capability()

	_, err := capability.Open(ctx, "https://example.com/data.json", Request{})
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, errors.Is(err, context.Canceled), "the cancellation the resolver returned reaches the caller")
}

func TestRemoteRequestCarriesCallerContext(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "caller")

	var seen context.Context
	capability := NewPolicy(Config{
		AllowRemoteURLAccess: true,
		RemoteURLAllowlist:   []string{"example.com"},
		HostResolver:         publicResolver,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			seen = req.Context()
			return respondWith(http.StatusOK, `{}`)(req)
		})},
	}).Capability()

	body, err := capability.Open(ctx, "https://example.com/data.json", Request{})
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.NotNil(t, seen)
	assert.Equal(t, "caller", seen.Value(contextKey{}))
}
