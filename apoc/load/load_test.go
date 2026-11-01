package load

// The apoc.load Go entry points are the bundled plugin's real I/O handlers.
// These checks pin the admission contract they share with the native APOC
// procedures: an explicit context capability is required, a missing one
// performs no I/O, and the zero capability cannot inherit an outer one.

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/apocio"
	"github.com/stretchr/testify/require"
)

// roundTripFunc adapts a function to http.RoundTripper.
type roundTripFunc func(req *http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return fn(req) }

// callerContext is the statement context a query executor would supply.
func callerContext(c apocio.Capability) context.Context {
	return apocio.WithCapability(context.Background(), c)
}

// publicResolver answers every lookup with a public address, so the address
// policy never depends on ambient DNS.
func publicResolver(context.Context, string) ([]net.IPAddr, error) {
	return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
}

func TestLoadersRequireBoundCapability(t *testing.T) {
	file := filepath.Join(t.TempDir(), "payload.json")
	require.NoError(t, os.WriteFile(file, []byte(`[{"name":"neo"}]`), 0o600))

	// Import file access is explicitly allowed for the policy, so only the
	// missing capability can deny the load.
	allowed := apocio.NewPolicy(apocio.Config{AllowImportFileAccess: true}).Capability()

	for name, load := range map[string]func(context.Context) error{
		"Json": func(ctx context.Context) error {
			_, err := Json(ctx, file)
			return err
		},
		"JSONItems(DocumentItems)": func(ctx context.Context) error {
			_, err := JSONItems(DocumentItems)(ctx, file)
			return err
		},
		"JSONItems(RequireArray)": func(ctx context.Context) error {
			_, err := JSONItems(RequireArray)(ctx, file)
			return err
		},
		"Csv": func(ctx context.Context) error {
			_, err := Csv(ctx, file, map[string]interface{}{})
			return err
		},
		"Xml": func(ctx context.Context) error {
			_, err := Xml(ctx, file)
			return err
		},
		"Binary": func(ctx context.Context) error {
			_, err := Binary(ctx, file)
			return err
		},
		"Stream": func(ctx context.Context) error {
			reader, err := Stream(ctx, file, map[string]interface{}{})
			if err == nil {
				reader.Close()
			}
			return err
		},
	} {
		load := load
		t.Run(name, func(t *testing.T) {
			// No capability at all.
			require.ErrorIs(t, load(context.Background()), apocio.ErrCapabilityMissing)

			// The zero capability bound over a privileged caller context: it
			// shadows the outer capability, so it only ever denies.
			shadowed := apocio.WithCapability(callerContext(allowed), apocio.Capability{})
			require.ErrorIs(t, load(shadowed), apocio.ErrCapabilityMissing)

			// The same call through the explicit capability succeeds.
			require.NoError(t, load(callerContext(allowed)))
		})
	}
}

// TestLoadersUseBoundPolicy proves the loaders read the caller's capability,
// not an ambient one: an unprivileged capability denies a file the privileged
// capability in the same process may read.
func TestLoadersUseBoundPolicy(t *testing.T) {
	file := filepath.Join(t.TempDir(), "payload.json")
	require.NoError(t, os.WriteFile(file, []byte(`[{"name":"neo"}]`), 0o600))

	allowed := apocio.NewPolicy(apocio.Config{AllowImportFileAccess: true}).Capability()
	denied := apocio.NewPolicy(apocio.Config{}).Capability()

	require.NoError(t, mustJson(t, callerContext(allowed), file))
	_, err := Json(callerContext(denied), file)
	require.ErrorIs(t, err, apocio.ErrLocalImportFileAccessDisabled)
}

func mustJson(t *testing.T, ctx context.Context, source string) error {
	t.Helper()
	data, err := Json(ctx, source)
	if err != nil {
		return err
	}
	items, ok := data.([]interface{})
	if !ok || len(items) != 1 {
		t.Fatalf("unexpected JSON document: %T", data)
	}
	return nil
}

// TestRestCarriesCallerContext proves the shared owner runs the outbound
// request on the caller's context, so a cancelled statement ends the load.
func TestRestCarriesCallerContext(t *testing.T) {
	type contextKey struct{}
	ctx, cancel := context.WithCancel(apocio.WithCapability(
		context.WithValue(context.Background(), contextKey{}, "caller"),
		apocio.NewPolicy(apocio.Config{
			AllowRemoteURLAccess: true,
			RemoteURLAllowlist:   []string{"example.com"},
			HostResolver:         publicResolver,
			HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if err := req.Context().Err(); err != nil {
					return nil, err
				}
				require.Equal(t, "caller", req.Context().Value(contextKey{}))
				return &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Body:       io.NopCloser(strings.NewReader(`{"name":"neo"}`)),
					Header:     make(http.Header),
					Request:    req,
				}, nil
			})},
		}).Capability()))
	cancel()

	_, err := Rest(ctx, "https://example.com/data", map[string]interface{}{})
	require.ErrorIs(t, err, context.Canceled)
}

type cancelJSONBody struct {
	*strings.Reader
	cancel context.CancelFunc
	closes int
}

func (b *cancelJSONBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.cancel()
	return n, err
}
func (b *cancelJSONBody) Close() error { b.closes++; return nil }

func TestJSONLoaderRejectsCancellationDuringBodyRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := &cancelJSONBody{Reader: strings.NewReader(`{"value":1}`), cancel: cancel}
	policy := apocio.NewPolicy(apocio.Config{AllowRemoteURLAccess: true, RemoteURLAllowlist: []string{"example.com"}, HostResolver: publicResolver, HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: body, Header: make(http.Header), Request: r}, nil
	})}})
	result, err := Json(apocio.WithCapability(ctx, policy.Capability()), "https://example.com/data.json")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result, "a body that cancels its actual caller cannot publish JSON")
	require.Equal(t, 1, body.closes)
}
