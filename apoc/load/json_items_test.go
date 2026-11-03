package load

// The JSON row loader has one owner: JSONItems binds a row shape once and
// delegates acquisition, decoding, admission and cancellation to Json. These
// checks pin the row shapes, the invalid-mode rejection before I/O, the
// canonical error prefix, and the caller's cancellation and deadline identity.

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/apocio"
	"github.com/stretchr/testify/require"
)

// jsonItemsShapes pairs each supported shape with its test label.
var jsonItemsShapes = []struct {
	name  string
	shape JSONItemShape
}{
	{"DocumentItems", DocumentItems},
	{"RequireArray", RequireArray},
}

// jsonItemsFile writes one JSON document under the test's temporary directory.
func jsonItemsFile(t *testing.T, document string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "payload.json")
	require.NoError(t, os.WriteFile(path, []byte(document), 0o600))
	return path
}

// jsonItemsAllowedCapability is the explicit policy a Go caller binds: import
// file access is allowed, so only the loader's own behavior decides the result.
func jsonItemsAllowedCapability() apocio.Capability {
	return apocio.NewPolicy(apocio.Config{AllowImportFileAccess: true}).Capability()
}

// TestJSONItemsDocumentShapes pins both row shapes: DocumentItems publishes an
// array's items and wraps every other document as one item, while RequireArray
// refuses anything that is not an array. An empty array publishes no rows but
// remains a non-nil item slice.
func TestJSONItemsDocumentShapes(t *testing.T) {
	ctx := callerContext(jsonItemsAllowedCapability())

	tests := []struct {
		name         string
		document     string
		want         []interface{}
		requireArray bool
	}{
		{
			name:     "array",
			document: `[{"id":1},{"id":2}]`,
			want: []interface{}{
				map[string]interface{}{"id": float64(1)},
				map[string]interface{}{"id": float64(2)},
			},
		},
		{
			name:     "empty array",
			document: `[]`,
			want:     []interface{}{},
		},
		{
			name:         "object",
			document:     `{"id":1}`,
			want:         []interface{}{map[string]interface{}{"id": float64(1)}},
			requireArray: true,
		},
		{
			name:         "scalar",
			document:     `42`,
			want:         []interface{}{float64(42)},
			requireArray: true,
		},
		{
			name:         "null",
			document:     `null`,
			want:         []interface{}{nil},
			requireArray: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := jsonItemsFile(t, tc.document)

			document, err := JSONItems(DocumentItems)(ctx, path)
			require.NoError(t, err)
			require.Equal(t, tc.want, document)

			array, err := JSONItems(RequireArray)(ctx, path)
			if tc.requireArray {
				require.EqualError(t, err, "data is not an array")
				require.Nil(t, array)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, array)
		})
	}
}

// TestJSONItemsMalformedDocumentKeepsCanonicalError proves the row loader
// publishes the scalar Json owner's failure unchanged, including its prefix.
func TestJSONItemsMalformedDocumentKeepsCanonicalError(t *testing.T) {
	ctx := callerContext(jsonItemsAllowedCapability())
	path := jsonItemsFile(t, `{"broken":`)

	_, canonicalErr := Json(ctx, path)
	require.Error(t, canonicalErr)

	for _, tc := range jsonItemsShapes {
		t.Run(tc.name, func(t *testing.T) {
			items, err := JSONItems(tc.shape)(ctx, path)
			require.Nil(t, items)
			require.EqualError(t, err, canonicalErr.Error(),
				"the row loader publishes the scalar owner's error unchanged")
			require.True(t, strings.HasPrefix(err.Error(), "failed to load JSON: "),
				"malformed JSON keeps the canonical prefix: %v", err)
		})
	}
}

// TestJSONItemsRejectsInvalidShapeBeforeIO proves an unapproved shape fails
// deterministically before any source admission or read.
func TestJSONItemsRejectsInvalidShapeBeforeIO(t *testing.T) {
	const invalid = JSONItemShape(99)

	// No capability is bound: an I/O attempt would report ErrCapabilityMissing.
	items, err := JSONItems(invalid)(context.Background(), "/definitely/missing.json")
	require.Nil(t, items)
	require.EqualError(t, err, "invalid JSON item shape 99")
	require.NotErrorIs(t, err, apocio.ErrCapabilityMissing)

	// A cancelled caller would also fail differently if I/O were attempted.
	cancelled, cancel := context.WithCancel(callerContext(jsonItemsAllowedCapability()))
	cancel()
	_, err = JSONItems(invalid)(cancelled, "/definitely/missing.json")
	require.EqualError(t, err, "invalid JSON item shape 99")
	require.NotErrorIs(t, err, context.Canceled)

	// A transport that would fail loudly if the invalid shape reached the source.
	var hits atomic.Int64
	policy := apocio.NewPolicy(apocio.Config{
		AllowRemoteURLAccess: true,
		RemoteURLAllowlist:   []string{"example.com"},
		HostResolver:         publicResolver,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			hits.Add(1)
			return nil, errors.New("transport must not run for an invalid shape")
		})},
	})
	_, err = JSONItems(invalid)(callerContext(policy.Capability()), "https://example.com/data.json")
	require.EqualError(t, err, "invalid JSON item shape 99")
	require.Zero(t, hits.Load(), "an invalid shape is rejected before source acquisition")
}

// TestJSONItemsCallerCancellationAndDeadline proves both shapes preserve the
// caller's cancellation and deadline identity and still succeed on a live
// control context.
func TestJSONItemsCallerCancellationAndDeadline(t *testing.T) {
	path := jsonItemsFile(t, `[{"id":1}]`)

	cancelled, cancel := context.WithCancel(callerContext(jsonItemsAllowedCapability()))
	cancel()
	deadline, deadlineCancel := context.WithDeadline(callerContext(jsonItemsAllowedCapability()), time.Now().Add(-time.Second))
	defer deadlineCancel()

	for _, tc := range jsonItemsShapes {
		t.Run(tc.name, func(t *testing.T) {
			handler := JSONItems(tc.shape)

			items, err := handler(cancelled, path)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, items, "a cancelled caller publishes no row")

			items, err = handler(deadline, path)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Nil(t, items, "an expired deadline publishes no row")

			// Live control: the same bound handler succeeds on a live context.
			items, err = handler(callerContext(jsonItemsAllowedCapability()), path)
			require.NoError(t, err)
			require.Len(t, items, 1)
		})
	}
}

// TestJSONItemsCancelsDuringBodyRead proves cancellation reaches the shared
// source reader and publishes no row for either shape.
func TestJSONItemsCancelsDuringBodyRead(t *testing.T) {
	for _, tc := range jsonItemsShapes {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			body := &cancelJSONBody{Reader: strings.NewReader(`[{"id":1}]`), cancel: cancel}
			policy := apocio.NewPolicy(apocio.Config{
				AllowRemoteURLAccess: true,
				RemoteURLAllowlist:   []string{"example.com"},
				HostResolver:         publicResolver,
				HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Status:     "200 OK",
						Body:       body,
						Header:     make(http.Header),
						Request:    req,
					}, nil
				})},
			})

			items, err := JSONItems(tc.shape)(apocio.WithCapability(ctx, policy.Capability()), "https://example.com/data.json")
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, items, "a body that cancels its actual caller publishes no row")
			require.Equal(t, 1, body.closes)
		})
	}
}

// TestJSONItemsCapabilityAdmission proves the factory performs no I/O without
// the typed capability, honors the default deny policy, and succeeds only when
// the caller explicitly allows import file access.
func TestJSONItemsCapabilityAdmission(t *testing.T) {
	path := jsonItemsFile(t, `[{"name":"neo"}]`)
	denied := apocio.NewPolicy(apocio.Config{}).Capability()
	allowed := jsonItemsAllowedCapability()

	for _, tc := range jsonItemsShapes {
		t.Run(tc.name, func(t *testing.T) {
			handler := JSONItems(tc.shape)

			items, err := handler(context.Background(), path)
			require.ErrorIs(t, err, apocio.ErrCapabilityMissing)
			require.Nil(t, items, "no capability means no I/O and no rows")

			items, err = handler(callerContext(denied), path)
			require.ErrorIs(t, err, apocio.ErrLocalImportFileAccessDisabled)
			require.Nil(t, items, "the default policy denies import file access")

			items, err = handler(callerContext(allowed), path)
			require.NoError(t, err)
			require.Len(t, items, 1)
		})
	}
}
