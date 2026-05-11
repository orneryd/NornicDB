package bolt

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReasonFromError asserts CONTEXT D-11c classification: every input
// error reaches exactly one closed-enum reason value, and non-decode
// errors return "" (caller must not observe under
// packstream_decode_errors_total).
func TestReasonFromError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		// Sentinel matches (errors.Is fast path).
		{"sentinel_truncated", errTruncated, "truncated"},
		{"sentinel_invalid_marker", errInvalidMarker, "invalid_marker"},
		{"sentinel_wrong_type", errWrongType, "wrong_type"},
		{"sentinel_oversize", errOversize, "oversize"},

		// Wrapped sentinels (forward-compat for sites that use %w).
		{"wrapped_truncated", fmt.Errorf("decoding HELLO: %w", errTruncated), "truncated"},
		{"wrapped_invalid_marker", fmt.Errorf("at offset 12: %w", errInvalidMarker), "invalid_marker"},

		// EOF family → truncated (idiomatic Go decode-truncation).
		{"io_eof", io.EOF, "truncated"},
		{"io_unexpected_eof", io.ErrUnexpectedEOF, "truncated"},
		{"wrapped_eof", fmt.Errorf("read header: %w", io.ErrUnexpectedEOF), "truncated"},

		// Substring fallback against legacy fmt.Errorf messages already
		// in pkg/bolt/packstream.go.
		{"legacy_incomplete_string8", errors.New("incomplete STRING8"), "truncated"},
		{"legacy_incomplete_string16", errors.New("incomplete STRING16"), "truncated"},
		{"legacy_offset_oob", errors.New("offset out of bounds"), "truncated"},
		{"legacy_string_data_oob", errors.New("string data out of bounds"), "truncated"},

		{"legacy_unknown_marker", errors.New("unknown marker: 0x42"), "invalid_marker"},

		{"legacy_not_string", errors.New("not a string marker: 0x42"), "wrong_type"},
		{"legacy_not_map", errors.New("not a map marker: 0x42"), "wrong_type"},
		{"legacy_not_list", errors.New("not a list marker: 0x42"), "wrong_type"},

		// Synthetic oversize error (no current site emits this verbatim,
		// but D-11c reserves the bucket; future size-cap errors land here).
		{"synth_too_large", errors.New("string too large: 1234567890"), "oversize"},
		{"synth_oversized", errors.New("oversized chunk"), "oversize"},
		{"synth_exceeds_max", errors.New("exceeds maximum size"), "oversize"},

		// Non-decode errors → "" (must not observe under packstream).
		{"non_decode_nil", nil, ""},
		{"non_decode_random", errors.New("connection reset by peer"), ""},
		{"non_decode_handler", errors.New("handler: failed to lookup database"), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := reasonFromError(tc.err)
			assert.Equal(t, tc.want, got)
			// Belt: every non-empty result MUST be in the closed enum.
			if got != "" {
				assert.True(t, isAllowedPackstreamReason(got),
					"D-11c: classified reason %q must be in AllowedPackstreamReasons", got)
			}
		})
	}
}

// TestPackstreamReason_AllPaths asserts every closed-enum reason value
// can flow through observePackstreamDecodeError → the *Vec — and
// nothing else can. Drives one synthetic decode failure per reason and
// verifies the *Vec series count matches.
func TestPackstreamReason_AllPaths(t *testing.T) {
	reg := prometheus.NewRegistry()
	bag := observability.NewBoltMetrics(reg)

	srv := New(DefaultConfig(), fakeQueryExecutor{})
	srv.SetBoltMetrics(bag)

	// One synthetic error per reason — ensures every closed-enum value
	// has at least one increment.
	syntheticErrors := map[string]error{
		"truncated":      io.ErrUnexpectedEOF,
		"invalid_marker": errInvalidMarker,
		"wrong_type":     errWrongType,
		"oversize":       errOversize,
	}
	for reason, err := range syntheticErrors {
		ok := srv.observePackstreamDecodeError(err)
		assert.True(t, ok, "reason %q should classify as packstream decode error", reason)
	}

	// Verify packstream_decode_errors_total has exactly 4 series, one
	// per reason value (matches the closed enum).
	mfs, err := reg.Gather()
	require.NoError(t, err)
	seen := map[string]bool{}
	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_bolt_packstream_decode_errors_total" {
			continue
		}
		for _, m := range mf.Metric {
			for _, lp := range m.Label {
				if lp.GetName() == "reason" {
					seen[lp.GetValue()] = true
				}
			}
		}
	}
	for _, want := range observability.AllowedPackstreamReasons {
		assert.True(t, seen[want],
			"D-11c: every closed-enum reason must have at least one observation; missing %q", want)
	}
	assert.Len(t, seen, len(observability.AllowedPackstreamReasons),
		"D-11c: exactly the closed-enum reason values must appear; nothing else")

	// Negative case: a non-decode error must NOT increment the *Vec.
	// We capture pre-state and verify post-state is unchanged.
	preCount := histogramCountVec(t, reg, "nornicdb_bolt_packstream_decode_errors_total")
	ok := srv.observePackstreamDecodeError(errors.New("handler: completely unrelated"))
	assert.False(t, ok, "non-decode errors must return false")
	postCount := histogramCountVec(t, reg, "nornicdb_bolt_packstream_decode_errors_total")
	assert.Equal(t, preCount, postCount,
		"non-decode errors must NOT increment packstream_decode_errors_total")
}

// TestPackstreamDecodeError_NilSafe asserts the observer is nil-safe
// when the bag has not been wired. observePackstreamDecodeError still
// returns the classification result so callers can decide whether to
// emit a higher-level error metric.
func TestPackstreamDecodeError_NilSafe(t *testing.T) {
	srv := New(DefaultConfig(), fakeQueryExecutor{})
	// metricsState is nil — bag never wired.

	// Decode error: classifies but cannot observe — returns true to
	// signal "yes, this was a decode error".
	got := srv.observePackstreamDecodeError(errTruncated)
	assert.True(t, got)

	// Non-decode error: returns false regardless of bag state.
	got = srv.observePackstreamDecodeError(errors.New("not a decode error"))
	assert.False(t, got)
}

// histogramCountVec sums all counter values across all label values
// for the named family.
func histogramCountVec(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	var total float64
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.Metric {
			if m.Counter != nil {
				total += m.GetCounter().GetValue()
			}
		}
	}
	return total
}
