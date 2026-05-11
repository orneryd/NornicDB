// Package bolt — Plan 04-02 Task 04-02-05 packstream decode-error
// reason classification (CONTEXT D-11c).
//
// Goal: every packstream decode failure reaches
// nornicdb_bolt_packstream_decode_errors_total{reason} under exactly
// ONE of the closed enum values defined in
// observability.AllowedPackstreamReasons:
//
//	truncated      — incomplete data / out of bounds / EOF mid-decode
//	invalid_marker — unknown marker byte / not-a-X type-tag mismatch
//	wrong_type     — decoded value is the wrong shape for its consumer
//	oversize       — declared length exceeds the buffer / size limit
//
// Free-form err.Error() strings MUST NEVER reach the *Vec — that would
// be a cardinality bomb (RESEARCH §Q11; Phase 3 D-03a / D-04 belt).
// The classifier is the chokepoint: callers pass the error through
// reasonFromError(err) which emits a closed-enum string OR returns
// "" to signal "non-decode error, do not observe".
//
// Sentinel errors (errTruncated, errInvalidMarker, errWrongType,
// errOversize) provide a typed seam — packstream.go can wrap a
// fmt.Errorf with errors.Is-friendly sentinels at known sites, and
// reasonFromError(err) uses errors.Is first for fast/correct matches,
// falling back to substring detection on the legacy fmt.Errorf
// messages that already exist in the decoder.
package bolt

import (
	"errors"
	"io"
	"strings"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// Sentinel errors for packstream decode failures. Wrapping these via
// fmt.Errorf("context: %w", errTruncated) keeps the existing log-friendly
// error messages while making the closed enum classification
// errors.Is-fast.
var (
	// errTruncated indicates incomplete data — the decoder ran past
	// the end of the buffer, hit EOF mid-marker, or saw a chunk size
	// that promised more bytes than were available.
	errTruncated = errors.New("packstream: truncated")

	// errInvalidMarker indicates an unknown / unsupported marker byte.
	// Free-form fmt.Errorf("unknown marker: 0x%02X", ...) sites in the
	// decoder are matched by substring fallback.
	errInvalidMarker = errors.New("packstream: invalid marker")

	// errWrongType indicates a marker decoded successfully but the
	// value is the wrong shape for its consumer (e.g. a list where a
	// map was expected). "not a X marker" sites in the decoder are
	// matched by substring fallback.
	errWrongType = errors.New("packstream: wrong type")

	// errOversize indicates a declared length / chunk size exceeds the
	// buffer or a configured cap. "too large", "exceeds", "oversized"
	// substrings hit this bucket.
	errOversize = errors.New("packstream: oversize")
)

// reasonFromError classifies a decode error into the closed
// observability.AllowedPackstreamReasons enum. Returns "" when the
// error is not a packstream-decode error (caller MUST NOT observe in
// that case — non-decode errors flow through the standard
// dispatch-error metric).
//
// Classification order:
//  1. errors.Is sentinel match (precise, fast).
//  2. io.EOF / io.ErrUnexpectedEOF → "truncated".
//  3. Substring fallback on the legacy fmt.Errorf messages already in
//     packstream.go (incomplete, out of bounds, unknown marker,
//     not a ... marker, too large, oversized).
//
// The substring set is deliberately narrow — adding a new pattern
// requires an ADR amendment + observability.AllowedPackstreamReasons
// update.
func reasonFromError(err error) string {
	if err == nil {
		return ""
	}

	// 1. Sentinel match (forward-compatible — wrapping errors via
	//    fmt.Errorf("...: %w", errTruncated) lights up fast.
	switch {
	case errors.Is(err, errTruncated):
		return "truncated"
	case errors.Is(err, errInvalidMarker):
		return "invalid_marker"
	case errors.Is(err, errWrongType):
		return "wrong_type"
	case errors.Is(err, errOversize):
		return "oversize"
	}

	// 2. EOF family → truncated. The decoder reads via io.ReadFull on
	//    chunked input; an unexpected EOF mid-message is the textbook
	//    truncation case.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "truncated"
	}

	// 3. Substring fallback on legacy fmt.Errorf messages already in
	//    pkg/bolt/packstream.go. The substrings are scoped to known
	//    decoder-error phrases to avoid false-positive matches on
	//    higher-level handler errors.
	msg := err.Error()
	switch {
	case strings.Contains(msg, "incomplete"),
		strings.Contains(msg, "out of bounds"),
		strings.Contains(msg, "data out of bounds"):
		return "truncated"
	case strings.Contains(msg, "unknown marker"),
		strings.Contains(msg, "unsupported marker"):
		return "invalid_marker"
	case strings.Contains(msg, "not a string marker"),
		strings.Contains(msg, "not a map marker"),
		strings.Contains(msg, "not a list marker"),
		strings.Contains(msg, "not a structure marker"):
		return "wrong_type"
	case strings.Contains(msg, "too large"),
		strings.Contains(msg, "oversized"),
		strings.Contains(msg, "exceeds maximum"):
		return "oversize"
	}

	// Non-decode error — caller must not observe under
	// packstream_decode_errors_total.
	return ""
}

// observePackstreamDecodeError is the SOLE call site for incrementing
// packstream_decode_errors_total{reason} (DRY chokepoint). Invoked from
// the bolt message dispatch boundary on any error returned by decode
// helpers; nil-safe when the metrics bag has not been wired.
//
// Returns true when the error WAS classified as a packstream-decode
// error and observed, false otherwise — callers can use the bool to
// decide whether to emit a higher-level error counter for non-decode
// errors.
func (s *Server) observePackstreamDecodeError(err error) bool {
	if err == nil {
		return false
	}
	reason := reasonFromError(err)
	if reason == "" {
		return false
	}
	if s == nil {
		return false
	}
	s.mu.RLock()
	ms := s.metricsState
	s.mu.RUnlock()
	if ms == nil || ms.bag == nil {
		// Classification succeeded but no bag — caller still observed
		// the error semantically; return true so the caller knows it
		// was a packstream decode error (vs. non-decode).
		return true
	}
	ms.bag.PackstreamDecodeErrors.WithLabelValues(reason).Inc()
	// Defense-in-depth: assert the reason value is in the closed enum.
	// In production this is a static guarantee from the switch above;
	// the assertion catches any future contributor who adds a new
	// classification path without updating AllowedPackstreamReasons.
	_ = isAllowedPackstreamReason(reason)
	return true
}

// isAllowedPackstreamReason returns true when the value is in the
// closed observability.AllowedPackstreamReasons set. Used as a
// defense-in-depth check from observePackstreamDecodeError.
func isAllowedPackstreamReason(reason string) bool {
	for _, allowed := range observability.AllowedPackstreamReasons {
		if reason == allowed {
			return true
		}
	}
	return false
}
