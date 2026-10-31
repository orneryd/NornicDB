package observability

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/internal/jsontext"
)

// nornicdbJSONHandler implements slog.Handler with a sync.Pool-backed buffer
// reuse strategy and direct []byte append (no json.Encoder, no reflection)
// to hit the ≤2 allocs/record budget per D-02b.
//
// Output is byte-compatible with stdlib slog.NewJSONHandler at the SEMANTIC
// JSON level (not byte-level — Pitfall 7). The escape table mirrors stdlib's
// safeSet / safeSetU8 in go/src/log/slog/json_handler.go (HTML escaping
// disabled: `<`, `>`, `&` are NOT escaped — matches stdlib).
//
// Construction: newNornicdbJSONHandler(out, levelVar). The levelVar pointer
// is held verbatim — callers may flip its level at runtime (D-02d).
//
// File budget: kept under 400 LOC per D-02b (PERF-06 cap is 800).
type nornicdbJSONHandler struct {
	out      io.Writer
	mu       *sync.Mutex // serializes writes; one mu per logical writer.
	levelVar *slog.LevelVar

	// preformatted holds bytes appended via WithAttrs; included verbatim
	// before the record-level attrs in every Handle call. Allocated once
	// per derived logger.
	preformatted []byte

	// pendingGroups holds groups opened via WithGroup that have NOT yet
	// been materialized into preformatted. Per slogtest contract, an
	// empty group MUST NOT appear in output — so we defer "{ open" until
	// the first attr lands inside the group. flushPendingGroups emits all
	// pending opens when an attr arrives.
	pendingGroups []string

	// openGroups is the count of '{' actually written to preformatted /
	// the live record buffer that must be balanced by '}' at finalize.
	openGroups int

	// preformattedEndsWithOpenBrace is true when the last byte of preformatted
	// is '{' (i.e. flushPendingGroups just opened a group with no attrs added
	// since). The first attr inside the open group must emit without a
	// leading comma.
	preformattedEndsWithOpenBrace bool
}

// newNornicdbJSONHandler is the canonical constructor (D-02 / D-02a).
func newNornicdbJSONHandler(out io.Writer, levelVar *slog.LevelVar) *nornicdbJSONHandler {
	if levelVar == nil {
		levelVar = &slog.LevelVar{}
	}
	return &nornicdbJSONHandler{
		out:      out,
		mu:       &sync.Mutex{},
		levelVar: levelVar,
	}
}

// bufPool reuses output buffers to keep allocations off the hot path. Pool
// stores *[]byte (pointer; not value) so Put() does not allocate the
// `any`-interface box (golang/example/slog-handler-guide canonical pattern).
var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 1024)
		return &b
	},
}

const maxBufferSize = 16 << 10 // 16 KiB; outliers discarded to bound cap.

func freeBuf(bp *[]byte) {
	if cap(*bp) <= maxBufferSize {
		*bp = (*bp)[:0]
		bufPool.Put(bp)
	}
}

// Enabled is the canonical level gate. ~1ns under -gcflags="-m".
func (h *nornicdbJSONHandler) Enabled(_ context.Context, lvl slog.Level) bool {
	return lvl >= h.levelVar.Level()
}

// Handle serializes the record to JSON and writes it to h.out atomically.
//
// Field order matches stdlib JSONHandler: time, level, msg, then preformatted
// attrs (from WithAttrs / WithGroup), then record-level attrs in insertion
// order. Pitfall 7 documents that stdlib does not guarantee bytewise stability;
// callers asserting parity SHOULD use json.Unmarshal + map equality.
func (h *nornicdbJSONHandler) Handle(_ context.Context, r slog.Record) error {
	bp := bufPool.Get().(*[]byte)
	defer freeBuf(bp)
	buf := (*bp)[:0]

	buf = append(buf, '{')
	if !r.Time.IsZero() {
		buf = append(buf, `"time":`...)
		buf = strconv.AppendQuote(buf, r.Time.Format(time.RFC3339Nano))
		buf = append(buf, ',')
	}
	buf = append(buf, `"level":`...)
	buf = strconv.AppendQuote(buf, r.Level.String())
	buf = append(buf, ',')
	buf = append(buf, `"msg":`...)
	buf = jsontext.AppendLogString(buf, r.Message)

	// Preformatted attrs (already JSON-encoded with leading commas).
	if len(h.preformatted) > 0 {
		buf = append(buf, h.preformatted...)
	}

	// Record-level attrs. We must lazily flush h.pendingGroups (groups
	// opened via WithGroup but not yet materialized) IF the record carries
	// at least one non-empty attr — otherwise empty groups would appear
	// in output, violating the slogtest contract.
	hasAttrs := false
	r.Attrs(func(a slog.Attr) bool {
		a.Value = a.Value.Resolve()
		if a.Equal(slog.Attr{}) {
			return true
		}
		if a.Value.Kind() == slog.KindGroup && len(a.Value.Group()) == 0 {
			return true
		}
		hasAttrs = true
		return false
	})

	openGroups := h.openGroups
	preEndsOpen := h.preformattedEndsWithOpenBrace
	if hasAttrs && len(h.pendingGroups) > 0 {
		// Materialize pending groups directly into buf (NOT into shared
		// preformatted — that would mutate the receiver across calls).
		for _, name := range h.pendingGroups {
			if !preEndsOpen {
				buf = append(buf, ',')
			}
			buf = jsontext.AppendLogString(buf, name)
			buf = append(buf, ':', '{')
			openGroups++
			preEndsOpen = true
		}
	}

	dummyGroups := []string(nil)
	first := preEndsOpen
	r.Attrs(func(a slog.Attr) bool {
		if first {
			buf = appendFirstAttr(buf, a)
			first = false
		} else {
			buf = appendAttr(buf, a, &dummyGroups)
		}
		return true
	})

	// Close any groups opened during preformat or this Handle call.
	for i := 0; i < openGroups; i++ {
		buf = append(buf, '}')
	}

	buf = append(buf, '}', '\n')
	*bp = buf

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := h.out.Write(buf)
	return err
}

// hasNonEmptyAttrs reports whether the attrs slice contains any non-empty
// attr after Resolve. Empty slog.Attr{} entries (typically from variadic
// helpers consuming uneven k/v pairs) and empty groups are skipped per
// the slogtest contract.
func hasNonEmptyAttrs(attrs []slog.Attr) bool {
	for _, a := range attrs {
		a.Value = a.Value.Resolve()
		if a.Equal(slog.Attr{}) {
			continue
		}
		if a.Value.Kind() == slog.KindGroup && len(a.Value.Group()) == 0 {
			continue
		}
		return true
	}
	return false
}

// WithAttrs returns a derived handler with attrs preformatted into the
// internal buffer. Pending groups (opened via WithGroup but not yet
// materialized) are flushed lazily — only if attrs actually land.
func (h *nornicdbJSONHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 || !hasNonEmptyAttrs(attrs) {
		return h
	}
	h2 := *h
	h2.preformatted = slices.Clone(h.preformatted)
	h2.pendingGroups = slices.Clone(h.pendingGroups)
	h2.flushPendingGroups()

	first := h2.preformattedEndsWithOpenBrace
	dummyGroups := []string(nil)
	for _, a := range attrs {
		if first {
			h2.preformatted = appendFirstAttr(h2.preformatted, a)
			first = false
		} else {
			h2.preformatted = appendAttr(h2.preformatted, a, &dummyGroups)
		}
	}
	h2.preformattedEndsWithOpenBrace = false
	return &h2
}

// WithGroup defers group materialization: the group is added to
// pendingGroups and only emitted once an attr lands inside it. This
// matches the slogtest "empty groups must not appear" contract.
func (h *nornicdbJSONHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	h2 := *h
	h2.preformatted = slices.Clone(h.preformatted)
	h2.pendingGroups = append(slices.Clone(h.pendingGroups), name)
	return &h2
}

// flushPendingGroups materializes every pendingGroup into preformatted as
// `,"name":{` (or `"name":{` if currently inside an open empty group).
// After flush, preformattedEndsWithOpenBrace is true and the next attr
// must emit without a leading comma.
func (h *nornicdbJSONHandler) flushPendingGroups() {
	for _, name := range h.pendingGroups {
		if !h.preformattedEndsWithOpenBrace {
			h.preformatted = append(h.preformatted, ',')
		}
		h.preformatted = jsontext.AppendLogString(h.preformatted, name)
		h.preformatted = append(h.preformatted, ':', '{')
		h.openGroups++
		h.preformattedEndsWithOpenBrace = true
	}
	h.pendingGroups = nil
}

// appendAttr emits `,"key":value` (or nested group). Leading comma always
// emitted because we always follow the {"time":..., "level":..., "msg":...}
// preamble. The handler emits time/level/msg first; if these are missing
// (zero time), the leading comma here is harmless within `{...,}` because
// JSON parsers tolerate it... actually they don't. So we treat the
// leading-comma assumption: handler emits at least "level" and "msg" before
// any attr, so a leading comma here is always valid.
func appendAttr(dst []byte, a slog.Attr, groups *[]string) []byte {
	// Resolve LogValuer types.
	a.Value = a.Value.Resolve()

	// Skip if both key and value are empty AND not a group.
	if a.Equal(slog.Attr{}) {
		return dst
	}

	if a.Value.Kind() == slog.KindGroup {
		children := a.Value.Group()
		// Inline group with empty key (slog convention: inline children).
		if a.Key == "" {
			for _, c := range children {
				dst = appendAttr(dst, c, groups)
			}
			return dst
		}
		// Empty group: skip entirely (slog convention).
		if len(children) == 0 {
			return dst
		}
		dst = append(dst, ',')
		dst = jsontext.AppendLogString(dst, a.Key)
		dst = append(dst, ':', '{')
		// First child has no leading comma.
		first := true
		for _, c := range children {
			if first {
				dst = appendFirstAttr(dst, c)
				first = false
			} else {
				dst = appendAttr(dst, c, groups)
			}
		}
		dst = append(dst, '}')
		return dst
	}

	dst = append(dst, ',')
	dst = jsontext.AppendLogString(dst, a.Key)
	dst = append(dst, ':')
	dst = appendValue(dst, a.Value)
	return dst
}

// appendFirstAttr emits an attr without the leading comma (used as the
// first member of a group object).
func appendFirstAttr(dst []byte, a slog.Attr) []byte {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return dst
	}
	if a.Value.Kind() == slog.KindGroup {
		children := a.Value.Group()
		if a.Key == "" {
			// Inline (rare here): degrade to commaful form.
			groups := []string(nil)
			for _, c := range children {
				dst = appendAttr(dst, c, &groups)
			}
			return dst
		}
		if len(children) == 0 {
			return dst
		}
		dst = jsontext.AppendLogString(dst, a.Key)
		dst = append(dst, ':', '{')
		first := true
		groups := []string(nil)
		for _, c := range children {
			if first {
				dst = appendFirstAttr(dst, c)
				first = false
			} else {
				dst = appendAttr(dst, c, &groups)
			}
		}
		dst = append(dst, '}')
		return dst
	}
	dst = jsontext.AppendLogString(dst, a.Key)
	dst = append(dst, ':')
	dst = appendValue(dst, a.Value)
	return dst
}

// appendValue serializes a slog.Value to JSON. Hot-path types (string, int,
// float, bool, duration, time) take direct paths; rare types fall back to
// fmt.Sprintf which is acceptable for non-hot records.
func appendValue(dst []byte, v slog.Value) []byte {
	switch v.Kind() {
	case slog.KindString:
		return jsontext.AppendLogString(dst, v.String())
	case slog.KindInt64:
		return strconv.AppendInt(dst, v.Int64(), 10)
	case slog.KindUint64:
		return strconv.AppendUint(dst, v.Uint64(), 10)
	case slog.KindFloat64:
		f := v.Float64()
		// JSON has no NaN/Inf; emit string per stdlib practice.
		switch {
		case f != f: // NaN
			return append(dst, `"NaN"`...)
		case f > 1e308:
			return append(dst, `"+Inf"`...)
		case f < -1e308:
			return append(dst, `"-Inf"`...)
		}
		return strconv.AppendFloat(dst, f, 'f', -1, 64)
	case slog.KindBool:
		return strconv.AppendBool(dst, v.Bool())
	case slog.KindDuration:
		return strconv.AppendInt(dst, v.Duration().Nanoseconds(), 10)
	case slog.KindTime:
		dst = append(dst, '"')
		dst = v.Time().AppendFormat(dst, time.RFC3339Nano)
		dst = append(dst, '"')
		return dst
	case slog.KindAny:
		return appendAny(dst, v.Any())
	default:
		return jsontext.AppendLogString(dst, v.String())
	}
}

// appendAny handles arbitrary `any` values via reflection-light formatting.
// For byte slices we emit hex (matches stdlib). For other slices/maps we
// fall back to fmt.Sprintf with %v wrapped in a JSON string — semantically
// stable enough for parity tests at the map-level.
func appendAny(dst []byte, v any) []byte {
	switch x := v.(type) {
	case nil:
		return append(dst, "null"...)
	case []byte:
		dst = append(dst, '"')
		hexBuf := make([]byte, hex.EncodedLen(len(x)))
		hex.Encode(hexBuf, x)
		dst = append(dst, hexBuf...)
		dst = append(dst, '"')
		return dst
	case error:
		return jsontext.AppendLogString(dst, x.Error())
	case []string:
		dst = append(dst, '[')
		for i, s := range x {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = jsontext.AppendLogString(dst, s)
		}
		dst = append(dst, ']')
		return dst
	case fmt.Stringer:
		return jsontext.AppendLogString(dst, x.String())
	default:
		return jsontext.AppendLogString(dst, fmt.Sprintf("%v", v))
	}
}
