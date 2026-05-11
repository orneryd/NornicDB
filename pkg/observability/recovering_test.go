package observability

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// panickyHandler triggers a panic in Handle. Used to verify recoveringHandler
// catches it and the calling goroutine survives.
type panickyHandler struct {
	panicVal any
	called   int
	mu       sync.Mutex
}

func (p *panickyHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (p *panickyHandler) Handle(_ context.Context, _ slog.Record) error {
	p.mu.Lock()
	p.called++
	p.mu.Unlock()
	panic(p.panicVal)
}
func (p *panickyHandler) WithAttrs(_ []slog.Attr) slog.Handler { return p }
func (p *panickyHandler) WithGroup(_ string) slog.Handler      { return p }

// TestRecoveringHandler_PanicInRedactor — install a panicking inner handler;
// assert (a) no panic propagates, (b) a single line on os.Stderr matches
// `"msg":"slog handler panic"`, (c) returned error from Handle is nil.
func TestRecoveringHandler_PanicInRedactor(t *testing.T) {
	// Capture os.Stderr.
	oldStderr := os.Stderr
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stderr = w
	t.Cleanup(func() { os.Stderr = oldStderr })

	inner := &panickyHandler{panicVal: errors.New("kaboom")}
	rec := newRecoveringHandler(inner)
	logger := slog.New(rec)

	require.NotPanics(t, func() {
		logger.Info("hello")
	})

	require.NoError(t, w.Close())
	captured := make([]byte, 4096)
	n, _ := r.Read(captured)
	out := string(captured[:n])

	require.Contains(t, out, "slog handler panic", "expected fallback line on stderr; got: %s", out)
	// Exactly one line.
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	require.Equal(t, 1, len(lines), "expected exactly one fallback line, got %d: %v", len(lines), lines)
}

// TestRecoveringHandler_NoPanicHappyPath — non-panicking inner handler
// produces zero stderr writes.
func TestRecoveringHandler_NoPanicHappyPath(t *testing.T) {
	oldStderr := os.Stderr
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stderr = w
	t.Cleanup(func() { os.Stderr = oldStderr })

	lv := &slog.LevelVar{}
	inner := newNornicdbJSONHandler(os.Stdout, lv) // writes to stdout, not stderr
	rec := newRecoveringHandler(inner)
	logger := slog.New(rec)
	logger.Info("ok", "k", "v")

	require.NoError(t, w.Close())
	buf := make([]byte, 4096)
	n, _ := r.Read(buf)
	require.Equal(t, 0, n, "no panic must produce zero stderr writes; got: %s", string(buf[:n]))
}

// TestRecoveringHandler_PreservesWith — WithAttrs / WithGroup must keep the
// recoveringHandler outermost (the invariant survives derivation).
func TestRecoveringHandler_PreservesWith(t *testing.T) {
	inner := &panickyHandler{panicVal: "boom"}
	rec := newRecoveringHandler(inner)
	derived := rec.WithAttrs([]slog.Attr{slog.String("k", "v")})
	_, ok := derived.(*recoveringHandler)
	require.True(t, ok, "WithAttrs must return *recoveringHandler so the outermost invariant holds")

	derivedG := rec.WithGroup("g")
	_, ok = derivedG.(*recoveringHandler)
	require.True(t, ok, "WithGroup must return *recoveringHandler")
}

// TestRecoveringHandler_EnabledDelegates — Enabled simply delegates.
func TestRecoveringHandler_EnabledDelegates(t *testing.T) {
	lv := &slog.LevelVar{}
	lv.Set(slog.LevelWarn)
	inner := newNornicdbJSONHandler(os.Stdout, lv)
	rec := newRecoveringHandler(inner)
	require.False(t, rec.Enabled(context.Background(), slog.LevelInfo))
	require.True(t, rec.Enabled(context.Background(), slog.LevelWarn))
}
