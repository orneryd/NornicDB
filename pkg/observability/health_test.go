package observability

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHealth_RegisterAndDeregister covers the basic lifecycle of the registry:
// registering a check makes it visible in Ready output, deregistering removes it.
func TestHealth_RegisterAndDeregister(t *testing.T) {
	h := NewHealth()

	h.Register("ping", func(ctx context.Context) error { return nil })

	res := h.Ready(context.Background())
	require.True(t, res.OK)
	require.Contains(t, res.Checks, "ping")
	require.True(t, res.Checks["ping"].OK)

	h.Deregister("ping")

	res2 := h.Ready(context.Background())
	require.True(t, res2.OK)
	require.NotContains(t, res2.Checks, "ping")

	// Deregister of unknown name is a no-op (idempotent).
	h.Deregister("does-not-exist")
}

// TestHealth_RegisterIdempotent — registering the same name twice must
// overwrite, so the second function is the one Ready runs (e.g. for t.Cleanup
// re-registration).
func TestHealth_RegisterIdempotent(t *testing.T) {
	h := NewHealth()
	var firstCalled, secondCalled atomic.Bool
	h.Register("svc", func(ctx context.Context) error {
		firstCalled.Store(true)
		return nil
	})
	h.Register("svc", func(ctx context.Context) error {
		secondCalled.Store(true)
		return nil
	})

	_ = h.Ready(context.Background())
	require.False(t, firstCalled.Load(), "first registered fn must NOT be called after overwrite")
	require.True(t, secondCalled.Load(), "second registered fn must be called")
}

// TestHealth_RequiredVsInformational — table-driven over Required×passes:
// only required-failure flips OK to false. Informational failures still appear
// in the JSON map but don't change overall status.
func TestHealth_RequiredVsInformational(t *testing.T) {
	cases := []struct {
		name      string
		required  bool
		fail      bool
		wantOK    bool
		wantError bool
	}{
		{"required passes", true, false, true, false},
		{"required fails", true, true, false, true},
		{"informational passes", false, false, true, false},
		{"informational fails", false, true, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHealth()
			fn := func(ctx context.Context) error {
				if tc.fail {
					return errors.New("boom")
				}
				return nil
			}
			h.Register("c", fn, CheckOpts{Required: tc.required})
			res := h.Ready(context.Background())
			require.Equal(t, tc.wantOK, res.OK)
			require.Contains(t, res.Checks, "c")
			if tc.wantError {
				require.False(t, res.Checks["c"].OK)
				require.NotEmpty(t, res.Checks["c"].Error)
			} else {
				require.True(t, res.Checks["c"].OK)
				require.Empty(t, res.Checks["c"].Error)
			}
		})
	}
}

// TestHealth_PerCheckTimeout — a slow check is bounded by CheckOpts.Timeout
// and reports a deadline-exceeded error in its CheckStatus. Ready returns
// well before the slow check would have completed naturally.
func TestHealth_PerCheckTimeout(t *testing.T) {
	h := NewHealth()
	h.Register("slow", func(ctx context.Context) error {
		select {
		case <-time.After(5 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, CheckOpts{Timeout: 100 * time.Millisecond, Required: true})

	start := time.Now()
	parentCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	res := h.Ready(parentCtx)
	elapsed := time.Since(start)

	require.False(t, res.OK)
	require.Less(t, elapsed, 500*time.Millisecond, "Ready must return well before slow check naturally completes")
	require.Contains(t, res.Checks, "slow")
	require.False(t, res.Checks["slow"].OK)
	require.NotEmpty(t, res.Checks["slow"].Error)
}

// TestHealth_ConcurrentRegisterRace — N goroutines registering, deregistering,
// and reading concurrently must be race-clean. Run under -race.
func TestHealth_ConcurrentRegisterRace(t *testing.T) {
	h := NewHealth()
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = h.Ready(context.Background())
			}
		}
	}()

	// Writers
	for i := 0; i < 10; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				name := "svc" + string(rune('A'+i))
				h.Register(name, func(ctx context.Context) error { return nil })
				h.Deregister(name)
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestHealth_RenderJSON_HasNoProgressFieldInPhase1 — Phase 9 K8S-06 will add a
// `progress` field to CheckStatus. Phase 1 must NOT emit it. We assert the
// JSON encoding produced by the /readyz handler omits the literal key
// "progress".
func TestHealth_RenderJSON_HasNoProgressFieldInPhase1(t *testing.T) {
	h := NewHealth()
	h.Register("ok", func(ctx context.Context) error { return nil })
	h.Register("bad", func(ctx context.Context) error { return errors.New("nope") }, CheckOpts{Required: true})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	h.handleReadyz(rec, req)

	body := rec.Body.String()
	require.NotContains(t, strings.ToLower(body), "progress", "Phase 1 JSON must not include 'progress' key")

	// Also assert it's valid JSON with the expected top-level shape.
	var parsed struct {
		OK     bool                       `json:"ok"`
		Checks map[string]json.RawMessage `json:"checks"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &parsed))
	require.False(t, parsed.OK)
	require.Contains(t, parsed.Checks, "bad")
	require.Contains(t, parsed.Checks, "ok")
}

// TestHealth_HandleReadyz_StatusCodes — table-driven assertion of status code
// versus required-check outcomes. Documents D-03 and ensures the JSON body is
// always present (never empty) on either 200 or 503.
func TestHealth_HandleReadyz_StatusCodes(t *testing.T) {
	cases := []struct {
		name     string
		setup    func(h *Health)
		wantCode int
		wantOK   bool
	}{
		{
			name:     "no checks registered → 200",
			setup:    func(h *Health) {},
			wantCode: http.StatusOK,
			wantOK:   true,
		},
		{
			name: "single required passing → 200",
			setup: func(h *Health) {
				h.Register("svc", func(ctx context.Context) error { return nil }, CheckOpts{Required: true})
			},
			wantCode: http.StatusOK,
			wantOK:   true,
		},
		{
			name: "single required failing → 503",
			setup: func(h *Health) {
				h.Register("svc", func(ctx context.Context) error { return errors.New("down") }, CheckOpts{Required: true})
			},
			wantCode: http.StatusServiceUnavailable,
			wantOK:   false,
		},
		{
			name: "informational failure → 200",
			setup: func(h *Health) {
				h.Register("svc", func(ctx context.Context) error { return errors.New("info") })
			},
			wantCode: http.StatusOK,
			wantOK:   true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHealth()
			tc.setup(h)
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
			h.handleReadyz(rec, req)
			require.Equal(t, tc.wantCode, rec.Code)
			assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

			var got ReadyResult
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			require.Equal(t, tc.wantOK, got.OK)
		})
	}
}
