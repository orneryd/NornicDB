package observability

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// sampledParent builds SamplingParameters that carry a validly-sampled parent
// SpanContext. Shared by the parent-mode tests.
func sampledParent() sdktrace.SamplingParameters {
	traceID, _ := trace.TraceIDFromHex("01020304050607080102030405060708")
	spanID, _ := trace.SpanIDFromHex("0102030405060708")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	return sdktrace.SamplingParameters{
		ParentContext: ctx,
		TraceID:       traceID,
		Name:          "test-span",
	}
}

// notSampledParent builds SamplingParameters with a parent that is valid but
// NOT sampled — this is the case where all three modes should defer to the
// fallback ratio-based sampler.
func notSampledParent() sdktrace.SamplingParameters {
	traceID, _ := trace.TraceIDFromHex("01020304050607080102030405060708")
	spanID, _ := trace.SpanIDFromHex("0102030405060708")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: 0,
		Remote:     true,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	return sdktrace.SamplingParameters{
		ParentContext: ctx,
		TraceID:       traceID,
		Name:          "test-span",
	}
}

func TestParseParentMode(t *testing.T) {
	cases := map[string]struct {
		in      string
		want    ParentMode
		wantErr bool
	}{
		"empty":   {"", ParentModeNone, false},
		"none":    {"none", ParentModeNone, false},
		"off":     {"off", ParentModeNone, false},
		"capped":  {"capped", ParentModeCapped, false},
		"strict":  {"strict", ParentModeStrict, false},
		"garbage": {"banana", ParentModeNone, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := parseParentMode(tc.in)
			assert.Equal(t, tc.want, got)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestBuildSampler_NoneMode_IgnoresSampledParent verifies TRC-05: the v1
// default sampler is standalone TraceIDRatioBased(ratio) and does NOT honor
// an upstream sampled=true parent. At ratio=0 every call must return Drop
// regardless of the parent flag.
func TestBuildSampler_NoneMode_IgnoresSampledParent(t *testing.T) {
	s := buildSampler(ParentModeNone, 0.0, 100)
	res := s.ShouldSample(sampledParent())
	assert.Equal(t, sdktrace.Drop, res.Decision,
		"TRC-05: mode=none must use standalone ratio-based sampler; ratio=0 => Drop even with sampled parent")
}

// TestBuildSampler_StrictMode_HonorsSampledParent verifies TRC-07: strict
// mode honors upstream sampled=true unconditionally (ParentBased semantics).
// At ratio=0 with a sampled parent we still expect RecordAndSample.
func TestBuildSampler_StrictMode_HonorsSampledParent(t *testing.T) {
	s := buildSampler(ParentModeStrict, 0.0, 100)
	res := s.ShouldSample(sampledParent())
	assert.Equal(t, sdktrace.RecordAndSample, res.Decision,
		"TRC-07: mode=strict must honor upstream sampled=true even when local ratio=0")
}

// TestBuildSampler_CappedMode_HonorsUpToCap verifies TRC-06's core behavior:
// the first maxQPS sampled-parent calls within a window return
// RecordAndSample; subsequent calls fall through to the ratio fallback. With
// ratio=0 the fallback returns Drop so we can observe the cap boundary.
func TestBuildSampler_CappedMode_HonorsUpToCap(t *testing.T) {
	const cap = 5
	s := buildSampler(ParentModeCapped, 0.0, cap).(*parentCappedSampler)
	// Freeze the sampler's clock so window never rolls during the test.
	frozen := time.Unix(1_700_000_000, 0)
	s.nowFn = func() time.Time { return frozen }
	s.windowStart.Store(frozen.UnixNano())
	s.tokens.Store(int64(cap))

	honored := 0
	dropped := 0
	for i := 0; i < cap*3; i++ {
		res := s.ShouldSample(sampledParent())
		switch res.Decision {
		case sdktrace.RecordAndSample:
			honored++
		case sdktrace.Drop:
			dropped++
		}
	}
	assert.Equal(t, cap, honored, "TRC-06: cap=%d sampled-parent calls must be honored in one window", cap)
	assert.Equal(t, cap*3-cap, dropped, "TRC-06: excess sampled-parent calls must fall through to fallback (ratio=0 => Drop)")
}

// TestBuildSampler_CappedMode_WindowRolls verifies the token bucket refills
// on each 1-second boundary.
func TestBuildSampler_CappedMode_WindowRolls(t *testing.T) {
	const cap = 2
	s := buildSampler(ParentModeCapped, 0.0, cap).(*parentCappedSampler)
	now := time.Unix(1_700_000_000, 0)
	s.nowFn = func() time.Time { return now }
	s.windowStart.Store(now.UnixNano())
	s.tokens.Store(int64(cap))

	// Exhaust the window.
	for i := 0; i < cap; i++ {
		res := s.ShouldSample(sampledParent())
		require.Equal(t, sdktrace.RecordAndSample, res.Decision)
	}
	res := s.ShouldSample(sampledParent())
	require.Equal(t, sdktrace.Drop, res.Decision, "cap exhausted mid-window")

	// Roll the clock forward past the 1s boundary.
	now = now.Add(1100 * time.Millisecond)
	res = s.ShouldSample(sampledParent())
	assert.Equal(t, sdktrace.RecordAndSample, res.Decision, "tokens must refill on window roll")
}

// TestBuildSampler_CappedMode_NotSampledParentUsesFallback verifies that
// the cap only applies to sampled-parent traffic. Not-sampled parents fall
// straight through to the fallback and never consume a token.
func TestBuildSampler_CappedMode_NotSampledParentUsesFallback(t *testing.T) {
	const cap = 3
	s := buildSampler(ParentModeCapped, 0.0, cap).(*parentCappedSampler)
	now := time.Unix(1_700_000_000, 0)
	s.nowFn = func() time.Time { return now }
	s.windowStart.Store(now.UnixNano())
	s.tokens.Store(int64(cap))

	// Hammer the sampler with not-sampled parents. With ratio=0 the fallback
	// returns Drop, and the token count must not move.
	for i := 0; i < 100; i++ {
		res := s.ShouldSample(notSampledParent())
		require.Equal(t, sdktrace.Drop, res.Decision)
	}
	assert.Equal(t, int64(cap), s.tokens.Load(), "tokens must only be consumed by sampled-parent traffic")
}

// TestBuildSampler_CappedMode_RatioClamped verifies the ratio-arg clamp
// doesn't blow up on out-of-range values.
func TestBuildSampler_CappedMode_RatioClamped(t *testing.T) {
	// These must not panic or deadlock.
	_ = buildSampler(ParentModeCapped, -1.0, 10)
	_ = buildSampler(ParentModeCapped, 2.0, 10)
	_ = buildSampler(ParentModeCapped, 0.5, 0)  // maxQPS clamped to 1
	_ = buildSampler(ParentModeCapped, 0.5, -5) // maxQPS clamped to 1
}

// TestParentCappedSampler_Concurrent verifies the token bucket is race-free
// under parallel ShouldSample calls. Races manifest as more than `cap`
// honored decisions within a single window (double-spend).
func TestParentCappedSampler_Concurrent(t *testing.T) {
	const cap = 50
	const workers = 16
	const callsPerWorker = 100

	s := buildSampler(ParentModeCapped, 0.0, cap).(*parentCappedSampler)
	now := time.Unix(1_700_000_000, 0)
	s.nowFn = func() time.Time { return now }
	s.windowStart.Store(now.UnixNano())
	s.tokens.Store(int64(cap))

	var honored atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < callsPerWorker; i++ {
				if s.ShouldSample(sampledParent()).Decision == sdktrace.RecordAndSample {
					honored.Add(1)
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	assert.Equal(t, int64(cap), honored.Load(),
		"token bucket must not double-spend under concurrent ShouldSample; observed=%d, cap=%d", honored.Load(), cap)
}
