package observability

import (
	"fmt"
	"sync/atomic"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// ParentMode controls how the sampler treats upstream parent-span decisions.
//
// ParentModeNone (the v1 default per TRC-05, KD-05): sampling is a standalone
// TraceIDRatioBased(ratio). The storage layer controls its own trace volume;
// upstream samplers cannot drive NornicDB to 100% volume.
//
// ParentModeCapped (TRC-06): honors upstream sampled=true up to a QPS cap,
// then falls back to the ratio-based sampler for additional sampled-parent
// spans. Operators that want *some* upstream honor without unbounded volume.
//
// ParentModeStrict (TRC-07): full upstream honor. Equivalent to OTel
// ParentBased + TraceIDRatioBased(ratio). Emits a WARN at startup about
// unbounded volume risk.
type ParentMode string

const (
	ParentModeNone   ParentMode = ""
	ParentModeCapped ParentMode = "capped"
	ParentModeStrict ParentMode = "strict"
)

// parseParentMode accepts the values we expose via NORNICDB_TRACE_PARENT_MODE.
// Unknown values return ParentModeNone + an error suitable for a startup WARN
// (we don't panic; an invalid value falls back to the v1 default).
func parseParentMode(raw string) (ParentMode, error) {
	switch raw {
	case "", "none", "off":
		return ParentModeNone, nil
	case "capped":
		return ParentModeCapped, nil
	case "strict":
		return ParentModeStrict, nil
	default:
		return ParentModeNone, fmt.Errorf("unknown parent mode %q (expected none|capped|strict)", raw)
	}
}

// buildSampler returns the root sampler configured per TRC-05..07.
//
//	mode=none   → TraceIDRatioBased(ratio)                 standalone (TRC-05)
//	mode=capped → parentCappedSampler(ratio, maxQPS)        (TRC-06)
//	mode=strict → ParentBased(root: TraceIDRatioBased(ratio)) (TRC-07)
//
// ratio is clamped to [0, 1]. maxQPS is only consulted when mode=capped and
// clamped to a minimum of 1 to avoid a zero-rate token bucket.
func buildSampler(mode ParentMode, ratio float64, maxQPS int) sdktrace.Sampler {
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	base := sdktrace.TraceIDRatioBased(ratio)
	switch mode {
	case ParentModeStrict:
		return sdktrace.ParentBased(base)
	case ParentModeCapped:
		if maxQPS < 1 {
			maxQPS = 1
		}
		return newParentCappedSampler(base, maxQPS)
	default:
		return base
	}
}

// parentCappedSampler honors upstream SpanContext.IsSampled() up to maxQPS
// sampled-parent spans per second, then falls through to the ratio-based
// fallback for the remainder of the window. It never rejects a span that
// would have been sampled by the fallback alone — it only *adds* sampled
// coverage from upstream-honored decisions, subject to the cap.
//
// Implementation is a minute-granularity token bucket (second-boundary
// refills) using atomic.Int64 for a lock-free hot path. Token bucket is
// per-process (one cap for the whole NornicDB binary); cross-replica
// sampling-rate mismatch detection is TRC-24's separate counter.
type parentCappedSampler struct {
	fallback sdktrace.Sampler
	maxQPS   int64
	// windowStart is the UnixNano of the current 1-second window. On refill,
	// tokens are reset to maxQPS and the window rolls forward.
	windowStart atomic.Int64
	tokens      atomic.Int64
	// nowFn is overridable for tests.
	nowFn func() time.Time
}

func newParentCappedSampler(fallback sdktrace.Sampler, maxQPS int) *parentCappedSampler {
	s := &parentCappedSampler{
		fallback: fallback,
		maxQPS:   int64(maxQPS),
		nowFn:    time.Now,
	}
	s.windowStart.Store(time.Now().UnixNano())
	s.tokens.Store(int64(maxQPS))
	return s
}

// ShouldSample implements sdktrace.Sampler.
//
// Precedence:
//  1. No parent OR parent is not sampled → fall through to fallback.
//  2. Parent is sampled AND a token is available in the current second →
//     honor the parent decision (RecordAndSample).
//  3. Parent is sampled AND no token available → fall through to fallback.
func (s *parentCappedSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	parent := trace.SpanContextFromContext(p.ParentContext)
	if !parent.IsValid() || !parent.IsSampled() {
		return s.fallback.ShouldSample(p)
	}
	if s.takeToken() {
		return sdktrace.SamplingResult{
			Decision:   sdktrace.RecordAndSample,
			Tracestate: parent.TraceState(),
		}
	}
	return s.fallback.ShouldSample(p)
}

// Description implements sdktrace.Sampler.
func (s *parentCappedSampler) Description() string {
	return fmt.Sprintf("ParentCapped{maxQPS=%d,fallback=%s}", s.maxQPS, s.fallback.Description())
}

// takeToken attempts to consume one token from the current 1-second window,
// refilling if the window has rolled over. Returns true if a token was taken.
//
// Lock-free via atomic.CompareAndSwap: under contention, only one goroutine
// successfully refills, and all others observe the post-refill state on the
// next load.
func (s *parentCappedSampler) takeToken() bool {
	now := s.nowFn().UnixNano()
	windowSize := int64(time.Second)
	for {
		start := s.windowStart.Load()
		if now-start >= windowSize {
			// Window has rolled; try to refill. Whichever goroutine wins the
			// CAS refills tokens to maxQPS and resets the window start.
			if s.windowStart.CompareAndSwap(start, now) {
				s.tokens.Store(s.maxQPS)
			}
			// Regardless of who won the CAS, fall through to the token load.
		}
		t := s.tokens.Load()
		if t <= 0 {
			return false
		}
		if s.tokens.CompareAndSwap(t, t-1) {
			return true
		}
	}
}
