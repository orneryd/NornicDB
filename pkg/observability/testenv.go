package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"golang.org/x/sync/errgroup"
)

// TestEnv carries per-test isolated observability primitives. It is the
// canonical TEST-01 fixture (ADR §2.8.1 / A10b) — every Phase 3+ test
// package SHOULD construct one of these via NewTestEnv(t).
//
// Each TestEnv has:
//   - its own *prometheus.Registry (never DefaultRegisterer);
//   - its own *tracetest.InMemoryExporter wired through SimpleSpanProcessor
//     so emitted spans are visible synchronously (no BSP batching);
//   - its own *slog.Logger using a discard handler (suppresses unless a
//     test explicitly writes against a captured handler);
//   - a *Provider built against those primitives (sampler:
//     sdktrace.AlwaysSample so tests CAN observe spans they emit, unlike
//     the production NeverSample default);
//   - a fresh *Health registry.
//
// Provider.Shutdown is registered on t.Cleanup automatically — callers
// don't need to call it explicitly.
type TestEnv struct {
	Registry *prometheus.Registry
	Exporter *tracetest.InMemoryExporter
	Logger   *slog.Logger
	Provider *Provider
	Health   *Health

	// Buffer is the lazily-allocated record-capture sink. Populated by the
	// first call to CaptureRecords(); nil otherwise. Per D-12 the discard
	// handler stays the default; tests opt-in to capture via CaptureRecords.
	Buffer *bytes.Buffer

	// captureMu serializes Buffer mutation across concurrent loggers (race
	// safety under -race -count=10). The bytes.Buffer itself is not
	// safe for concurrent writes; the slog.JSONHandler uses an internal
	// mutex for its own writes, but having all per-record bytes land
	// atomically requires the JSONHandler's serialization plus our own
	// guard around buffer ownership swaps.
	captureMu sync.Mutex
}

// NewTestEnv constructs an isolated observability environment for one
// test. Race-detector stable across `go test -race -count=10`.
//
// The constructed *Provider uses SimpleSpanProcessor(exp) + AlwaysSample
// rather than the production BSP + NeverSample combination, so tests can
// observe spans synchronously via env.Exporter.GetSpans(). This is a
// test-only path; production code goes through observability.New.
func NewTestEnv(t *testing.T) *TestEnv {
	t.Helper()

	reg := prometheus.NewRegistry()
	// MET-17 prep: same collectors as production newRegistry, so /metrics
	// in tests has the same shape as production.
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	exp := tracetest.NewInMemoryExporter()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	info := ServiceInfo{Name: "nornicdb-test", Version: "0.0.0"}
	res := buildResource(info)

	// OTel→Prom bridge against OUR registry (not DefaultRegisterer — TEST-01).
	bridge, err := otelprom.New(
		otelprom.WithRegisterer(reg),
		otelprom.WithoutUnits(),
		otelprom.WithNamespace("nornicdb_otel"),
	)
	if err != nil {
		t.Fatalf("NewTestEnv: otelprom.New: %v", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(bridge),
		sdkmetric.WithResource(res),
	)

	// SimpleSpanProcessor (NOT BSP) so tests see spans without flushing.
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exp)),
		sdktrace.WithResource(res),
	)

	cfg := DefaultConfig()
	// Tests bind to ephemeral ports.
	cfg.Metrics.Listen = "127.0.0.1:0"
	cfg.Tracing.Enabled = false // exporter is wired manually above

	instanceID, instanceIDSrc := resolveInstanceID(info.NodeID)

	prov := &Provider{
		tracerProvider: tp,
		meterProvider:  mp,
		registry:       reg,
		serviceInfo:    info,
		instanceID:     instanceID,
		instanceIDSrc:  instanceIDSrc,
		metricsEnabled: true,
		cfg:            cfg,
	}

	h := NewHealth()

	t.Cleanup(func() {
		if err := prov.Shutdown(context.Background()); err != nil {
			t.Logf("TestEnv provider shutdown: %v", err)
		}
	})

	return &TestEnv{
		Registry: reg,
		Exporter: exp,
		Logger:   logger,
		Provider: prov,
		Health:   h,
	}
}

// CaptureRecords rewires te.Logger to write JSON records into te.Buffer
// (D-12). Idempotent: subsequent calls are no-ops, preserving any records
// already written. The default discard handler is replaced only on the
// first call so tests can opt-in to capture without resetting state.
//
// Concurrency: the underlying slog.JSONHandler serializes its writes via
// its own internal mutex; te.Buffer is therefore safe for concurrent
// loggers spawned after CaptureRecords returns. Use a sync.Mutex-guarded
// buffer wrapper if you need ordering guarantees across multiple goroutines.
func (te *TestEnv) CaptureRecords() {
	te.captureMu.Lock()
	defer te.captureMu.Unlock()
	if te.Buffer != nil {
		return
	}
	te.Buffer = &bytes.Buffer{}
	te.Logger = slog.New(slog.NewJSONHandler(&lockedWriter{w: te.Buffer}, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

// LoggedRecords parses the captured buffer line-by-line into a slice of
// JSON-decoded maps. Tolerates an empty buffer (returns nil) and skips
// blank trailing lines. Each call re-parses the buffer so tests CAN call
// it multiple times if they wish to observe streaming.
func (te *TestEnv) LoggedRecords() []map[string]any {
	te.captureMu.Lock()
	defer te.captureMu.Unlock()
	if te.Buffer == nil {
		return nil
	}
	raw := te.Buffer.Bytes()
	if len(raw) == 0 {
		return nil
	}
	var out []map[string]any
	for _, line := range bytes.Split(bytes.TrimRight(raw, "\n"), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			continue // skip malformed lines (e.g. partial concurrent writes)
		}
		out = append(out, rec)
	}
	return out
}

// lockedWriter serializes Write calls so concurrent loggers cannot
// interleave bytes mid-record. slog.JSONHandler already serializes within
// a single Logger but multi-goroutine fan-in into the same handler is the
// stress case under -race -count=10.
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (lw *lockedWriter) Write(p []byte) (int, error) {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	return lw.w.Write(p)
}

// CardinalityT is the minimal *testing.T-shaped surface that
// AssertCardinalityCeiling consumes. *testing.T satisfies it transparently;
// negative-falsifiability sub-tests can plug in an in-package fake that
// captures Errorf/FailNow without propagating failure into the parent
// *testing.T (Go's c.Fail() unconditionally walks c.parent.Fail() — there
// is no way to scope a *testing.T failure to a sub-test alone).
//
// Mirrors github.com/stretchr/testify/require.TestingT plus Helper(); kept
// local so we don't take a public dependency on require's internal type.
type CardinalityT interface {
	Helper()
	Errorf(format string, args ...interface{})
	FailNow()
}

// AssertCardinalityCeiling exercises the named *Vec across 1000 deterministic
// synthetic tenant UUIDs concurrently across 8 goroutines, then asserts the
// test's isolated registry observes <= ceiling distinct series for `name`.
// Implements TEST-02 (CONTEXT D-04 / D-04a / D-04c).
//
// Called once per *Vec from Phase 4 subsystem tests; the "1000 UUIDs +
// 8-goroutine drive" knowledge lives ONCE here per AGENTS.md §7 (DRY).
//
// API choice: caller-supplied `drive` callback (Pattern A in RESEARCH §4).
// Keeps testenv.go agnostic of subsystem label shapes — a *Vec with
// labels []string{"database","op_type","result"} is driven via
//
//	te.AssertCardinalityCeiling(t, name, ceiling, func(tenant string) {
//	    cv.WithLabelValues(tenant, "read", "success").Inc()
//	})
//
// — and a *Vec with labels []string{"database"} is driven via
//
//	te.AssertCardinalityCeiling(t, name, ceiling, func(tenant string) {
//	    cv.WithLabelValues(tenant).Inc()
//	})
//
// Race-safety:
//   - errgroup.SetLimit(8) caps fan-out so -race -count=N -parallel=M
//     doesn't explode goroutine counts (D-04c).
//   - client_golang.HistogramVec.WithLabelValues / CounterVec.WithLabelValues
//     are documented race-safe (RESEARCH §1/§4 — internal sharded sync.Mutex
//     per label-set hash).
//   - The helper holds no state across calls; each invocation builds a
//     fresh errgroup.
//
// API note (RESEARCH §4 CRITICAL CORRECTION):
//
//	testutil.GatherAndCount takes a Gatherer (*prometheus.Registry implements).
//	DO NOT use testutil.CollectAndCount — that takes a Collector (it builds
//	its own pedantic registry internally) and will not compile against
//	*prometheus.Registry. CONTEXT.md draft shorthand `CollectAndCount(reg, …)`
//	was misleading; this helper uses the correct API.
//
// Signature note (Rule 1 deviation from literal D-04 form):
//
//	The parameter is typed as the small CardinalityT interface rather than
//	the concrete *testing.T. Production callers pass *testing.T transparently
//	(it satisfies the interface). Negative-falsifiability sub-tests pass an
//	in-package fake to capture the helper's t.FailNow() call without Go's
//	t.Run-propagates-failure-to-parent semantics tripping the parent test.
//	(Go testing.go:962 c.Fail() unconditionally propagates via c.parent.Fail();
//	there is no way to assert "this helper called Fatalf" from within a
//	*testing.T-typed sub-test without parent contamination.)
//	Plan 03-04 acceptance criterion grep for the literal `*testing.T` form is
//	relaxed to the equivalent `CardinalityT` interface.
func (te *TestEnv) AssertCardinalityCeiling(t CardinalityT, name string, ceiling int, drive func(tenant string)) {
	t.Helper()

	var g errgroup.Group
	g.SetLimit(8)

	for i := 0; i < 1000; i++ {
		i := i
		g.Go(func() error {
			tenant := uuid.NewMD5(uuid.NameSpaceDNS, []byte(strconv.Itoa(i))).String()
			drive(tenant)
			return nil
		})
	}
	require.NoError(t, g.Wait())

	got, err := testutil.GatherAndCount(te.Registry, name)
	require.NoError(t, err)
	require.LessOrEqualf(t, got, ceiling,
		"metric %q has %d distinct series, exceeds cardinality ceiling %d",
		name, got, ceiling)
}
