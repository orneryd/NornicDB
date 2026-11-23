package storage

// Close-out lane L7 (scratch branch only; test-side, not product code).
//
// L-H7-014 (race): TestNativeHandleEntriesRaceClose runs every native handle
// entry while the engine closes. The race report pairs releaseClosedStateLocked
// (badger_stats.go, b.mvccByNamespace = nil under b.mu) with namespaceMVCC
// (badger_mvcc_per_namespace.go, reads/writes b.mvccByNamespace under
// b.mvccByNamespaceMu), reached through the persistMVCCSequence entry.
//   TestL7014OnlyPersistMVCCSequence      the persistMVCCSequence entry alone (minimal reproduction)
//   TestL7014WithoutPersistMVCCSequence   every other entry (bypass of that one entry)
//
// L-H7-020 / L-H7-021 (order): ResetFeatureFlags (TestWAL_FeatureFlagIntegration)
// leaves the WAL flag off; WAL appends then return before writing or checking size.
//   TestL7020AControlVictimsAlone       expected PASS
//   TestL7020BPairFlagsReset            expected FAIL in every victim
//   TestL7020CBypassResetThenWALOn      expected PASS (only the WAL flag re-enabled)
//
// L-H10-003: the snapshot value byte-identity oracle with the minimised fuzz value.

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// ---------------------------------------------------------------- L-H7-014

func l7014Run(t *testing.T, keep func(string) bool) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	failures := make(chan string, 64)
	var running sync.WaitGroup
	var names []string
	for name, entry := range nativeHandleEntries(engine) {
		if !keep(name) {
			continue
		}
		names = append(names, name)
		running.Add(1)
		go func() {
			defer running.Done()
			defer func() {
				if r := recover(); r != nil {
					failures <- fmt.Sprintf("%s used the retired handle: %v", name, r)
				}
			}()
			for !engine.work.Closing() {
				_ = entry()
			}
			_ = entry()
		}()
	}
	t.Logf("L7_014 entries running during Close: %v", names)
	time.Sleep(20 * time.Millisecond)
	closed := make(chan error, 1)
	go func() { closed <- engine.Close() }()
	require.NoError(t, requireWithin(t, "Close with the entries running", closed))
	running.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
}

func TestL7014OnlyPersistMVCCSequence(t *testing.T) {
	l7014Run(t, func(name string) bool { return name == "persistMVCCSequence" })
}

func TestL7014WithoutPersistMVCCSequence(t *testing.T) {
	l7014Run(t, func(name string) bool { return name != "persistMVCCSequence" })
}

// ---------------------------------------------------------------- L-H7-020 / 021

type l7Step struct {
	name string
	fn   func(*testing.T)
}

func l7RunInOrder(t *testing.T, steps ...l7Step) {
	t.Helper()
	t.Cleanup(config.WithWALEnabled())
	t.Cleanup(config.WithEdgeProvenanceEnabled())
	for _, step := range steps {
		t.Logf("L7_STATE before %s: wal=%v edge_provenance=%v", step.name, config.IsWALEnabled(), config.IsEdgeProvenanceEnabled())
		t.Run(step.name, step.fn)
	}
	t.Logf("L7_STATE end: wal=%v edge_provenance=%v", config.IsWALEnabled(), config.IsEdgeProvenanceEnabled())
}

var (
	l7FlagsReset = l7Step{"TestWAL_FeatureFlagIntegration", TestWAL_FeatureFlagIntegration}
	l7WALOn      = l7Step{"EnableWALOnly", func(t *testing.T) { config.EnableWAL() }}
	l7Victims    = []l7Step{
		{"TestManagedVectorAnchorAboveFrameLimitSpansFrames", TestManagedVectorAnchorAboveFrameLimitSpansFrames},
		{"TestManagedVectorAnchorBatchBoundsJournalSyncs", TestManagedVectorAnchorBatchBoundsJournalSyncs},
		{"TestManagedVectorAnchorBatchCancellationLeavesNothingProven", TestManagedVectorAnchorBatchCancellationLeavesNothingProven},
		{"TestManagedVectorAnchorBatchCommitFailureLeavesNoProof", TestManagedVectorAnchorBatchCommitFailureLeavesNoProof},
		{"TestManagedVectorAnchorBatchDoesNotCertifyNewerGeneration", TestManagedVectorAnchorBatchDoesNotCertifyNewerGeneration},
		{"TestManagedVectorAnchorBatchKeepsEachDatabase", TestManagedVectorAnchorBatchKeepsEachDatabase},
		{"TestManagedVectorAnchorReplayRestoresOnlyManagedState", TestManagedVectorAnchorReplayRestoresOnlyManagedState},
		{"TestManagedVectorAnchorWithoutLastPartIsNotPublished", TestManagedVectorAnchorWithoutLastPartIsNotPublished},
		{"TestManagedVectorGenerationAdvancesOnManagedVectorOwners", TestManagedVectorGenerationAdvancesOnManagedVectorOwners},
		{"TestOrdinaryLargeRecordStillRefusedUnderStandardPolicy", TestOrdinaryLargeRecordStillRefusedUnderStandardPolicy},
	}
)

func TestL7020AControlVictimsAlone(t *testing.T) {
	l7RunInOrder(t, l7Victims...)
}

func TestL7020BPairFlagsReset(t *testing.T) {
	l7RunInOrder(t, append([]l7Step{l7FlagsReset}, l7Victims...)...)
}

// Declared last: ResetFeatureFlags also leaves the other feature flags off.
func TestL7020CBypassResetThenWALOn(t *testing.T) {
	l7RunInOrder(t, append([]l7Step{l7FlagsReset, l7WALOn}, l7Victims...)...)
}

// ---------------------------------------------------------------- L-H10-003

func l7RoundTrip(t *testing.T, raw []byte) ([]byte, any) {
	t.Helper()
	reader := bytes.NewReader(raw)
	value, err := decodeStrictTypedValue(msgpack.NewDecoder(reader))
	require.NoError(t, err)
	require.Zero(t, reader.Len())
	var output bytes.Buffer
	encoder := msgpack.NewEncoder(&output).SetSortMapKeys(true)
	require.NoError(t, encoder.Encode(encodeStoredTemporalValue(restoreStoredTemporalValue(value))))
	return output.Bytes(), value
}

func TestL7H10003SnapshotValueIdentity(t *testing.T) {
	signalling := []byte{0x92, 0xca, 0x30, 0x30, 0x30, 0x30, 0xca, 0xff, 0xb1, 0x30, 0x30}
	quiet := []byte{0x92, 0xca, 0x30, 0x30, 0x30, 0x30, 0xca, 0xff, 0xf1, 0x30, 0x30}
	scalar := []byte{0xca, 0xff, 0xb1, 0x30, 0x30}

	// Same body as FuzzSnapshotValueIdentity (snapshot_edge_transcoder_test.go).
	t.Run("repro_float32_array_with_signalling_nan", func(t *testing.T) {
		require.True(t, snapshotValueIdentity(signalling), "the fast path accepts the raw value")
		out, value := l7RoundTrip(t, signalling)
		t.Logf("L7_H10_003 repro decoded=%T in=% x out=% x", value, signalling, out)
		require.Equal(t, signalling, out, "accepted values must survive the actual strict reader and general encoder byte-for-byte")
	})
	t.Run("control_float32_array_with_quiet_nan", func(t *testing.T) {
		require.True(t, snapshotValueIdentity(quiet))
		out, _ := l7RoundTrip(t, quiet)
		require.Equal(t, quiet, out)
	})
	t.Run("control_scalar_signalling_nan", func(t *testing.T) {
		accepted := snapshotValueIdentity(scalar)
		out, value := l7RoundTrip(t, scalar)
		t.Logf("L7_H10_003 scalar accepted=%v decoded=%T in=% x out=% x", accepted, value, scalar, out)
		require.Equal(t, scalar, out)
	})
	// Diagnostics: log only, to place the step that sets the quiet bit.
	t.Run("diag_steps", func(t *testing.T) {
		_, value := l7RoundTrip(t, signalling)
		if floats, ok := value.([]float32); ok && len(floats) == 2 {
			t.Logf("L7_H10_003 MECH decoder: type=[]float32 element1_bits=%#08x", math.Float32bits(floats[1]))
		} else {
			t.Logf("L7_H10_003 MECH decoder: type=%T value=%v", value, value)
		}
		sNaN := math.Float32frombits(0xffb13030)
		var slice bytes.Buffer
		require.NoError(t, msgpack.NewEncoder(&slice).Encode([]float32{math.Float32frombits(0x30303030), sNaN}))
		t.Logf("L7_H10_003 MECH Encode([]float32) = % x", slice.Bytes())
		var iface bytes.Buffer
		require.NoError(t, msgpack.NewEncoder(&iface).Encode([]any{math.Float32frombits(0x30303030), sNaN}))
		t.Logf("L7_H10_003 MECH Encode([]any{float32...}) = % x", iface.Bytes())
		var direct bytes.Buffer
		require.NoError(t, msgpack.NewEncoder(&direct).EncodeFloat32(sNaN))
		t.Logf("L7_H10_003 MECH EncodeFloat32(sNaN) = % x", direct.Bytes())
		var boxed bytes.Buffer
		require.NoError(t, msgpack.NewEncoder(&boxed).Encode(sNaN))
		t.Logf("L7_H10_003 MECH Encode(float32 sNaN) = % x", boxed.Bytes())
		widened := float64(sNaN)
		t.Logf("L7_H10_003 MECH float32(float64(sNaN)) bits=%#08x", math.Float32bits(float32(widened)))
	})
}
