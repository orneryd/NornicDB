package storage

// H7 verification helper (verify-h7 lane, scratch branch only; test-side, not product code).
// Each TestH7Order* test starts from the process default of the WAL and edge provenance
// feature flags (both enabled, pkg/config/feature_flags.go init), runs existing tests of
// this package in a fixed order as subtests, logs both flags before each step, and restores
// both flags at its end. TestH7OrderPair* are expected to FAIL in their victim step (the
// order dependence the H7 leads report); TestH7OrderControl* are expected to PASS.

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
)

type h7Step struct {
	name string
	fn   func(*testing.T)
}

func h7RunInOrder(t *testing.T, steps ...h7Step) {
	t.Helper()
	t.Cleanup(config.WithWALEnabled())
	t.Cleanup(config.WithEdgeProvenanceEnabled())
	for _, step := range steps {
		t.Logf("H7_STATE before %s: wal=%v edge_provenance=%v", step.name, config.IsWALEnabled(), config.IsEdgeProvenanceEnabled())
		t.Run(step.name, step.fn)
	}
	t.Logf("H7_STATE end: wal=%v edge_provenance=%v", config.IsWALEnabled(), config.IsEdgeProvenanceEnabled())
}

var (
	h7WALOff      = h7Step{"TestWALEngine_WithFeatureFlagDisabled", TestWALEngine_WithFeatureFlagDisabled}
	h7FlagsReset  = h7Step{"TestWAL_FeatureFlagIntegration", TestWAL_FeatureFlagIntegration}
	h7WALBackOn   = h7Step{"TestWriteBehindTotalCountsRecoveryPublication", TestWriteBehindTotalCountsRecoveryPublication}
	h7Buffered    = h7Step{"TestBufferedRecoveryMultipleCommitsPreserveHistory", TestBufferedRecoveryMultipleCommitsPreserveHistory}
	h7Payload     = h7Step{"TestManagedVectorAnchorPayloadKeepsStoredTypes", TestManagedVectorAnchorPayloadKeepsStoredTypes}
	h7Survives    = h7Step{"TestManagedVectorAnchorSurvivesLaterPropertyUpdate", TestManagedVectorAnchorSurvivesLaterPropertyUpdate}
	h7Replay      = h7Step{"TestManagedVectorAnchorReplayRejectsOtherSource", TestManagedVectorAnchorReplayRejectsOtherSource}
	h7EdgeMeta    = h7Step{"TestEdgeMetaAppendGivesEachRecordItsOwnEdgeID", TestEdgeMetaAppendGivesEachRecordItsOwnEdgeID}
	h7MergeBuffer = h7Step{"TestPrepareMergeNodeForWriteBufferedAdmission", TestPrepareMergeNodeForWriteBufferedAdmission}
)

// Controls: the five victims alone, in the default flag state.
func TestH7OrderControlVictimsAlone(t *testing.T) {
	h7RunInOrder(t, h7Buffered, h7Payload, h7Survives, h7Replay, h7EdgeMeta)
}

// L-H7-001, -002, -003, -005: the WAL flag left off by the polluter.
func TestH7OrderPair001(t *testing.T) { h7RunInOrder(t, h7WALOff, h7Buffered) }
func TestH7OrderPair002(t *testing.T) { h7RunInOrder(t, h7WALOff, h7Payload) }
func TestH7OrderPair003(t *testing.T) { h7RunInOrder(t, h7WALOff, h7Survives) }
func TestH7OrderPair005(t *testing.T) { h7RunInOrder(t, h7WALOff, h7Replay) }

// Control: the later test that switches the WAL flag back on (seed 20261120 order).
func TestH7OrderControlWALBackOn(t *testing.T) {
	h7RunInOrder(t, h7WALOff, h7WALBackOn, h7Survives)
}

// Control for L-H7-004: the WAL polluter does not touch edge provenance.
func TestH7OrderControl004WALOffOnly(t *testing.T) { h7RunInOrder(t, h7WALOff, h7EdgeMeta) }

// L-H7-006: no order dependence found in source; repeat it alone 20 times.
func TestH7OrderRepeat006(t *testing.T) {
	steps := make([]h7Step, 0, 20)
	for i := 0; i < 20; i++ {
		steps = append(steps, h7MergeBuffer)
	}
	h7RunInOrder(t, steps...)
}

// L-H7-004: ResetFeatureFlags leaves edge provenance off. Declared last: the reset also
// leaves the other feature flags off, which this helper does not restore.
func TestH7OrderPair004(t *testing.T) { h7RunInOrder(t, h7FlagsReset, h7EdgeMeta) }
