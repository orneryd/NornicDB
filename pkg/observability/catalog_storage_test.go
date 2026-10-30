package observability

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Plan 04-04-03 GREEN: StorageMetrics bag with 8 families. Closed enums
// for kind / op / index labels per CONTEXT D-13c. Tenant flag (D-08)
// forward-compat for index_rebuild_total{database, ...} only — op_duration
// stays tenant-agnostic per D-08a (database is on index_rebuild only).

// storageProbeStub is the test seam for the StorageProbe accessors used by
// the nodes_total / edges_total GaugeFunc callbacks.
type storageProbeStub struct {
	nodes         int64
	edges         int64
	counterNodes  uint64
	counterEdges  uint64
	freelistNodes int64
	freelistEdges int64
}

func (s storageProbeStub) NodeCount() int64           { return s.nodes }
func (s storageProbeStub) EdgeCount() int64           { return s.edges }
func (s storageProbeStub) IDDictCounterNodes() uint64 { return s.counterNodes }
func (s storageProbeStub) IDDictCounterEdges() uint64 { return s.counterEdges }
func (s storageProbeStub) IDDictFreelistNodes() int64 { return s.freelistNodes }
func (s storageProbeStub) IDDictFreelistEdges() int64 { return s.freelistEdges }

// TestStorageMetrics_RegistersEight asserts that all 8 storage families
// surface from a single Gather().
func TestStorageMetrics_RegistersEight(t *testing.T) {
	te := NewTestEnv(t)
	probe := storageProbeStub{nodes: 100, edges: 200}
	bag := NewStorageMetrics(te.Registry, false, probe)
	require.NotNil(t, bag)

	// Touch *Vec families so they appear in Gather (vectors only surface
	// with ≥1 child series).
	bag.Bytes.WithLabelValues("nodes").Set(0)
	bag.OpDuration.Bind("get").Observe(nil, 0.001)
	bag.CompactionsTotal.WithLabelValues("0", "success").Inc()
	bag.CompactionDuration.Bind("0").Observe(nil, 0.001)
	bag.IndexRebuildTotal.WithLabelValues("label", "success").Inc()
	bag.WALLagBytes.Set(0)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)

	want := []string{
		"nornicdb_storage_nodes_total",
		"nornicdb_storage_edges_total",
		"nornicdb_storage_bytes",
		"nornicdb_storage_op_duration_seconds",
		"nornicdb_storage_compactions_total",
		"nornicdb_storage_compaction_duration_seconds",
		"nornicdb_storage_wal_lag_bytes",
		"nornicdb_storage_index_rebuild_total",
	}
	for _, name := range want {
		found := false
		for _, mf := range mfs {
			if mf.GetName() == name {
				found = true
				break
			}
		}
		require.Truef(t, found, "family %s missing from Gather()", name)
	}
}

// TestStorageBytes_KindClosedEnum asserts MET-10 + CONTEXT D-07: kind only
// accepts {nodes, edges, index, wal, search}. Cardinality ceiling = 5.
func TestStorageBytes_KindClosedEnum(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewStorageMetrics(te.Registry, false, storageProbeStub{})
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_storage_bytes", 5, func(tenant string) {
		for _, kind := range AllowedStorageBytesKinds {
			bag.Bytes.WithLabelValues(kind).Set(1.0)
		}
		_ = tenant
	})
}

// TestStorageOp_ClosedEnum asserts the closed op enum {get, put, delete,
// scan} for the op_duration_seconds histogram. Cardinality ceiling = 4.
func TestStorageOp_ClosedEnum(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewStorageMetrics(te.Registry, false, storageProbeStub{})
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_storage_op_duration_seconds", 4, func(tenant string) {
		for _, op := range AllowedStorageOps {
			bag.OpDuration.Bind(op).Observe(nil, 0.001)
		}
		_ = tenant
	})
}

// TestIndexRebuild_ClosedEnum asserts CONTEXT D-13c: index ∈ {label,
// edge_between, temporal, embedding, user_created}. Without tenant flag
// cardinality ceiling = 5 indices × 3 results = 15.
func TestIndexRebuild_ClosedEnum(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewStorageMetrics(te.Registry, false, storageProbeStub{})
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_storage_index_rebuild_total", 15, func(tenant string) {
		for _, idx := range AllowedStorageIndexes {
			for _, res := range AllowedStorageResults {
				bag.IndexRebuildTotal.WithLabelValues(idx, res).Inc()
			}
		}
		_ = tenant
	})
}

// TestNodesEdgesGaugeFunc asserts that the nodes_total / edges_total
// GaugeFuncs read the StorageProbe accessors at Gather() time.
func TestNodesEdgesGaugeFunc(t *testing.T) {
	te := NewTestEnv(t)
	probe := storageProbeStub{nodes: 100, edges: 200}
	bag := NewStorageMetrics(te.Registry, false, probe)
	require.NotNil(t, bag)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)

	got := map[string]float64{}
	for _, mf := range mfs {
		switch mf.GetName() {
		case "nornicdb_storage_nodes_total", "nornicdb_storage_edges_total":
			require.Len(t, mf.Metric, 1)
			got[mf.GetName()] = mf.Metric[0].GetGauge().GetValue()
		}
	}
	require.InDelta(t, 100.0, got["nornicdb_storage_nodes_total"], 0.0001)
	require.InDelta(t, 200.0, got["nornicdb_storage_edges_total"], 0.0001)
}

// TestStorageGaugeFuncs_PanicSafe verifies RESEARCH RISK-8 / Pitfall 1:
// when the probe accessors panic, the GaugeFunc callbacks recover and
// return 0 so the scrape does not 500.
type panickyStorageProbe struct{}

func (p panickyStorageProbe) NodeCount() int64           { panic("nodes panic") }
func (p panickyStorageProbe) EdgeCount() int64           { panic("edges panic") }
func (p panickyStorageProbe) IDDictCounterNodes() uint64 { panic("id-dict counter nodes panic") }
func (p panickyStorageProbe) IDDictCounterEdges() uint64 { panic("id-dict counter edges panic") }
func (p panickyStorageProbe) IDDictFreelistNodes() int64 { panic("id-dict freelist nodes panic") }
func (p panickyStorageProbe) IDDictFreelistEdges() int64 { panic("id-dict freelist edges panic") }

func TestStorageGaugeFuncs_PanicSafe(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewStorageMetrics(te.Registry, false, panickyStorageProbe{})
	require.NotNil(t, bag)
	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		switch mf.GetName() {
		case "nornicdb_storage_nodes_total", "nornicdb_storage_edges_total":
			require.Len(t, mf.Metric, 1)
			require.Equal(t, 0.0, mf.Metric[0].GetGauge().GetValue())
		}
	}
}

// TestTenantFlag_LabelOmit_Storage exercises the D-08 parameterized
// cardinality axis. With tenant=ON, index_rebuild has shape {database,
// index, result} so cardinality ceiling rises by the database axis.
func TestTenantFlag_LabelOmit_Storage(t *testing.T) {
	t.Run("tenant_off", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewStorageMetrics(te.Registry, false, storageProbeStub{})
		require.NotNil(t, bag)
		// 5 indices × 3 results = 15 (tenant flag drops database label).
		te.AssertCardinalityCeiling(t, "nornicdb_storage_index_rebuild_total", 15, func(tenant string) {
			for _, idx := range AllowedStorageIndexes {
				for _, res := range AllowedStorageResults {
					bag.IndexRebuildTotal.WithLabelValues(idx, res).Inc()
				}
			}
			_ = tenant
		})
	})
	t.Run("tenant_on", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewStorageMetrics(te.Registry, true, storageProbeStub{})
		require.NotNil(t, bag)
		// With tenant=ON the bag carries a database label. We drive a
		// FIXED-tenant single-database workload so the ceiling check
		// remains 5 × 3 = 15 — proving the label shape accepts the
		// database arg without mistakenly multiplying cardinality.
		te.AssertCardinalityCeiling(t, "nornicdb_storage_index_rebuild_total", 15, func(tenant string) {
			for _, idx := range AllowedStorageIndexes {
				for _, res := range AllowedStorageResults {
					bag.IndexRebuildTotal.WithLabelValues("nornic" /* fixed db */, idx, res).Inc()
				}
			}
			_ = tenant
		})
	})
}
