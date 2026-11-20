package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// NOR279 reproduction (reproduction only; production code is unchanged).
//
// Reported scope: EdgeMetaStore Append/Import ingress and GetHistory/GetLatest/
// Export egress must not share nested metadata values with the retained
// evidence, and NodeConfigStore Set/Import ingress plus Get/GetOrCreate/Export
// egress must detach PinList, DenyList, LabelConfigs and Metadata, including
// nested maps and slices. Ordering and value fidelity must survive an
// export/import round trip. Every case below drives only the public API.
//
// The shared metadata fixture covers each reference shape the reported scope
// names:
//
//	origin_file -> string                      (top-level scalar)
//	nested      -> map[string]interface{}      (nested map)
//	tags        -> []interface{}               (nested slice)
//	holders     -> []map[string]interface{}    (nested map slice)
//
// Scalar struct fields are already copied by value, so the cases include a
// scalar control that passes today; the failures are confined to nested
// reference values.

func nor279NestedMetadata() map[string]interface{} {
	return map[string]interface{}{
		"origin_file": "docs/design.md",
		"nested": map[string]interface{}{
			"retries": 3,
			"tags":    []string{"evidence", "audit"},
		},
		"tags":    []interface{}{"alpha", "beta"},
		"holders": []map[string]interface{}{{"tenant": "t1", "region": "eu"}},
	}
}

// nor279MutateNestedMetadata writes through every reference member reachable
// from md, so any holder still aliasing it observes the write.
func nor279MutateNestedMetadata(t *testing.T, md map[string]interface{}) {
	t.Helper()
	if md == nil {
		return
	}

	md["origin_file"] = "mutated"
	md["injected"] = "mutated"

	nested, ok := md["nested"].(map[string]interface{})
	require.Truef(t, ok, "NOR279 fixture: nested map has unexpected shape %T", md["nested"])
	nested["retries"] = 99

	nestedTags, ok := nested["tags"].([]string)
	require.Truef(t, ok, "NOR279 fixture: nested string slice has unexpected shape %T", nested["tags"])
	nestedTags[0] = "mutated"

	tags, ok := md["tags"].([]interface{})
	require.Truef(t, ok, "NOR279 fixture: top-level slice has unexpected shape %T", md["tags"])
	tags[0] = "mutated"

	holders, ok := md["holders"].([]map[string]interface{})
	require.Truef(t, ok, "NOR279 fixture: nested map slice has unexpected shape %T", md["holders"])
	holders[0]["tenant"] = "mutated"
}

// nor279DeepCopyMetadata returns an independent copy of a fixture metadata
// map. Expectations must not share nested values with any value under test, or
// a write through the value under test would also change the expectation.
func nor279DeepCopyMetadata(md map[string]interface{}) map[string]interface{} {
	if md == nil {
		return nil
	}
	out := make(map[string]interface{}, len(md))
	for k, v := range md {
		out[k] = nor279DeepCopyFixtureValue(v)
	}
	return out
}

// nor279DeepCopyFixtureValue copies exactly the value shapes the metadata
// fixture builds; scalars are immutable and are shared by value.
func nor279DeepCopyFixtureValue(v interface{}) interface{} {
	switch value := v.(type) {
	case map[string]interface{}:
		return nor279DeepCopyMetadata(value)
	case []interface{}:
		out := make([]interface{}, len(value))
		for i := range value {
			out[i] = nor279DeepCopyFixtureValue(value[i])
		}
		return out
	case []string:
		out := make([]string, len(value))
		copy(out, value)
		return out
	case []map[string]interface{}:
		out := make([]map[string]interface{}, len(value))
		for i := range value {
			out[i] = nor279DeepCopyMetadata(value[i])
		}
		return out
	default:
		return v
	}
}

// --- EdgeMetaStore ----------------------------------------------------------

var nor279EdgeBaseTime = time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)

// nor279EdgeMetaRecord builds a fully deterministic record; every record
// shares the same edge key so per-key ordering is observable.
func nor279EdgeMetaRecord(i int) EdgeMeta {
	metadata := nor279NestedMetadata()
	metadata["record"] = i
	return EdgeMeta{
		EdgeID:        fmt.Sprintf("edge-%d", i),
		Src:           "node-a",
		Dst:           "node-b",
		Label:         "relates_to",
		Score:         0.5 + float64(i)/10,
		SignalType:    string(SignalSimilarity),
		EvidenceCount: i + 1,
		Origin:        fmt.Sprintf("origin-%d", i),
		SessionID:     "session-1",
		Materialized:  i%2 == 0,
		Timestamp:     nor279EdgeBaseTime.Add(time.Duration(i) * time.Minute),
		Metadata:      metadata,
	}
}

func nor279EdgeMetaRecords(n int) []EdgeMeta {
	records := make([]EdgeMeta, n)
	for i := range records {
		records[i] = nor279EdgeMetaRecord(i)
	}
	return records
}

func nor279RequireEdgeMeta(t *testing.T, got, want *EdgeMeta, label string) {
	t.Helper()
	require.NotNilf(t, got, "%s: record missing", label)

	gotFields, wantFields := *got, *want
	gotMetadata, wantMetadata := gotFields.Metadata, wantFields.Metadata
	gotFields.Metadata, wantFields.Metadata = nil, nil

	require.Equalf(t, wantFields, gotFields, "%s: record fields not preserved", label)
	require.Equalf(t, wantMetadata, gotMetadata,
		"%s: metadata must preserve the written shape and must not observe writes through another holder", label)
}

// nor279RequireEdgeMetaStoreState asserts GetHistory order and fidelity,
// GetLatest identity and Export order/fidelity of the retained evidence.
func nor279RequireEdgeMetaStoreState(t *testing.T, store *EdgeMetaStore, ctx context.Context, want []EdgeMeta, label string) {
	t.Helper()
	require.NotEmptyf(t, want, "%s: expectation set must not be empty", label)

	history, err := store.GetHistory(ctx, want[0].Src, want[0].Dst, want[0].Label)
	require.NoErrorf(t, err, "%s: GetHistory returned an error", label)
	require.Lenf(t, history, len(want), "%s: GetHistory must return every record for the edge in append order", label)
	for i := range want {
		nor279RequireEdgeMeta(t, history[i], &want[i], fmt.Sprintf("%s: GetHistory[%d]", label, i))
	}

	latest, err := store.GetLatest(ctx, want[0].Src, want[0].Dst, want[0].Label)
	require.NoErrorf(t, err, "%s: GetLatest returned an error", label)
	nor279RequireEdgeMeta(t, latest, &want[len(want)-1], label+": GetLatest")

	exported := store.Export()
	require.Lenf(t, exported, len(want), "%s: Export must return every record in insert order", label)
	for i := range want {
		nor279RequireEdgeMeta(t, exported[i], &want[i], fmt.Sprintf("%s: Export[%d]", label, i))
	}
}

// nor279SeedEdgeMetaStore writes the first three deterministic records and
// leaves them unmodified afterwards.
func nor279SeedEdgeMetaStore(t *testing.T, ctx context.Context) *EdgeMetaStore {
	t.Helper()
	store := NewEdgeMetaStore()
	for _, record := range nor279EdgeMetaRecords(3) {
		require.NoError(t, store.Append(ctx, record))
	}
	return store
}

type nor279EdgeMetaPath int

const (
	nor279EdgeAppend nor279EdgeMetaPath = iota
	nor279EdgeImport
	nor279EdgeGetHistory
	nor279EdgeGetLatest
	nor279EdgeExport
	nor279EdgeRoundTrip
)

func TestNor279EdgeMetaStoreNestedMetadataOwnership(t *testing.T) {
	defer config.WithEdgeProvenanceEnabled()()

	ctx := context.Background()
	cases := []struct {
		name string
		path nor279EdgeMetaPath
	}{
		{"AppendDetachesCallerMetadata", nor279EdgeAppend},
		{"ImportDetachesCallerMetadata", nor279EdgeImport},
		{"GetHistoryDetachesReturnedMetadata", nor279EdgeGetHistory},
		{"GetLatestDetachesReturnedMetadata", nor279EdgeGetLatest},
		{"ExportDetachesReturnedMetadata", nor279EdgeExport},
		{"ExportImportRoundTripPreservesNestedMetadata", nor279EdgeRoundTrip},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nor279RunEdgeMetaCase(t, ctx, tc.path)
		})
	}
}

func nor279RunEdgeMetaCase(t *testing.T, ctx context.Context, path nor279EdgeMetaPath) {
	t.Helper()

	switch path {
	case nor279EdgeAppend:
		store := nor279SeedEdgeMetaStore(t, ctx)
		ingress := nor279EdgeMetaRecord(3)
		want := nor279EdgeMetaRecords(4)
		require.NoError(t, store.Append(ctx, ingress))

		// Scalar control: value fields are already copied, which localizes the
		// failure to the nested reference members.
		ingress.Score = 0.99
		history, err := store.GetHistory(ctx, ingress.Src, ingress.Dst, ingress.Label)
		require.NoError(t, err)
		require.Len(t, history, 4)
		require.Equalf(t, want[3].Score, history[3].Score,
			"Append: scalar field ingress must not be observable after the write")

		nor279MutateNestedMetadata(t, ingress.Metadata)
		nor279RequireEdgeMetaStoreState(t, store, ctx, want, "Append: caller mutation after write")

	case nor279EdgeImport:
		store := nor279SeedEdgeMetaStore(t, ctx)
		ingress := nor279EdgeMetaRecord(3)
		want := nor279EdgeMetaRecords(4)
		require.Equal(t, 1, store.Import([]*EdgeMeta{&ingress}), "Import must report the imported record count")
		require.Equal(t, 4, store.Size(), "Import must append to the retained records")

		ingress.EdgeID = "mutated"
		require.Equalf(t, want[3].EdgeID, store.Export()[3].EdgeID,
			"Import: imported record identity must be copied")

		nor279MutateNestedMetadata(t, ingress.Metadata)
		nor279RequireEdgeMetaStoreState(t, store, ctx, want, "Import: caller mutation after import")

	case nor279EdgeGetHistory:
		store := nor279SeedEdgeMetaStore(t, ctx)
		want := nor279EdgeMetaRecords(3)

		history, err := store.GetHistory(ctx, want[0].Src, want[0].Dst, want[0].Label)
		require.NoError(t, err)
		require.Len(t, history, 3)
		for i := range want {
			nor279RequireEdgeMeta(t, history[i], &want[i], fmt.Sprintf("GetHistory pristine[%d]", i))
		}

		nor279MutateNestedMetadata(t, history[0].Metadata)
		// A returned record must also be independent from the other returned
		// records, not only from the store.
		nor279RequireEdgeMeta(t, history[1], &want[1],
			"GetHistory: mutation of one result must not affect another result")

		nor279RequireEdgeMetaStoreState(t, store, ctx, want, "GetHistory: caller mutation of a returned record")

	case nor279EdgeGetLatest:
		store := nor279SeedEdgeMetaStore(t, ctx)
		want := nor279EdgeMetaRecords(3)

		latest, err := store.GetLatest(ctx, want[0].Src, want[0].Dst, want[0].Label)
		require.NoError(t, err)
		nor279RequireEdgeMeta(t, latest, &want[2], "GetLatest pristine")

		nor279MutateNestedMetadata(t, latest.Metadata)
		nor279RequireEdgeMetaStoreState(t, store, ctx, want, "GetLatest: caller mutation of the returned record")

	case nor279EdgeExport:
		store := nor279SeedEdgeMetaStore(t, ctx)
		want := nor279EdgeMetaRecords(3)

		exported := store.Export()
		require.Len(t, exported, 3)
		for i := range want {
			nor279RequireEdgeMeta(t, exported[i], &want[i], fmt.Sprintf("Export pristine[%d]", i))
		}

		exported[0].EdgeID = "mutated"
		require.Equalf(t, want[0].EdgeID, store.Export()[0].EdgeID,
			"Export: exported record fields must be copies")

		nor279MutateNestedMetadata(t, exported[1].Metadata)
		nor279RequireEdgeMetaStoreState(t, store, ctx, want, "Export: caller mutation of an exported record")

	case nor279EdgeRoundTrip:
		source := nor279SeedEdgeMetaStore(t, ctx)
		want := nor279EdgeMetaRecords(3)
		exported := source.Export()

		dest := NewEdgeMetaStore()
		require.Equal(t, 3, dest.Import(exported), "Import must report the imported record count")

		// Fidelity and ordering before any mutation.
		nor279RequireEdgeMetaStoreState(t, dest, ctx, want, "Export/Import round trip fidelity")
		require.Equal(t, source.Stats().TotalRecords, dest.Stats().TotalRecords,
			"Export/Import round trip must preserve the total record count")
		require.Equal(t, source.Stats().TotalMaterialized, dest.Stats().TotalMaterialized,
			"Export/Import round trip must preserve the materialized record count")
		require.Equal(t, source.UniqueEdgeCount(), dest.UniqueEdgeCount(),
			"Export/Import round trip must preserve the unique edge count")

		nor279MutateNestedMetadata(t, exported[0].Metadata)
		nor279RequireEdgeMetaStoreState(t, source, ctx, want,
			"Export/Import round trip: mutating an exported record must not affect the source store")
		nor279RequireEdgeMetaStoreState(t, dest, ctx, want,
			"Export/Import round trip: mutating the imported slice must not affect the importing store")
	}
}

func TestNor279EdgeMetaNilAndEmptyMetadataControls(t *testing.T) {
	defer config.WithEdgeProvenanceEnabled()()

	ctx := context.Background()
	cases := []struct {
		name     string
		metadata map[string]interface{}
	}{
		{"NilMetadata", nil},
		{"EmptyMetadata", map[string]interface{}{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := NewEdgeMetaStore()
			record := nor279EdgeMetaRecord(0)
			record.Metadata = tc.metadata
			require.NoError(t, store.Append(ctx, record))

			history, err := store.GetHistory(ctx, record.Src, record.Dst, record.Label)
			require.NoError(t, err)
			require.Len(t, history, 1)
			require.Empty(t, history[0].Metadata, "GetHistory: nil/empty metadata must stay empty")

			latest, err := store.GetLatest(ctx, record.Src, record.Dst, record.Label)
			require.NoError(t, err)
			require.NotNil(t, latest)
			require.Empty(t, latest.Metadata, "GetLatest: nil/empty metadata must stay empty")

			exported := store.Export()
			require.Len(t, exported, 1)
			require.Empty(t, exported[0].Metadata, "Export: nil/empty metadata must stay empty")

			dest := NewEdgeMetaStore()
			require.Equal(t, 1, dest.Import(exported), "Import must round trip a nil/empty metadata record")
			roundTripped, err := dest.GetHistory(ctx, record.Src, record.Dst, record.Label)
			require.NoError(t, err)
			require.Len(t, roundTripped, 1)
			require.Empty(t, roundTripped[0].Metadata, "Export/Import: nil/empty metadata must stay empty")

			// Writing through a returned empty map must not reach retained
			// state. A nil map cannot be written, so the guard leaves the nil
			// control as a no-panic/empty assertion.
			if roundTripped[0].Metadata != nil {
				roundTripped[0].Metadata["injected"] = "mutated"
				recheck, err := dest.GetHistory(ctx, record.Src, record.Dst, record.Label)
				require.NoError(t, err)
				require.Len(t, recheck, 1)
				require.Empty(t, recheck[0].Metadata,
					"empty metadata control: writes through a returned map must not be retained")
			}
		})
	}
}

// --- NodeConfigStore --------------------------------------------------------

func nor279NodeConfigFixture(nodeID string) *NodeConfig {
	cfg := NewNodeConfig(nodeID)
	cfg.CreatedAt = time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)
	cfg.UpdatedAt = cfg.CreatedAt
	cfg.PinList = []string{"pin-a", "pin-b"}
	cfg.DenyList = []string{"deny-a", "deny-b"}
	cfg.LabelConfigs = map[string]LabelConfig{
		"relates_to": {Label: "relates_to", MaxEdges: 5, MinConfidence: 0.7, Cooldown: 30 * time.Second},
		"cites":      {Label: "cites", Disabled: true},
	}
	cfg.MaxOutEdges = 50
	cfg.MaxInEdges = 10
	cfg.MaxTotalEdges = 60
	cfg.TrustLevel = TrustLevelHigh
	cfg.MinConfidence = 0.35
	cfg.Cooldown = 5 * time.Second
	cfg.Metadata = nor279NestedMetadata()
	return cfg
}

// nor279NodeConfigExpectation returns an independent expectation built from
// seed, so a write through any value under test cannot alter it.
func nor279NodeConfigExpectation(seed *NodeConfig) *NodeConfig {
	want := *seed
	want.PinList = append([]string(nil), seed.PinList...)
	want.DenyList = append([]string(nil), seed.DenyList...)
	want.LabelConfigs = make(map[string]LabelConfig, len(seed.LabelConfigs))
	for k, v := range seed.LabelConfigs {
		want.LabelConfigs[k] = v
	}
	want.Metadata = nor279DeepCopyMetadata(seed.Metadata)
	return &want
}

// nor279MutateNodeConfig writes through every reference member of a fixture
// config. The fixture supplies non-empty lists and maps.
func nor279MutateNodeConfig(t *testing.T, cfg *NodeConfig) {
	t.Helper()
	require.NotNilf(t, cfg, "NOR279 fixture: config missing")

	cfg.PinList[0] = "mutated"
	cfg.PinList = append(cfg.PinList, "mutated")
	cfg.DenyList[0] = "mutated"
	cfg.DenyList = append(cfg.DenyList, "mutated")
	cfg.LabelConfigs["relates_to"] = LabelConfig{Label: "relates_to", MaxEdges: 99}
	cfg.LabelConfigs["injected"] = LabelConfig{Label: "injected"}
	nor279MutateNestedMetadata(t, cfg.Metadata)
}

// nor279RequireNodeConfig compares every retained field. UpdatedAt is excluded
// because Set intentionally stamps the write time; round-trip cases assert it
// separately.
func nor279RequireNodeConfig(t *testing.T, got, want *NodeConfig, label string) {
	t.Helper()
	require.NotNilf(t, got, "%s: config missing", label)

	require.Equalf(t, want.NodeID, got.NodeID, "%s: NodeID", label)
	require.Equalf(t, want.CreatedAt, got.CreatedAt, "%s: CreatedAt", label)
	require.Equalf(t, want.PinList, got.PinList, "%s: PinList must preserve order and values", label)
	require.Equalf(t, want.DenyList, got.DenyList, "%s: DenyList must preserve order and values", label)
	require.Equalf(t, want.LabelConfigs, got.LabelConfigs, "%s: LabelConfigs must preserve every entry", label)
	require.Equalf(t, want.Metadata, got.Metadata,
		"%s: Metadata must preserve nested maps/slices and must not observe writes through another holder", label)
	require.Equalf(t, want.MaxOutEdges, got.MaxOutEdges, "%s: MaxOutEdges", label)
	require.Equalf(t, want.MaxInEdges, got.MaxInEdges, "%s: MaxInEdges", label)
	require.Equalf(t, want.MaxTotalEdges, got.MaxTotalEdges, "%s: MaxTotalEdges", label)
	require.Equalf(t, want.CurrentOutEdges, got.CurrentOutEdges, "%s: CurrentOutEdges", label)
	require.Equalf(t, want.CurrentInEdges, got.CurrentInEdges, "%s: CurrentInEdges", label)
	require.Equalf(t, want.CurrentTotalEdges, got.CurrentTotalEdges, "%s: CurrentTotalEdges", label)
	require.Equalf(t, want.TrustLevel, got.TrustLevel, "%s: TrustLevel", label)
	require.Equalf(t, want.MinConfidence, got.MinConfidence, "%s: MinConfidence", label)
	require.Equalf(t, want.Cooldown, got.Cooldown, "%s: Cooldown", label)
	require.Equalf(t, want.Disabled, got.Disabled, "%s: Disabled", label)
}

// nor279FindNodeConfig locates a config in an Export result, whose order is
// map-iteration order and therefore not asserted.
func nor279FindNodeConfig(t *testing.T, configs []*NodeConfig, nodeID string) *NodeConfig {
	t.Helper()
	for _, cfg := range configs {
		if cfg != nil && cfg.NodeID == nodeID {
			return cfg
		}
	}
	require.FailNowf(t, "NOR279: config missing from export", "nodeID=%s", nodeID)
	return nil
}

type nor279NodeConfigPath int

const (
	nor279NodeSet nor279NodeConfigPath = iota
	nor279NodeImport
	nor279NodeGet
	nor279NodeGetOrCreate
	nor279NodeExportPath
	nor279NodeRoundTrip
)

func TestNor279NodeConfigStoreNestedOwnership(t *testing.T) {
	cases := []struct {
		name string
		path nor279NodeConfigPath
	}{
		{"SetDetachesCallerConfig", nor279NodeSet},
		{"ImportDetachesCallerConfig", nor279NodeImport},
		{"GetDetachesReturnedConfig", nor279NodeGet},
		{"GetOrCreateDetachesReturnedConfig", nor279NodeGetOrCreate},
		{"ExportDetachesReturnedConfig", nor279NodeExportPath},
		{"ExportImportRoundTripPreservesNestedFields", nor279NodeRoundTrip},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nor279RunNodeConfigCase(t, tc.path)
		})
	}
}

func nor279RunNodeConfigCase(t *testing.T, path nor279NodeConfigPath) {
	t.Helper()

	switch path {
	case nor279NodeSet:
		store := NewNodeConfigStore()
		ingress := nor279NodeConfigFixture("node-set")
		want := nor279NodeConfigExpectation(ingress)
		store.Set(*ingress)

		// Scalar control: the value parameter already isolates scalars.
		ingress.MaxOutEdges = 1
		require.Equalf(t, want.MaxOutEdges, store.Get("node-set").MaxOutEdges,
			"Set: scalar field ingress must not be observable after the write")

		nor279MutateNodeConfig(t, ingress)
		nor279RequireNodeConfig(t, store.Get("node-set"), want, "Set: caller mutation after write")
		nor279RequireNodeConfig(t, nor279FindNodeConfig(t, store.Export(), "node-set"), want,
			"Set: caller mutation after write (Export)")

	case nor279NodeImport:
		store := NewNodeConfigStore()
		ingress := nor279NodeConfigFixture("node-import")
		want := nor279NodeConfigExpectation(ingress)
		require.Equal(t, 1, store.Import([]*NodeConfig{ingress}), "Import must report the imported config count")

		ingress.NodeID = "mutated"
		require.NotNilf(t, store.Get("node-import"), "Import: imported config identity must be copied")

		nor279MutateNodeConfig(t, ingress)
		nor279RequireNodeConfig(t, store.Get("node-import"), want, "Import: caller mutation after import")
		nor279RequireNodeConfig(t, nor279FindNodeConfig(t, store.Export(), "node-import"), want,
			"Import: caller mutation after import (Export)")

	case nor279NodeGet:
		store := NewNodeConfigStore()
		seed := nor279NodeConfigFixture("node-get")
		want := nor279NodeConfigExpectation(seed)
		store.Set(*seed)

		got := store.Get("node-get")
		nor279RequireNodeConfig(t, got, want, "Get pristine")

		nor279MutateNodeConfig(t, got)
		nor279RequireNodeConfig(t, store.Get("node-get"), want, "Get: caller mutation of the returned config")
		nor279RequireNodeConfig(t, nor279FindNodeConfig(t, store.Export(), "node-get"), want,
			"Get: caller mutation of the returned config (Export)")

	case nor279NodeGetOrCreate:
		// Newly created config: the returned default must not alias retained state.
		store := NewNodeConfigStore()
		created := store.GetOrCreate("node-created")
		require.NotNil(t, created)
		created.Metadata["injected"] = "mutated"
		created.PinList = append(created.PinList, "mutated")
		require.Emptyf(t, store.Get("node-created").Metadata,
			"GetOrCreate: writes through the returned new config must not reach retained state (Metadata)")
		require.Emptyf(t, store.Get("node-created").PinList,
			"GetOrCreate: writes through the returned new config must not reach retained state (PinList)")

		// Existing config: nested members must be detached as well.
		seed := nor279NodeConfigFixture("node-existing")
		want := nor279NodeConfigExpectation(seed)
		store.Set(*seed)

		existing := store.GetOrCreate("node-existing")
		nor279RequireNodeConfig(t, existing, want, "GetOrCreate existing pristine")

		nor279MutateNodeConfig(t, existing)
		nor279RequireNodeConfig(t, store.Get("node-existing"), want,
			"GetOrCreate existing: caller mutation of the returned config")

	case nor279NodeExportPath:
		store := NewNodeConfigStore()
		seedA := nor279NodeConfigFixture("node-a")
		seedB := nor279NodeConfigFixture("node-b")
		wantA := nor279NodeConfigExpectation(seedA)
		wantB := nor279NodeConfigExpectation(seedB)
		store.Set(*seedA)
		store.Set(*seedB)

		exported := store.Export()
		require.Len(t, exported, 2, "Export must return every config")
		for _, want := range []*NodeConfig{wantA, wantB} {
			nor279RequireNodeConfig(t, nor279FindNodeConfig(t, exported, want.NodeID), want,
				"Export pristine "+want.NodeID)
		}

		exportedA := nor279FindNodeConfig(t, exported, "node-a")
		exportedA.NodeID = "mutated"
		require.NotNilf(t, store.Get("node-a"), "Export: exported config fields must be copies")

		nor279MutateNodeConfig(t, nor279FindNodeConfig(t, exported, "node-b"))
		nor279RequireNodeConfig(t, store.Get("node-b"), wantB,
			"Export: caller mutation of an exported config")

	case nor279NodeRoundTrip:
		source := NewNodeConfigStore()
		seedA := nor279NodeConfigFixture("node-a")
		seedB := nor279NodeConfigFixture("node-b")
		wantA := nor279NodeConfigExpectation(seedA)
		wantB := nor279NodeConfigExpectation(seedB)
		source.Set(*seedA)
		source.Set(*seedB)

		exported := source.Export()
		require.Len(t, exported, 2)

		dest := NewNodeConfigStore()
		require.Equal(t, 2, dest.Import(exported), "Import must report the imported config count")

		// Fidelity and ordering before any mutation.
		for _, want := range []*NodeConfig{wantA, wantB} {
			got := dest.Get(want.NodeID)
			nor279RequireNodeConfig(t, got, want, "Export/Import round trip fidelity "+want.NodeID)
			require.Equalf(t, nor279FindNodeConfig(t, exported, want.NodeID).UpdatedAt, got.UpdatedAt,
				"Export/Import round trip must preserve UpdatedAt for %s", want.NodeID)
		}

		nor279MutateNodeConfig(t, nor279FindNodeConfig(t, exported, "node-a"))
		nor279RequireNodeConfig(t, source.Get("node-a"), wantA,
			"Export/Import round trip: mutating an exported config must not affect the source store")
		nor279RequireNodeConfig(t, dest.Get("node-a"), wantA,
			"Export/Import round trip: mutating an exported config must not affect the importing store")
	}
}

func TestNor279NodeConfigStoreNilAndEmptyControls(t *testing.T) {
	cases := []struct {
		name string
		zero func(*NodeConfig)
	}{
		{"NilCollections", func(cfg *NodeConfig) {
			cfg.PinList = nil
			cfg.DenyList = nil
			cfg.LabelConfigs = nil
			cfg.Metadata = nil
		}},
		{"EmptyCollections", func(cfg *NodeConfig) {
			cfg.PinList = []string{}
			cfg.DenyList = []string{}
			cfg.LabelConfigs = map[string]LabelConfig{}
			cfg.Metadata = map[string]interface{}{}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := NewNodeConfig("node-empty")
			tc.zero(cfg)

			store := NewNodeConfigStore()
			store.Set(*cfg)

			got := store.Get("node-empty")
			require.NotNil(t, got)
			require.Empty(t, got.PinList, "Get: nil/empty PinList must stay empty")
			require.Empty(t, got.DenyList, "Get: nil/empty DenyList must stay empty")
			require.Empty(t, got.LabelConfigs, "Get: nil/empty LabelConfigs must stay empty")
			require.Empty(t, got.Metadata, "Get: nil/empty Metadata must stay empty")
			require.False(t, got.IsPinned("target"), "nil/empty PinList must stay nil-safe")
			require.False(t, got.IsDenied("target"), "nil/empty DenyList must stay nil-safe")

			dest := NewNodeConfigStore()
			require.Equal(t, 1, dest.Import(store.Export()), "Export/Import must round trip an empty config")

			roundTripped := dest.Get("node-empty")
			require.NotNil(t, roundTripped)
			require.Empty(t, roundTripped.PinList, "Export/Import: nil/empty PinList must stay empty")
			require.Empty(t, roundTripped.DenyList, "Export/Import: nil/empty DenyList must stay empty")
			require.Empty(t, roundTripped.LabelConfigs, "Export/Import: nil/empty LabelConfigs must stay empty")
			require.Empty(t, roundTripped.Metadata, "Export/Import: nil/empty Metadata must stay empty")
			require.False(t, roundTripped.IsPinned("target"), "imported nil/empty PinList must stay nil-safe")
			require.False(t, roundTripped.IsDenied("target"), "imported nil/empty DenyList must stay nil-safe")

			if roundTripped.Metadata != nil {
				roundTripped.Metadata["injected"] = "mutated"
				require.Empty(t, dest.Get("node-empty").Metadata,
					"empty metadata control: writes through a returned map must not be retained")
			}
		})
	}
}

// --- Cost evidence ----------------------------------------------------------
//
// Recorded on unchanged source to size the detach requirement below. These
// benchmarks assert nothing and make no improvement claim.

func BenchmarkNor279EdgeMetaGetHistoryNestedMetadata(b *testing.B) {
	defer config.WithEdgeProvenanceEnabled()()

	store := NewEdgeMetaStore()
	ctx := context.Background()
	for _, record := range nor279EdgeMetaRecords(64) {
		if err := store.Append(ctx, record); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.GetHistory(ctx, "node-a", "node-b", "relates_to"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNor279NodeConfigGetNestedMetadata(b *testing.B) {
	store := NewNodeConfigStore()
	store.Set(*nor279NodeConfigFixture("node-a"))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if cfg := store.Get("node-a"); cfg == nil {
			b.Fatal("config missing")
		}
	}
}
