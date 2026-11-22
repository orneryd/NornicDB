package storage

// NOR279 additional coverage: the members of the reported ownership contract
// that the two reproduction files do not pin down.
//
// nor279_metadata_ownership_test.go and nor279_filtered_read_ownership_test.go
// are the reported fail-first reproduction, kept verbatim. These cases add:
//
//   - the delivered collection representation, which this change does not alter:
//     an EdgeMeta with nil metadata still delivers nil, and a NodeConfig supplied
//     with nil collections is still delivered as allocated empty collections, so
//     Get and Export keep publishing `[]`/`{}`;
//   - isolation of those published collections, through ingress, every read/
//     export boundary and an export/import round trip;
//   - the pin/deny list getters, which deliver retained slices independently of
//     the value returned by Get;
//   - Import replacing an existing config, which must detach the replacement.
//
// Fixtures and assertions are the reported ones; no new value shapes are
// invented.

import (
	"encoding/json"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// TestNor279EdgeMetaNilAndEmptyFidelity keeps nil and empty retained metadata
// distinguishable across every public boundary.
func TestNor279EdgeMetaNilAndEmptyFidelity(t *testing.T) {
	defer config.WithEdgeProvenanceEnabled()()

	ctx := t.Context()
	cases := []struct {
		name     string
		metadata map[string]interface{}
		wantNil  bool
	}{
		{"NilMetadata", nil, true},
		{"EmptyMetadata", map[string]interface{}{}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := NewEdgeMetaStore()
			record := nor279EdgeMetaRecord(0)
			record.Metadata = tc.metadata
			require.NoError(t, store.Append(ctx, record))

			requireMetadataFidelity := func(label string, got map[string]interface{}) {
				t.Helper()
				require.Equalf(t, tc.wantNil, got == nil,
					"%s: nil and empty metadata must stay distinguishable", label)
				require.Emptyf(t, got, "%s: metadata values must be preserved", label)
			}

			history, err := store.GetHistory(ctx, record.Src, record.Dst, record.Label)
			require.NoError(t, err)
			require.Len(t, history, 1)
			requireMetadataFidelity("GetHistory", history[0].Metadata)

			latest, err := store.GetLatest(ctx, record.Src, record.Dst, record.Label)
			require.NoError(t, err)
			require.NotNil(t, latest)
			requireMetadataFidelity("GetLatest", latest.Metadata)

			exported := store.Export()
			require.Len(t, exported, 1)
			requireMetadataFidelity("Export", exported[0].Metadata)

			dest := NewEdgeMetaStore()
			require.Equal(t, 1, dest.Import(exported), "Import must round trip the record")
			roundTripped, err := dest.GetHistory(ctx, record.Src, record.Dst, record.Label)
			require.NoError(t, err)
			require.Len(t, roundTripped, 1)
			requireMetadataFidelity("Export/Import", roundTripped[0].Metadata)
		})
	}
}

// TestNor279NodeConfigCollectionRepresentation pins the collection
// representation the store publishes, which this change does not alter: a config
// supplied with nil collections is retained and delivered as an allocated empty
// collection, so Get and Export keep publishing `[]`/`{}` for them. The
// nil-preservation expectation that briefly replaced this one was an unapproved
// contract change, not an established defect.
func TestNor279NodeConfigCollectionRepresentation(t *testing.T) {
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
			cfg := nor279NodeConfigFixture("node-fidelity")
			tc.zero(cfg)

			requirePublishedCollections := func(label string, got *NodeConfig) {
				t.Helper()
				require.NotNilf(t, got, "%s: config missing", label)
				require.NotNilf(t, got.PinList, "%s: PinList is published as an allocated empty list", label)
				require.NotNilf(t, got.DenyList, "%s: DenyList is published as an allocated empty list", label)
				require.NotNilf(t, got.LabelConfigs, "%s: LabelConfigs is published as an allocated empty map", label)
				require.NotNilf(t, got.Metadata, "%s: Metadata is published as an allocated empty map", label)
				require.Emptyf(t, got.PinList, "%s: PinList values", label)
				require.Emptyf(t, got.DenyList, "%s: DenyList values", label)
				require.Emptyf(t, got.LabelConfigs, "%s: LabelConfigs values", label)
				require.Emptyf(t, got.Metadata, "%s: Metadata values", label)
				// Healthy control: the published collections stay nil-safe.
				require.Falsef(t, got.IsPinned("target"), "%s: empty PinList must stay nil-safe", label)
				require.Falsef(t, got.IsDenied("target"), "%s: empty DenyList must stay nil-safe", label)
			}

			requireJSONRepresentation := func(label string, got *NodeConfig) {
				t.Helper()
				encoded, err := json.Marshal(got)
				require.NoErrorf(t, err, "%s: config must marshal", label)
				var fields map[string]json.RawMessage
				require.NoErrorf(t, json.Unmarshal(encoded, &fields), "%s: %s", label, encoded)
				require.JSONEqf(t, "[]", string(fields["pin_list"]), "%s: pin_list is published as []", label)
				require.JSONEqf(t, "[]", string(fields["deny_list"]), "%s: deny_list is published as []", label)
				require.JSONEqf(t, "{}", string(fields["label_configs"]), "%s: label_configs is published as {}", label)
				_, present := fields["metadata"]
				require.Falsef(t, present, "%s: metadata is omitted while it is empty", label)
			}

			store := NewNodeConfigStore()
			store.Set(*cfg)

			got := store.Get("node-fidelity")
			requirePublishedCollections("Get", got)
			requireJSONRepresentation("Get", got)

			exported := store.Export()
			require.Len(t, exported, 1)
			requirePublishedCollections("Export", exported[0])
			requireJSONRepresentation("Export", exported[0])

			dest := NewNodeConfigStore()
			require.Equal(t, 1, dest.Import(exported), "Import must round trip the config")
			roundTripped := dest.Get("node-fidelity")
			requirePublishedCollections("Export/Import", roundTripped)
			requireJSONRepresentation("Export/Import", roundTripped)

			// Isolation: the published collections belong to the caller, so writes
			// through a delivered config must not reach retained state.
			got.PinList = append(got.PinList, "target")
			got.DenyList = append(got.DenyList, "target")
			got.LabelConfigs["injected"] = LabelConfig{Label: "injected"}
			got.Metadata["injected"] = "value"
			requirePublishedCollections("Get after caller writes", store.Get("node-fidelity"))

			// A config created by GetOrCreate keeps the same published shape.
			created := store.GetOrCreate("node-created")
			requirePublishedCollections("GetOrCreate created", created)
			requireJSONRepresentation("GetOrCreate created", created)
		})
	}
}

// TestNor279NodeConfigListGettersDetach checks that the pin/deny list getters
// deliver retained slices independently of the store and of each other.
func TestNor279NodeConfigListGettersDetach(t *testing.T) {
	store := NewNodeConfigStore()
	seed := nor279NodeConfigFixture("node-lists")
	want := nor279NodeConfigExpectation(seed)
	store.Set(*seed)

	pinned := store.GetPinnedTargets("node-lists")
	denied := store.GetDeniedTargets("node-lists")
	require.Equal(t, want.PinList, pinned, "GetPinnedTargets must return the retained pin list in order")
	require.Equal(t, want.DenyList, denied, "GetDeniedTargets must return the retained deny list in order")

	pinned[0] = "mutated"
	denied[0] = "mutated"
	pinned = append(pinned, "appended")
	denied = append(denied, "appended")

	require.Equal(t, want.PinList, store.GetPinnedTargets("node-lists"),
		"GetPinnedTargets: writes through a returned slice must not reach retained state")
	require.Equal(t, want.DenyList, store.GetDeniedTargets("node-lists"),
		"GetDeniedTargets: writes through a returned slice must not reach retained state")
	nor279RequireNodeConfig(t, store.Get("node-lists"), want, "list getters: caller mutation of returned slices")
}

// TestNor279NodeConfigImportReplacementDetachesCallerConfig covers Import of an
// already configured node: the replacement is retained, so the imported config
// must be detached from its caller as well.
func TestNor279NodeConfigImportReplacementDetachesCallerConfig(t *testing.T) {
	store := NewNodeConfigStore()
	original := nor279NodeConfigFixture("node-replace")
	store.Set(*original)

	replacement := nor279NodeConfigFixture("node-replace")
	replacement.MaxOutEdges = 7
	replacement.PinList = []string{"pin-c", "pin-d"}
	replacement.Metadata["replacement"] = map[string]interface{}{"state": "new"}
	wantReplacement := nor279NodeConfigExpectation(replacement)

	require.Equal(t, 1, store.Import([]*NodeConfig{replacement}), "Import must report the imported config count")
	require.Equal(t, 1, store.Size(), "Import must replace the existing config rather than add one")
	nor279RequireNodeConfig(t, store.Get("node-replace"), wantReplacement, "Import replacement pristine")

	nor279MutateNodeConfig(t, replacement)
	nor279RequireNodeConfig(t, store.Get("node-replace"), wantReplacement,
		"Import replacement: caller mutation after import")
	nor279RequireNodeConfig(t, nor279FindNodeConfig(t, store.Export(), "node-replace"), wantReplacement,
		"Import replacement: caller mutation after import (Export)")
}
