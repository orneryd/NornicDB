package storage

// Root-cause diagnostic (campaign v1.5.2, lane A, group G-inference-provenance-alias).
// Test-side only. Logs map/slice identity across the stores' copy points:
// EdgeMetaStore.Append (`metaCopy := meta`), GetHistory / Export
// (`metaCopy := *meta`) are struct copies, so EdgeMeta.Metadata is the same
// map; NodeConfigStore.Set stores `&cfg` (caller's PinList backing array and
// Metadata map); copyNodeConfig copies Metadata one level deep only.
// The corrected variant deep-copies the metadata before Append.

import (
	"context"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
)

func rcaMapID(m map[string]interface{}) uintptr {
	if m == nil {
		return 0
	}
	return reflect.ValueOf(m).Pointer()
}

func rcaDeepCopyAny(v interface{}) interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(x))
		for k, e := range x {
			out[k] = rcaDeepCopyAny(e)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(x))
		for i, e := range x {
			out[i] = rcaDeepCopyAny(e)
		}
		return out
	case []string:
		return append([]string(nil), x...)
	default:
		return v
	}
}

func TestRCAEdgeMetaSharesMetadataMap(t *testing.T) {
	defer config.WithEdgeProvenanceEnabled()()
	ctx := context.Background()

	s := NewEdgeMetaStore()
	callerMeta := map[string]interface{}{"origin_file": "docs/design.md", "nested": map[string]interface{}{"retries": 3}}
	if err := s.Append(ctx, EdgeMeta{Src: "a", Dst: "b", Label: "REL", Metadata: callerMeta}); err != nil {
		t.Fatal(err)
	}
	hist, _ := s.GetHistory(ctx, "a", "b", "REL")
	exp := s.Export()
	t.Logf("RCA_EDGEMETA caller=%#x stored(GetHistory)=%#x export=%#x same=%v",
		rcaMapID(callerMeta), rcaMapID(hist[0].Metadata), rcaMapID(exp[0].Metadata),
		rcaMapID(callerMeta) == rcaMapID(hist[0].Metadata))
	callerMeta["origin_file"] = "mutated-after-append"
	hist[0].Metadata["injected"] = "via-read-copy"
	again, _ := s.GetHistory(ctx, "a", "b", "REL")
	t.Logf("RCA_EDGEMETA stored after caller and reader writes: %#v", again[0].Metadata)

	// Corrected variant: deep copy at ingress (and the same at egress).
	s2 := NewEdgeMetaStore()
	callerMeta2 := map[string]interface{}{"origin_file": "docs/design.md"}
	if err := s2.Append(ctx, EdgeMeta{Src: "a", Dst: "b", Label: "REL", Metadata: rcaDeepCopyAny(callerMeta2).(map[string]interface{})}); err != nil {
		t.Fatal(err)
	}
	callerMeta2["origin_file"] = "mutated-after-append"
	h2, _ := s2.GetHistory(ctx, "a", "b", "REL")
	t.Logf("RCA_EDGEMETA corrected(deep-copied ingress) stored=%#v", h2[0].Metadata)
}

func TestRCANodeConfigSetKeepsCallerSlice(t *testing.T) {
	s := NewNodeConfigStore()
	cfg := NewNodeConfig("n1")
	cfg.PinList = []string{"pin-a", "pin-b"}
	cfg.Metadata = map[string]interface{}{"tags": []string{"t1"}}
	s.Set(*cfg)
	cfg.PinList[0] = "mutated" // caller edits its own value after Set
	got := s.Get("n1")
	t.Logf("RCA_NODECONFIG after Set+caller edit: PinList=%#v", got.PinList)

	got.Metadata["tags"].([]string)[0] = "mutated-nested"
	again := s.Get("n1")
	t.Logf("RCA_NODECONFIG after editing nested slice of a Get copy: Metadata=%#v", again.Metadata)
}
