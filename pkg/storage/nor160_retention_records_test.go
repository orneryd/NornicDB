// NOR-160 lane (Claude), 30 October 2026: provenance records are caller copies.
package storage

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNOR160RetentionEdgeProvenance(t *testing.T) {
	if !config.IsEdgeProvenanceEnabled() {
		config.EnableEdgeProvenance()
		t.Cleanup(config.DisableEdgeProvenance)
	}
	ctx := context.Background()
	store := NewEdgeMetaStore()
	metadata := map[string]interface{}{"evidence": map[string]interface{}{"ids": []interface{}{"a"}}}
	require.NoError(t, store.Append(ctx, EdgeMeta{EdgeID: "e1", Src: "a", Dst: "b", Label: "T", SignalType: "manual", Origin: "o", SessionID: "s", Metadata: metadata}))
	readownership.Mutate(metadata)
	want := readownership.Capture(map[string]interface{}{"evidence": map[string]interface{}{"ids": []interface{}{"a"}}})
	reads := map[string]func() []*EdgeMeta{
		"GetHistory":      func() []*EdgeMeta { m, _ := store.GetHistory(ctx, "a", "b", "T"); return m },
		"GetBySignalType": func() []*EdgeMeta { m, _ := store.GetBySignalType(ctx, "manual", 0); return m },
		"GetByOrigin":     func() []*EdgeMeta { m, _ := store.GetByOrigin(ctx, "o", 0); return m },
		"GetBySession":    func() []*EdgeMeta { m, _ := store.GetBySession(ctx, "s", 0); return m },
		"Export":          store.Export,
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			for pass := 0; pass < 3; pass++ {
				got := read()
				require.Len(t, got, 1)
				require.Equal(t, want, readownership.Capture(got[0].Metadata), "pass %d", pass)
				readownership.Mutate(got)
			}
		})
	}
}
