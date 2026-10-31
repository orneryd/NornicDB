// NOR-160 lane (Claude), 30 October 2026: evidence snapshots are caller copies.
package inference

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/stretchr/testify/require"
)

func TestNOR160RetentionEvidenceMetadata(t *testing.T) {
	buffer := NewEvidenceBuffer()
	metadata := map[string]interface{}{"context": map[string]interface{}{"ids": []interface{}{"a"}}}
	buffer.AddEvidenceWithMetadata("a", "b", "T", 0.5, "manual", "s", metadata)
	want := readownership.Capture(buffer.GetEvidence("a", "b", "T").Metadata)
	for pass := 0; pass < 3; pass++ {
		got := buffer.GetEvidence("a", "b", "T")
		require.NotNil(t, got)
		require.Equal(t, want, readownership.Capture(got.Metadata), "pass %d", pass)
		readownership.Mutate(got.Metadata)
	}
}
