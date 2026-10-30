package storage

import (
	"github.com/orneryd/nornicdb/internal/workcensus"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The deterministic selector tests prove the capture contract. This guard
// ensures its public consumers keep using that owner as new read paths arrive.
func TestSnapshotOverlayPublicReadsReachCapture(t *testing.T) {
	const pkg = "github.com/orneryd/nornicdb/pkg/storage"
	inv, err := workcensus.Extract(workcensus.Config{
		Dir: "../..", Patterns: []string{"./pkg/storage"}, BuildFlags: []string{"-tags=noui,nolocalllm"},
		Packages: []string{pkg}, IncludeProgram: true,
		Selectors: []workcensus.Selector{{Kind: "buffer access", Node: "call", Reachability: "all", Packages: []string{pkg}, NameContains: []string{"Overlay", "GetNode", "GetEdge", "LookupNode", "LookupEdge", "NodeDeleted", "EdgeDeleted"}}},
	})
	require.NoError(t, err)
	// Domain policy stays here; source loading and call/method/closure
	// reachability come from the same engine as the production work census.
	for _, member := range inv.Members {
		if strings.Contains(member.Owner, ".BadgerEngine).") && strings.Contains(member.Receiver, ".writeBehind") {
			t.Errorf("%s bypasses capture: %s", member.Owner, member.Code)
		}
	}
	readers := []string{"GetQueryNode", "GetNode", "GetNodeProjected", "GetNodeWithoutEmbeddings", "GetEdge", "BatchGetNodes", "BatchGetNodesWithoutEmbeddings", "HasLabelBatch", "RelationshipEndpointVisible", "GetNodesByLabelInScope", "GetFirstNodeByLabelInScope", "ForEachNodeIDByLabelInScope", "AllNodes", "AllEdges", "GetEdgesByType", "GetOutgoingEdges", "GetIncomingEdges", "GetAdjacentEdges", "OutgoingEdgeHeaders", "IncomingEdgeHeaders", "GetEdgesBetween", "GetEdgeBetween", "MatchEdgesBetween", "StreamNodesWithOptions", "StreamNodesByLabelProjectedInScope", "StreamEdges", "StreamEdgesByTypeInScope", "IterateNodes", "StreamNodeChunks", "GetNodeEmbeddingMeta"}
	for _, reader := range readers {
		population, err := inv.Program.ReachableFrom([]workcensus.Root{{Package: pkg, Name: reader, Receiver: "BadgerEngine", Required: true}})
		require.NoError(t, err)
		require.True(t, population.Contains(workcensus.Root{Package: pkg, Name: "captureReadView", Receiver: "BadgerEngine"}), "%s must reach the shared snapshot/overlay capture", reader)
	}
}
