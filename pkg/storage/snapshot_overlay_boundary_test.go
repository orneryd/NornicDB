package storage

import (
	"github.com/orneryd/nornicdb/internal/workcensus"
	"os"
	"path/filepath"
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
	readers := []string{"CheckIDDictionary", "ReadStored", "GetStoredNode", "GetStoredEdge", "GetQueryNode", "GetNode", "GetNodeProjected", "GetNodeWithoutEmbeddings", "GetEdge", "BatchGetNodes", "BatchGetNodesWithoutEmbeddings", "HasLabelBatch", "RelationshipEndpointVisible", "GetNodesByLabelInScope", "GetFirstNodeByLabelInScope", "ForEachNodeIDByLabelInScope", "AllNodes", "AllEdges", "GetEdgesByType", "GetOutgoingEdges", "GetIncomingEdges", "GetAdjacentEdges", "OutgoingEdgeHeaders", "IncomingEdgeHeaders", "GetEdgesBetween", "GetEdgeBetween", "MatchEdgesBetween", "StreamNodesWithOptions", "StreamNodesByLabelProjectedInScope", "StreamEdges", "StreamEdgesByTypeInScope", "IterateNodes", "StreamNodeChunks", "GetNodeEmbeddingMeta"}
	for _, reader := range readers {
		population, err := inv.Program.ReachableFrom([]workcensus.Root{{Package: pkg, Name: reader, Receiver: "BadgerEngine", Required: true}})
		require.NoError(t, err)
		require.True(t, population.Contains(workcensus.Root{Package: pkg, Name: "captureReadView", Receiver: "BadgerEngine"}), "%s must reach the shared snapshot/overlay capture", reader)
	}
}

// This domain control uses the parent's already-loaded call graph. Removing
// the capture from a real method/closure must remove public-root reachability;
// no private parser, native-call approximation or display-name walk intervenes.
func TestSnapshotOverlayCaptureRemovalControl(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module github.com/orneryd/nornicdb\ngo 1.26.0\n"), 0644))
	path := filepath.Join(root, "pkg", "storage", "native.go")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
	source := []byte(`package storage
type BadgerEngine struct{}
func(b *BadgerEngine)captureReadView(){}
func(b *BadgerEngine)ReadStored(){func(){b.captureReadView()}()}
func(*BadgerEngine)Unnamed(){}
`)
	require.NoError(t, os.WriteFile(path, source, 0644))
	const pkg = "github.com/orneryd/nornicdb/pkg/storage"
	config := workcensus.Config{Dir: root, Patterns: []string{"./pkg/storage"}, IncludeProgram: true, Selectors: []workcensus.Selector{{Kind: "capture call", Node: "call", Reachability: "all"}}}
	check := func(config workcensus.Config) bool {
		inventory, err := workcensus.Extract(config)
		require.NoError(t, err)
		population, err := inventory.Program.ReachableFrom([]workcensus.Root{{Package: pkg, Name: "ReadStored", Receiver: "BadgerEngine", Required: true}})
		require.NoError(t, err)
		return population.Contains(workcensus.Root{Package: pkg, Name: "captureReadView", Receiver: "BadgerEngine"})
	}
	require.True(t, check(config))
	config.Overlay = map[string][]byte{path: []byte(strings.Replace(string(source), "func(){b.captureReadView()}()", "func(){}()", 1))}
	require.False(t, check(config), "the same public-root capture oracle must reject the omitted owner call")
}
