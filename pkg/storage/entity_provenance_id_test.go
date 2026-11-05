package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEntitySourceOutranksLocalIDText(t *testing.T) {
	first := &Node{runtime: &nodeRuntime{metadata: NodeRuntimeMetadata{Database: "one"}}, ID: "same"}
	second := &Node{runtime: &nodeRuntime{metadata: NodeRuntimeMetadata{Database: "two"}}, ID: "4:one:same"}
	require.NotEqual(t, first.ElementID("comp"), second.ElementID("comp"))
	require.Equal(t, "4:two:4:one:same", second.ElementID("comp"))
	kind, database, local, ok := ParseElementID(second.ElementID("comp"))
	require.True(t, ok)
	require.Equal(t, "4", kind)
	require.Equal(t, second.RuntimeMetadata().Database, database)
	require.Equal(t, string(second.ID), local)
	relationship := &Edge{Database: "two", ID: "5:one:rel", StartNode: second.ID}
	require.Equal(t, "5:two:5:one:rel", relationship.ElementID("comp"))
	require.Equal(t, second.ElementID("comp"), relationship.StartElementID("comp"))
}
