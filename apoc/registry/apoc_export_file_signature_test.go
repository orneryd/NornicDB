package registry_test

import (
	"errors"
	"path/filepath"
	"testing"

	apocexport "github.com/orneryd/nornicdb/apoc/export"
	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/stretchr/testify/require"
)

// Root APOC admits the whole-graph export file shape: the registered
// apoc.export.graphMLAll / graphMLData entry points reach the writer with the
// caller's own graph and destination, and a failed write is the call's error
// rather than a successful nil result. Admission stays closed: a neighbouring
// shape the export family does not declare is still rejected.
func TestAPOCExportFileSignatureAdmitsCallerGraph(t *testing.T) {
	var gotNodes []*apocexport.Node
	var gotRels []*apocexport.Relationship
	var gotPath string
	handler, err := registry.Compile(func(nodes []*apocexport.Node, rels []*apocexport.Relationship, path string) error {
		gotNodes, gotRels, gotPath = nodes, rels, path
		return nil
	})
	require.NoError(t, err)

	nodes := []*apocexport.Node{{ID: 1}}
	path := filepath.Join(t.TempDir(), "graph.graphml")
	value, err := handler.CallWithPolicy([]interface{}{nodes, []*apocexport.Relationship(nil), path}, nil, registry.APOCArguments)
	require.NoError(t, err)
	require.Nil(t, value)
	require.Len(t, gotNodes, 1)
	require.Same(t, nodes[0], gotNodes[0])
	require.Empty(t, gotRels)
	require.Equal(t, path, gotPath)

	destinationErr := errors.New("destination refused the export")
	failing, err := registry.Compile(func([]*apocexport.Node, []*apocexport.Relationship, string) error {
		return destinationErr
	})
	require.NoError(t, err)
	value, err = failing.CallWithPolicy([]interface{}{nil, nil, path}, nil, registry.APOCArguments)
	require.ErrorIs(t, err, destinationErr)
	require.Nil(t, value)

	unlisted, err := registry.Compile(func([]*apocexport.Node, string) error { return nil })
	require.NoError(t, err)
	_, err = unlisted.CallWithPolicy([]interface{}{nodes, path}, nil, registry.APOCArguments)
	require.ErrorIs(t, err, registry.ErrUnsupportedSignature)
}
