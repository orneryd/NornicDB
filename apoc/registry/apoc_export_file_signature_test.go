package registry_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	apocexport "github.com/orneryd/nornicdb/apoc/export"
	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/stretchr/testify/require"
)

// Root APOC admits exactly the context-first whole-graph export file shapes:
// the registered apoc.export.{json,csv,cypher,graphml}{All,Data} entry points and
// apoc.export.toFile reach their writer with the caller's own context, graph and
// destination, and a failed write is the call's error rather than a successful
// nil result. The superseded context-free declarations are not admitted, so no
// unrelated two-string signature -- apoc.log.toFile is one -- is admitted as an
// export file surface, and a caller that supplies no context writes nothing.
func TestAPOCExportFileSignatureAdmitsCallerGraphOnItsContext(t *testing.T) {
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "caller")
	nodes := []*apocexport.Node{{ID: 1}}
	path := filepath.Join(t.TempDir(), "graph.graphml")

	var gotContext context.Context
	var gotNodes []*apocexport.Node
	var gotRels []*apocexport.Relationship
	var gotPath string
	graphHandler, err := registry.Compile(func(
		callerContext context.Context,
		graph []*apocexport.Node,
		relationships []*apocexport.Relationship,
		destination string,
	) error {
		gotContext, gotNodes, gotRels, gotPath = callerContext, graph, relationships, destination
		return nil
	}, registry.OpaqueReads)
	require.NoError(t, err)
	value, err := graphHandler.CallWithPolicy(
		[]interface{}{nodes, []*apocexport.Relationship(nil), path},
		[]reflect.Value{reflect.ValueOf(ctx)},
		registry.APOCArguments,
	)
	require.NoError(t, err)
	require.Nil(t, value)
	require.Same(t, ctx, gotContext, "the caller's context is forwarded unchanged")
	require.Len(t, gotNodes, 1)
	require.Same(t, nodes[0], gotNodes[0])
	require.Empty(t, gotRels)
	require.Equal(t, path, gotPath)

	var csvPath string
	csvHandler, err := registry.Compile(func(_ context.Context, _ []*apocexport.Node, destination string) error {
		csvPath = destination
		return nil
	}, registry.OpaqueReads)
	require.NoError(t, err)
	value, err = csvHandler.CallWithPolicy(
		[]interface{}{nodes, path},
		[]reflect.Value{reflect.ValueOf(ctx)},
		registry.APOCArguments,
	)
	require.NoError(t, err)
	require.Nil(t, value)
	require.Equal(t, path, csvPath)

	var rawData string
	rawHandler, err := registry.Compile(func(_ context.Context, data string, _ string) error {
		rawData = data
		return nil
	}, registry.OpaqueReads)
	require.NoError(t, err)
	value, err = rawHandler.CallWithPolicy(
		[]interface{}{"raw content", path},
		[]reflect.Value{reflect.ValueOf(ctx)},
		registry.APOCArguments,
	)
	require.NoError(t, err)
	require.Nil(t, value)
	require.Equal(t, "raw content", rawData)

	destinationErr := errors.New("destination refused the export")
	failing, err := registry.Compile(func(context.Context, []*apocexport.Node, []*apocexport.Relationship, string) error {
		return destinationErr
	}, registry.OpaqueReads)
	require.NoError(t, err)
	value, err = failing.CallWithPolicy(
		[]interface{}{nil, nil, path},
		[]reflect.Value{reflect.ValueOf(ctx)},
		registry.APOCArguments,
	)
	require.ErrorIs(t, err, destinationErr)
	require.Nil(t, value)

	// A root call cannot supply the graph and destination alone: without the
	// caller's context the argument count does not match the context-first
	// declaration, so the invocation is rejected before any I/O.
	_, err = graphHandler.CallWithPolicy(
		[]interface{}{nodes, []*apocexport.Relationship(nil), path},
		nil,
		registry.APOCArguments,
	)
	require.ErrorIs(t, err, registry.ErrUnsupportedSignature)
}

// TestAPOCExportFileSignatureRejectsSupersededContextFreeShapes pins the closed
// root surface: the context-free export file declarations are gone, including
// the two-string shape the retired raw writer shared with apoc.log.toFile.
func TestAPOCExportFileSignatureRejectsSupersededContextFreeShapes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graph.graphml")
	nodes := []*apocexport.Node{{ID: 1}}
	for name, tc := range map[string]struct {
		function interface{}
		args     []interface{}
	}{
		"context-free graph file": {
			function: func([]*apocexport.Node, []*apocexport.Relationship, string) error { return nil },
			args:     []interface{}{nodes, []*apocexport.Relationship(nil), path},
		},
		"context-free node file": {
			function: func([]*apocexport.Node, string) error { return nil },
			args:     []interface{}{nodes, path},
		},
		"context-free two strings": {
			function: func(string, string) error { return nil },
			args:     []interface{}{"content", path},
		},
	} {
		t.Run(name, func(t *testing.T) {
			handler, err := registry.Compile(tc.function, registry.OpaqueReads)
			require.NoError(t, err)
			_, err = handler.CallWithPolicy(tc.args, nil, registry.APOCArguments)
			require.ErrorIs(t, err, registry.ErrUnsupportedSignature)
		})
	}
}
