package fn

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeCtx builds a Context with the given nodes and rels for testing.
func makeCtx(nodes map[string]*storage.Node, rels map[string]*storage.Edge) Context {
	return Context{
		Nodes:    nodes,
		Rels:     rels,
		Database: "test",
		Eval: func(expr string) (interface{}, error) {
			return nil, nil
		},
	}
}

// makeCtxWithEval builds a Context where Eval returns the given value for any expr.
func makeCtxWithEval(v interface{}) Context {
	return Context{
		Nodes:    map[string]*storage.Node{},
		Rels:     map[string]*storage.Edge{},
		Database: "test",
		Eval: func(expr string) (interface{}, error) {
			return v, nil
		},
	}
}

type sizedValue struct{}

func (sizedValue) CypherSize() int64 { return 4 }

// Every former branch test is one named row. One runner preserves registry
// dispatch, error identity and typed value assertions without copying setup.
func TestBuiltinExtraEvaluation(t *testing.T) {
	for _, tc := range []struct {
		name, function string
		args           []string
		scope          Context
		want           interface{}
		argumentError  bool
		unorderedList  bool
	}{
		{name: "TestEvalID_ViaRel", function: "id", args: []string{"r"}, scope: makeCtx(nil, map[string]*storage.Edge{"r": {ID: "nornic:r1", Type: "KNOWS"}}), want: "nornic:r1"},
		{name: "TestEvalID_ViaEval_Node", function: "id", args: []string{"expr"}, scope: makeCtxWithEval(&storage.Node{ID: "nornic:n1"}), want: "nornic:n1"},
		{name: "TestEvalID_ViaEval_Edge", function: "id", args: []string{"expr"}, scope: makeCtxWithEval(&storage.Edge{ID: "nornic:e1"}), want: "nornic:e1"},
		{name: "TestEvalID_NoArgs", function: "id", args: []string{}, scope: makeCtx(nil, nil)},
		{name: "TestEvalElementID_ViaRel", function: "elementid", args: []string{"r"}, scope: makeCtx(nil, map[string]*storage.Edge{"r": {ID: "nornic:r1", Type: "LIKES"}}), want: "5:test:nornic:r1"},
		{name: "TestEvalElementID_NoArgs", function: "elementid", args: []string{}, scope: makeCtx(nil, nil)},
		{name: "TestEvalLabels_ViaEval_Node", function: "labels", args: []string{"expr"}, scope: makeCtxWithEval(&storage.Node{ID: "nornic:n1", Labels: []string{"Person", "Employee"}}), want: []interface{}{"Person", "Employee"}, unorderedList: true},
		{name: "TestEvalLabels_NoArgs", function: "labels", args: []string{}, scope: makeCtx(nil, nil)},
		{name: "TestEvalLabels_NoMatch", function: "labels", args: []string{"nonexistent"}, scope: makeCtx(nil, nil)},
		{name: "TestEvalType_ViaEvalMap", function: "type", args: []string{"expr"}, scope: makeCtxWithEval(map[string]interface{}{"type": "CONNECTED_TO"}), argumentError: true},
		{name: "TestEvalType_NoArgs", function: "type", args: []string{}, scope: makeCtx(nil, nil)},
		{name: "TestEvalKeys_ViaRel", function: "keys", args: []string{"r"}, scope: makeCtx(nil, map[string]*storage.Edge{"r": {ID: "nornic:r1", Type: "KNOWS", Properties: map[string]interface{}{"since": 2020, "weight": 0.9}}}), want: []interface{}{"since", "weight"}, unorderedList: true},
		{name: "TestEvalKeys_NoArgs", function: "keys", args: []string{}, scope: makeCtx(nil, nil)},
		{name: "TestEvalProperties_ViaRel", function: "properties", args: []string{"r"}, scope: makeCtx(nil, map[string]*storage.Edge{"r": {ID: "nornic:r1", Type: "LIKES", Properties: map[string]interface{}{"strength": 0.8}}}), want: map[string]interface{}{"strength": 0.8}},
		{name: "TestEvalProperties_NoArgs", function: "properties", args: []string{}, scope: makeCtx(nil, nil)},
		{name: "TestEvalProperties_NoMatch", function: "properties", args: []string{"ghost"}, scope: makeCtx(nil, nil)},
		{name: "TestEvalToUpper_Nil", function: "toupper", args: []string{"x"}, scope: makeCtxWithEval(nil)},
		{name: "TestEvalToLower_Nil", function: "tolower", args: []string{"x"}, scope: makeCtxWithEval(nil)},
		{name: "TestEvalToUpper_NoArgs", function: "toupper", args: []string{}, scope: makeCtx(nil, nil)},
		{name: "TestEvalSize_StringSlice", function: "size", args: []string{"x"}, scope: makeCtxWithEval([]string{"a", "b", "c"}), want: int64(3)},
		{name: "TestEvalSize_Sized", function: "size", args: []string{"x"}, scope: makeCtxWithEval(sizedValue{}), want: int64(4)},
		{name: "TestEvalSize_NoArgs", function: "size", args: []string{}, scope: makeCtx(nil, nil), want: int64(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, found, err := EvaluateFunction(context.Background(), tc.function, tc.args, tc.scope)
			require.True(t, found, "every row reaches an actual registered implementation")
			if tc.argumentError {
				var argumentError *ArgumentTypeError
				require.ErrorAs(t, err, &argumentError)
			} else {
				require.NoError(t, err)
			}
			if tc.want == nil {
				assert.Nil(t, got)
				return
			}
			if tc.unorderedList {
				list, ok := got.([]interface{})
				require.True(t, ok)
				assert.ElementsMatch(t, tc.want, list)
				return
			}
			assert.Equal(t, tc.want, got)
		})
	}
}
