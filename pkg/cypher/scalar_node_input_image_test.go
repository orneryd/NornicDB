package cypher

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The property demand a call makes is the promise its registration declares, not
// the spelling of its name. These statements run through the public executor
// over the fixture of scalar_node_demand_test.go, whose recording label scan
// reports whether a stored property body was decoded: a nil projection is the
// whole-body demand, an empty non-nil one is the header-only demand.
//
// Registering a name replaces both policies for the whole process, so each
// declaration is exercised in its own child process, as the built-in identity
// override regression is.
const scalarNodeInputImageChild = "NORNIC_TEST_SCALAR_INPUT_IMAGE_CHILD"

// scalarNodeVariable is the one MATCH variable of the fixture statement.
const scalarNodeVariable = "n"

// scalarNodeStatementPrefixes are the two statement parsers an admitted
// statement may come from: Cypher 5 (no prefix) and Cypher 25.
var scalarNodeStatementPrefixes = []string{"", "CYPHER 25 "}

// scalarNodeInputImageDeclarations are the registration promises driven in an
// isolated process, one per child: a custom callback's full scope, a custom
// callback's arguments only, a header scalar, the same implicit reader declared
// arguments-only (its control), and a custom implementation of a structural
// built-in's name.
var scalarNodeInputImageDeclarations = []string{"full_scope", "arguments", "header", "arguments_control", "structural_name"}

// singleScan returns the label read the statement made, whose recorded
// projection describes.
func (g *scalarNodeDemandGraph) singleScan(t *testing.T) recordedNodeScan {
	t.Helper()
	require.Len(t, g.store.scans, 1, "one label read answers the statement")
	return g.store.scans[0]
}

// wholeBodyScans asserts every label read the statement made delivered the
// fixture's complete property body, so a callback that reads a visible binding
// it never names could read any of its properties.
func (g *scalarNodeDemandGraph) wholeBodyScans(t *testing.T) {
	t.Helper()
	require.NotEmpty(t, g.store.scans, "the statement made a label read")
	for _, scan := range g.store.scans {
		require.Nil(t, scan.projection, "a full-scope call keeps every visible binding's whole body")
		require.NotEmpty(t, scan.images, "the read delivered the matched node")
		require.Equal(t, scalarNodeWholeBodySizes(), scan.images[0].propertySizes, "the read acquired the complete property body")
	}
}

// TestScalarNodeInputImageIsTheRegisteredDeclaration drives one statement per
// declaration in an isolated process and asserts both the answer and the demand
// the read used.
func TestScalarNodeInputImageIsTheRegisteredDeclaration(t *testing.T) {
	for _, declaration := range scalarNodeInputImageDeclarations {
		if os.Getenv(scalarNodeInputImageChild) == declaration {
			runScalarNodeInputImageChild(t, declaration)
			return
		}
	}
	for _, declaration := range scalarNodeInputImageDeclarations {
		command := exec.Command(os.Args[0], "-test.run=^TestScalarNodeInputImageIsTheRegisteredDeclaration$", "-test.count=1")
		command.Env = append(os.Environ(), scalarNodeInputImageChild+"="+declaration)
		output, err := command.CombinedOutput()
		require.NoError(t, err, "isolated %s registration: %s", declaration, output)
	}
}

// readVisibleBindingFunction is a custom implementation that reads the row's
// MATCH variable whether or not its own arguments name it: it evaluates the
// binding through the call context, which is what a full-scope registration is
// allowed to do.
func readVisibleBindingFunction(properties int) cypherfn.Func {
	return func(_ context.Context, scope cypherfn.Context, _ []string) (interface{}, error) {
		value, err := scope.Eval(scalarNodeVariable)
		if err != nil {
			return nil, err
		}
		node, ok := value.(*storage.Node)
		if !ok {
			return nil, nil
		}
		if properties > 0 {
			text, _ := node.Properties[scalarNodeUnusedProperty].(string)
			return int64(len(text)), nil
		}
		return int64(len(node.Properties)), nil
	}
}

// runScalarNodeInputImageChild registers one custom implementation under a
// built-in name and executes the statements that distinguish its declaration
// from the built-in's.
func runScalarNodeInputImageChild(t *testing.T, declaration string) {
	t.Helper()
	switch declaration {
	case "full_scope":
		// A full-scope callback reads any visible binding completely, including
		// the binding its own argument never names: toUpper's argument is a
		// literal, and the callback reads the MATCH variable through the call
		// context.
		cypherfn.Register("toupper", readVisibleBindingFunction(1), cypherfn.OpaqueReads, cypherfn.FullInputImage)
		for _, prefix := range scalarNodeStatementPrefixes {
			graph := newScalarNodeDemandGraph(t, true)
			result := graph.run(t, prefix+"MATCH (n:EIBig) RETURN toUpper('ignored')")
			require.Equal(t, [][]interface{}{{int64(scalarNodeUnusedBytes)}}, result.Rows,
				"a full-scope callback reads the visible binding its argument never names")
			scan := graph.singleScan(t)
			require.Nil(t, scan.projection, "a full-scope registration keeps every visible binding's whole body")
			require.Equal(t, scalarNodeWholeBodySizes(), scan.images[0].propertySizes)
		}
		for _, tc := range []struct {
			name      string
			statement string
			want      [][]interface{}
		}{
			{
				name:      "filtered_count",
				statement: "MATCH (n:EIBig) WHERE toUpper('ignored') = " + fmt.Sprint(scalarNodeUnusedBytes) + " RETURN count(n)",
				want:      [][]interface{}{{int64(1)}},
			},
			{
				// The call sits inside another call's argument, a form the
				// per-variable walk reads as that unknown-shaped argument: the
				// binding the statement names beside it must still keep its
				// whole body, because the callback may read it too.
				name:      "nested_call",
				statement: "MATCH (n:EIBig) RETURN coalesce(toUpper('ignored'), 'fallback'), n.small",
				want:      [][]interface{}{{int64(scalarNodeUnusedBytes), scalarNodeSmallValue}},
			},
			{
				// Two bindings are visible in the row: the earlier MATCH and the
				// later one both keep their whole body for the callback, which
				// names neither of them in its arguments.
				name:      "visible_bindings",
				statement: "MATCH (n:EIBig) WITH n MATCH (m:EIBig) RETURN coalesce(toUpper('ignored'), 'fallback'), m.small",
				want:      [][]interface{}{{int64(scalarNodeUnusedBytes), scalarNodeSmallValue}},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				for _, prefix := range scalarNodeStatementPrefixes {
					graph := newScalarNodeDemandGraph(t, true)
					result := graph.run(t, prefix+tc.statement)
					require.Equal(t, tc.want, result.Rows)
					graph.wholeBodyScans(t)
				}
			})
		}
	case "arguments":
		// An arguments-only implementation reads its arguments' values: head's
		// argument is the list [n], so the node's whole body is that value. The
		// name grants no transform semantics: the projection through the result
		// is not read from one of the node's own properties.
		cypherfn.Register("head", func(_ context.Context, scope cypherfn.Context, args []string) (interface{}, error) {
			value, err := scope.Eval(args[0])
			if err != nil {
				return nil, err
			}
			list, ok := value.([]interface{})
			if !ok || len(list) == 0 {
				return nil, nil
			}
			return list[0], nil
		}, cypherfn.OpaqueReads, cypherfn.ArgumentInputImage)
		for _, prefix := range scalarNodeStatementPrefixes {
			graph := newScalarNodeDemandGraph(t, true)
			result := graph.run(t, prefix+"MATCH (n:EIBig) RETURN head([n]).small")
			require.Equal(t, [][]interface{}{{scalarNodeSmallValue}}, result.Rows)
			require.Nil(t, graph.singleScan(t).projection,
				"a custom head declared arguments-only is not a structural built-in: its argument keeps its whole value")
		}
	case "arguments_control":
		// The control for the full-scope reader above: the same implementation
		// shape, declaring that it reads only its own arguments' values. The
		// declaration is the promise, so the row decodes no property body and
		// the callback sees the header the declaration asked for. An implicit
		// read is not detected and is not trusted with a body it never claimed.
		cypherfn.Register("tolower", readVisibleBindingFunction(0), cypherfn.OpaqueReads, cypherfn.ArgumentInputImage)
		for _, prefix := range scalarNodeStatementPrefixes {
			graph := newScalarNodeDemandGraph(t, true)
			result := graph.run(t, prefix+"MATCH (n:EIBig) RETURN toLower('IGNORED')")
			require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows,
				"an arguments-only declaration decodes no property body for an unnamed binding")
			scan := graph.singleScan(t)
			require.NotNil(t, scan.projection, "an argument-only registration needs no property body")
			require.Empty(t, scan.projection)
		}
	case "header":
		// A header declaration narrows what a custom implementation may read: it
		// receives identity, labels and existence, so the row decodes no property
		// body for it.
		cypherfn.Register("property_exists", func(_ context.Context, _ cypherfn.Context, _ []string) (interface{}, error) {
			return "header only", nil
		}, cypherfn.OpaqueReads, cypherfn.HeaderInputImage)
		for _, prefix := range scalarNodeStatementPrefixes {
			graph := newScalarNodeDemandGraph(t, true)
			result := graph.run(t, prefix+"MATCH (n:EIBig) RETURN property_exists(n, 'small')")
			require.Equal(t, [][]interface{}{{"header only"}}, result.Rows)
			projection := graph.singleScan(t).projection
			require.NotNil(t, projection, "a header declaration decodes no property body")
			require.Empty(t, projection)
		}
	case "structural_name":
		// A custom implementation of a structural built-in's name declares its
		// own demand: size's name does not grant the planner's cardinality
		// contract, so an arguments-only declaration keeps its argument's whole
		// value instead of reading the list's element count.
		cypherfn.Register("size", func(_ context.Context, _ cypherfn.Context, _ []string) (interface{}, error) {
			return int64(-1), nil
		}, cypherfn.OpaqueReads, cypherfn.ArgumentInputImage)
		for _, prefix := range scalarNodeStatementPrefixes {
			graph := newScalarNodeDemandGraph(t, true)
			graph.run(t, prefix+"MATCH (n:EIBig) RETURN size([n])")
			require.Nil(t, graph.singleScan(t).projection,
				"a custom size declared arguments-only is not a structural built-in: the name alone narrows nothing")
		}
	}
}

// A registration that declares only its arguments keeps the row out of the
// statement: the built-in toUpper reads the literal's value, so a MATCH variable
// the statement never mentions is not decoded for it.
func TestScalarNodeArgumentOnlyCallKeepsUnrelatedBindingHeader(t *testing.T) {
	for _, prefix := range scalarNodeStatementPrefixes {
		graph := newScalarNodeDemandGraph(t, true)
		result := graph.run(t, prefix+"MATCH (n:EIBig) RETURN toUpper('small')")
		require.Equal(t, [][]interface{}{{"SMALL"}}, result.Rows)
		projection := graph.singleScan(t).projection
		require.NotNil(t, projection, "an argument-only registration needs no property body")
		require.Empty(t, projection)
	}
}

// The declarations are read at planning for both statement parsers: each
// statement below asks for the same demand under Cypher 5 and Cypher 25, so the
// syntax the planner reads is the same promise either way.
func TestScalarNodeInputImagesAgreeAcrossParsers(t *testing.T) {
	for _, tc := range []struct {
		name       string
		statement  string
		properties []string
	}{
		{"header_scalars", "MATCH (n:EIBig) RETURN id(n), elementId(n), labels(n), n IS NOT NULL", []string{}},
		{"list_size", "MATCH (n:EIBig) RETURN size([n]), size(collect(n))", []string{}},
		{"property", "MATCH (n:EIBig) RETURN n.small", []string{scalarNodeSmallProperty}},
		{"head_property", "MATCH (n:EIBig) RETURN head([n]).small", []string{scalarNodeSmallProperty}},
		{"property_body", "MATCH (n:EIBig) RETURN n.small, properties(n).small", nil},
		{"node_escape", "MATCH (n:EIBig) RETURN n", nil},
		{"dynamic_subscript", "MATCH (n:EIBig) RETURN n['a' + 'b']", nil},
		{"alias_escape", "MATCH (n:EIBig) WITH n AS other RETURN other", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, prefix := range scalarNodeStatementPrefixes {
				graph := newScalarNodeDemandGraph(t, true)
				graph.run(t, prefix+tc.statement)
				projection := graph.singleScan(t).projection
				if tc.properties == nil {
					require.Nil(t, projection, prefix+tc.statement)
					continue
				}
				require.NotNil(t, projection, prefix+tc.statement)
				require.Equal(t, tc.properties, projection, prefix+tc.statement)
			}
		})
	}
}
