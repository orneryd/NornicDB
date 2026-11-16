package cypher

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A registration's input image is the promise its own declaration makes, not an
// inference from the name it is registered under. The registered identity
// replacement below reads a property of its argument, so while it owns "id" the
// statement keeps every visible binding's whole body, and the callback receives
// the 128 KiB property the fixture's statement never names.
//
// The registry replacement is process-wide and documented as mutable, so each
// statement parser runs the replacement in its own isolated process, as the
// other registry regressions do.
func TestScalarNodeDemandPreservesRegisteredIdentityOverride(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		cypherfn.Register("id", func(_ context.Context, scope cypherfn.Context, args []string) (interface{}, error) {
			value, err := scope.Eval(args[0])
			if err != nil {
				return nil, err
			}
			return value.(*storage.Node).Properties[scalarNodeUnusedProperty], nil
		}, cypherfn.OpaqueReads, cypherfn.FullInputImage)

		// The whole call reads through the declared full-scope demand: nothing
		// about the name "id" narrows the binding its implementation may read.
		graph := newScalarNodeDemandGraph(t, true)
		graph.run(t, "MATCH (n:EIBig) RETURN id(n)")
		graph.wholeBodyScans(t)

		// The same registration receiving the call: an expression position that
		// reaches the registered implementation evaluates the argument's value,
		// so the property body the declaration kept is the one the callback
		// reads. A whole standalone id() call is answered by the row evaluator's
		// own identity boundary without asking the registry, which this demand
		// migration does not change.
		result := graph.run(t, "MATCH (n:EIBig) RETURN id(n) + ''")
		require.Len(t, result.Rows, 1)
		require.Len(t, result.Rows[0], 1)
		value, ok := result.Rows[0][0].(string)
		require.True(t, ok, "registered identity override did not receive its property body: got %T", result.Rows[0][0])
		require.Equal(t, sha256.Sum256([]byte(strings.Repeat("x", scalarNodeUnusedBytes))), sha256.Sum256([]byte(value)))
	})
}
