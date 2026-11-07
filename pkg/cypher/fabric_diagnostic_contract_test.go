package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

// A body that cannot conclude is one graph's clause composition rule for the
// whole statement, not a participant's verdict on the fragment it runs. The
// statement's admission owner reports it before any part of the statement
// plans, enlists a participant or runs, so a composite statement sees the same
// canonical message, status and detail as the same body on one database, on
// every route and for EXPLAIN, PROFILE and execution alike.
func TestFabricQueryBodyCompositionDiagnostic(t *testing.T) {
	canonical := localization.CypherMatchingQueryCannotConclude("CALL").Fallback
	exec := newFabricCallRowsExecutor(t, true)
	for _, shape := range []struct {
		name string
		body func(use string) string
	}{
		{"unit", func(use string) string {
			return use + "UNWIND [] AS seed CALL { CALL { RETURN 'inner' AS hidden } } RETURN *"
		}},
		{"unit/union", func(use string) string {
			return use + "UNWIND [] AS seed CALL { CALL { RETURN 'inner' AS hidden } } RETURN * UNION " + use + "RETURN 'outer' AS seed"
		}},
		{"unit/union_all", func(use string) string {
			return use + "UNWIND [] AS seed CALL { CALL { RETURN 'inner' AS hidden } } RETURN * UNION ALL " + use + "RETURN 'outer' AS seed"
		}},
		{"optional", func(use string) string {
			return use + "UNWIND [] AS seed OPTIONAL CALL { CALL { RETURN 'inner' AS hidden } } RETURN *"
		}},
		{"optional/union", func(use string) string {
			return use + "UNWIND [] AS seed OPTIONAL CALL { CALL { RETURN 'inner' AS hidden } } RETURN * UNION " + use + "RETURN 'outer' AS seed"
		}},
		{"optional/union_all", func(use string) string {
			return use + "UNWIND [] AS seed OPTIONAL CALL { CALL { RETURN 'inner' AS hidden } } RETURN * UNION ALL " + use + "RETURN 'outer' AS seed"
		}},
	} {
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			t.Run(shape.name+"/"+mode, func(t *testing.T) {
				composite := unionNarrowObserve(exec, "", "", mode+shape.body("USE comp.one "))
				native := unionNarrowObserve(exec, "", "", mode+shape.body(""))
				// The participant's failure text and positional prefixes are not
				// this statement's diagnostic: the sentence is Neo4j's own.
				require.Equal(t, canonical, composite.Error, composite.Query)
				require.Equal(t, canonical, native.Error, native.Query)
				require.Equal(t, nornicerrors.StatementSyntaxError, composite.Code, composite.Query)
				require.Equal(t, "InvalidClauseComposition", composite.Detail, composite.Query)
				require.Equal(t, native.Code, composite.Code, composite.Query)
				require.Equal(t, native.Detail, composite.Detail, composite.Query)
			})
		}
	}
}

// Statement errors reach Bolt and HTTP through Neo4jStatus, whose message is
// the diagnostic itself: a composite statement's composition failure must not
// carry the Fabric boundary's routing or participant text, and the rejection
// must leave no effect behind on any participant.
func TestFabricQueryBodyCompositionPropagation(t *testing.T) {
	exec := newFabricCallRowsExecutor(t, true)
	const statement = "USE comp.one CREATE (probe:DiagnosticProbe) CALL { CALL { RETURN 'inner' AS hidden } } RETURN *"
	_, err := exec.Execute(context.Background(), statement, nil)
	require.Error(t, err)
	code, message := nornicerrors.Neo4jStatus(err)
	require.Equal(t, nornicerrors.StatementSyntaxError, code)
	require.Equal(t, localization.CypherMatchingQueryCannotConclude("CALL").Fallback, message)
	var detailed interface{ BoltErrorDetail() string }
	require.ErrorAs(t, err, &detailed)
	require.Equal(t, "InvalidClauseComposition", detailed.BoltErrorDetail())

	probe := unionNarrowObserve(exec, "", "", "USE comp.one MATCH (probe:DiagnosticProbe) RETURN count(*) AS remaining")
	require.Empty(t, probe.Error, probe.Query)
	require.Len(t, probe.Rows, 1, probe.Query)
	require.EqualValues(t, 0, probe.Rows[0][0], probe.Query)
}
