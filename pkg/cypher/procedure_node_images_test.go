package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The node-image contract is a declaration each registration makes about the
// node objects a call hands to its handler. These tests cover the registry
// mechanics: the zero value is the native full contract, a declaration is kept,
// an unknown declaration is refused, and the bundled registrations make the
// declaration their audited handlers support.

func TestProcedureRegistrationPreservesNodeImageContract(t *testing.T) {
	for _, register := range []struct {
		name string
		call func(*ProcedureRegistry, ProcedureSpec, ProcedureHandler) error
	}{
		{"built-in", (*ProcedureRegistry).RegisterBuiltIn},
		{"user", (*ProcedureRegistry).RegisterUser},
	} {
		t.Run(register.name, func(t *testing.T) {
			for _, images := range []ProcedureNodeImages{FullNodeImages, MetadataNodeImages, 2, 255} {
				registry := NewProcedureRegistry()
				spec := ProcedureSpec{Name: "test.nodeimages", Mode: ProcedureModeRead, NodeImages: images}
				err := register.call(registry, spec, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
					return &ExecuteResult{}, nil
				})
				procedure, found := registry.Get(spec.Name)
				if images > MetadataNodeImages {
					require.Error(t, err)
					require.False(t, found, "rejected declarations must not enter the live registry")
				} else {
					require.NoError(t, err)
					require.True(t, found)
					require.Equal(t, images, procedure.Spec.NodeImages)
				}
			}
		})
	}
}

// TestBundledProcedureNodeImagesFollowTheirReviewedHandlers checks the two
// declarations the bundle makes: a bundled handler that reads a node argument
// reads its identity, labels and Cypher properties, so the bundle declares the
// metadata image for it, while a procedure that runs statement text keeps the
// native full image.
func TestBundledProcedureNodeImagesFollowTheirReviewedHandlers(t *testing.T) {
	ensureBuiltInProceduresRegistered()

	// These procedures take a node (or a node list) argument, and each reviewed
	// handler resolves it to node IDs and properties.
	nodeArgumentProcedures := []string{
		"db.create.setNodeVectorProperty",
		"kp.suppress",
		"kp.unsuppress",
		"apoc.path.subgraphNodes",
		"apoc.path.subgraphAll",
		"apoc.path.expand",
		"apoc.path.expandConfig",
		"apoc.path.spanningTree",
		"apoc.algo.dijkstra",
		"apoc.algo.aStar",
		"apoc.algo.allSimplePaths",
		"apoc.algo.pageRank",
		"apoc.algo.betweenness",
		"apoc.algo.closeness",
		"apoc.algo.louvain",
		"apoc.algo.labelPropagation",
		"apoc.algo.wcc",
		"apoc.neighbors.tohop",
		"apoc.neighbors.byhop",
	}
	for _, name := range nodeArgumentProcedures {
		procedure, found := globalProcedureRegistry.Get(name)
		require.True(t, found, name)
		require.Equal(t, MetadataNodeImages, procedure.Spec.NodeImages, name)
	}

	// A procedure that runs statement text can hand the node it is given to a
	// consumer this bundle cannot review.
	statementProcedures := []string{
		"apoc.cypher.run",
		"apoc.cypher.doitall",
		"apoc.cypher.doIt",
		"apoc.cypher.runMany",
		"apoc.export.json.query",
		"apoc.export.csv.query",
	}
	for _, name := range statementProcedures {
		procedure, found := globalProcedureRegistry.Get(name)
		require.True(t, found, name)
		require.True(t, procedure.Spec.ExecutesStatements, name)
		require.Equal(t, FullNodeImages, procedure.Spec.NodeImages, name)
	}

	for _, spec := range globalProcedureRegistry.ListBuiltIns() {
		if spec.ExecutesStatements || spec.OwnsTransactions {
			require.Equal(t, FullNodeImages, spec.NodeImages, spec.Name)
			continue
		}
		require.Equal(t, MetadataNodeImages, spec.NodeImages, spec.Name)
	}
}

// TestDeprecatedProcedureAliasKeepsTheOriginalNodeImageContract checks that an
// alias is the original declaration under another name.
func TestDeprecatedProcedureAliasKeepsTheOriginalNodeImageContract(t *testing.T) {
	ensureBuiltInProceduresRegistered()

	original, found := globalProcedureRegistry.Get("kp.info")
	require.True(t, found)
	alias, found := globalProcedureRegistry.Get("nornicdb.knowledgepolicy.info")
	require.True(t, found)
	require.True(t, alias.Spec.IsDeprecated)
	require.Equal(t, "kp.info", alias.Spec.DeprecatedBy)
	require.Equal(t, original.Spec.NodeImages, alias.Spec.NodeImages)
}

func TestProcedureImageDeclarationReplacementUpdatesCachedQuery(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		const name = "image.rebound"
		cleanupAdmissionProcedure(t, name)
		graph := newNOR413ColdDocGraph(t)
		const query = "MATCH (:Hub)-[:HAS]->(n:Doc {k:3}) CALL image.rebound(n) YIELD value RETURN value"
		for _, images := range []ProcedureNodeImages{MetadataNodeImages, FullNodeImages, MetadataNodeImages} {
			spec := ProcedureSpec{Name: name, Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 1,
				NodeImages: images, Returns: []ProcedureColumn{{Name: "value", Type: "ANY"}}}
			require.NoError(t, RegisterUserProcedure(spec, func(_ context.Context, _ *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
				return &ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{nor413VectorComponent(args[0])}}}, nil
			}))
			var want interface{}
			if images == FullNodeImages {
				want = float64(1)
			}
			// Reusing the exact source exercises both statement effect and
			// semantic caches across changes to the live registration.
			for repeat := 0; repeat < 2; repeat++ {
				require.Equal(t, [][]interface{}{{want}}, graph.run(t, query, nil).Rows)
			}
		}
	})
}
