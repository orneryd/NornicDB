package cypher

// NOR-287 reproduction (release head d6ec7766e6e73dbae401ae547aa3c8b6e4c327b5).
//
// These are the supported public Cypher entry points that would read the merged
// composite schema or the composite count catalogue. On a composite root they
// are rejected before any executor reaches storage.GetSchema, under both
// configured parsers, so the composite schema admission skip is not reachable
// from public Cypher today. This test records that boundary; it does not ask
// for any change to the rejection.

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNOR287CompositeRootRejectsMergedSchemaAndCountConsumers(t *testing.T) {
	statements := []string{
		"SHOW INDEXES",
		"SHOW CONSTRAINTS",
		// The constraint-contract branch is behind the same composite guard;
		// it is listed because it is the one SHOW branch that reaches
		// storage.GetSchema outside the two guarded handlers' own reads.
		"SHOW CONSTRAINT CONTRACTS",
		"CALL db.stats.retrieve('GRAPH COUNTS')",
		"CALL db.labels()",
		"CALL db.schema.visualization()",
	}

	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })

			_, mgr := compositeTestFixture(t)
			exec := newCompositeTestExecutor(t, mgr, "comp", &testDatabaseManagerAdapter{manager: mgr})

			for _, statement := range statements {
				t.Run(statement, func(t *testing.T) {
					result, err := exec.Execute(t.Context(), statement, nil)
					require.Error(t, err, "a composite root must not answer %q from merged constituent state", statement)
					require.Nil(t, result)
					require.Contains(t, strings.ToLower(err.Error()), "composite",
						"the rejection must name the composite target requirement, got: %v", err)
				})
			}
		})
	}
}
