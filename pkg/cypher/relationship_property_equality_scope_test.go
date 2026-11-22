package cypher

import (
	"context"
	"fmt"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This file pins the scope of the accepted NOR-42 relationship-property
// equality slice at the public Cypher boundary, with counted reads instead of
// durations:
//
//   - a declared relationship UNIQUE constraint owns an equality index, so the
//     release statement reads the relationship its indexed values select and a
//     population of unrelated relationships of the same type adds no reads;
//   - an ordinary (non-unique) relationship CREATE INDEX is accepted and
//     reported ONLINE, but it owns no equality lookup: the statement keeps the
//     complete type scan and the same correct rows. A missing speed-up from an
//     ordinary index is therefore not by itself a defect of the accepted slice.
//     Its declaration being a metadata-only no-op (no owner builds or maintains
//     it) is the acknowledged broader gap these controls record rather than
//     silently accept;
//   - the same statement with no declaration is the sensitivity control: it
//     returns the identical row and its counted reads grow with the type's
//     unrelated population, so a flat count above is a property of the equality
//     lookup, not of the counter.

// relationshipEqualityConstraintDeclaration is the canonical accepted-slice
// declaration the relationship equality lookup requires.
const relationshipEqualityConstraintDeclaration = "CREATE CONSTRAINT pd_relation_id IF NOT EXISTS FOR ()-[r:PD_RELATION]-() REQUIRE r.id IS UNIQUE"

// relationshipEqualityOrdinaryIndexDeclaration is the release report's
// standalone relationship property index declaration, without uniqueness.
const relationshipEqualityOrdinaryIndexDeclaration = "CREATE INDEX pd_relation_id_ordinary FOR ()-[r:PD_RELATION]-() ON (r.id)"

// relationshipEqualityTransactions are the two execution modes the release
// report measured in: an auto-commit statement and a statement inside an
// explicit read transaction.
var relationshipEqualityTransactions = []struct {
	name     string
	explicit bool
}{
	{"auto-commit", false},
	{"explicit transaction", true},
}

// relationshipEqualityCountedFixture is the canonical access fixture's
// population — two endpoints, the one relationship the indexed value selects,
// and the unrelated relationships of the same type — with the schema
// declarations supplied by the case instead of always the canonical constraint.
type relationshipEqualityCountedFixture struct {
	exec *StorageExecutor
}

func newRelationshipEqualityCountedFixture(t testing.TB, population int, declarations ...string) *relationshipEqualityCountedFixture {
	t.Helper()
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "pd")
	ctx := context.Background()
	for _, id := range []string{"n73", "n128561"} {
		_, err := store.CreateNode(ctx, &storage.Node{
			ID:         storage.NodeID(id),
			Labels:     []string{"PDGraphNode"},
			Properties: map[string]interface{}{"id": id, "archived": false},
		})
		require.NoError(t, err)
	}
	createRelationship := func(edgeID, value string) {
		require.NoError(t, store.CreateEdge(ctx, &storage.Edge{
			ID:         storage.EdgeID(edgeID),
			StartNode:  "n73",
			EndNode:    "n128561",
			Type:       "PD_RELATION",
			Properties: map[string]interface{}{"id": value},
		}))
	}
	for i := 0; i < population; i++ {
		createRelationship(fmt.Sprintf("unrelated%d", i), fmt.Sprintf("x%d", i))
	}
	createRelationship("target", "r289999")
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	for _, declaration := range declarations {
		_, err := exec.Execute(ctx, declaration, nil)
		require.NoError(t, err, declaration)
	}
	return &relationshipEqualityCountedFixture{exec: exec}
}

// relationshipEqualityCountedRead is one PROFILE execution of the release
// statement: the rows it answered and the entity reads its plan reported. The
// statement is the production public query; only its access path differs.
type relationshipEqualityCountedRead struct {
	rows  [][]interface{}
	reads int64
}

// profile runs the canonical release statement through the public Execute
// path as a PROFILE statement, so the read count is the plan's own record of
// what the statement read rather than a measured duration.
func (f *relationshipEqualityCountedFixture) profile(t *testing.T, ctx context.Context) relationshipEqualityCountedRead {
	t.Helper()
	result, err := f.exec.Execute(ctx, "PROFILE "+relationshipEqualityAccessQuery, map[string]interface{}{"id": "r289999"})
	require.NoError(t, err)
	plan := executionPlanFromResult(t, result)
	require.True(t, plan.TotalDBHitsObserved, "PROFILE must report the reads the statement performed")
	return relationshipEqualityCountedRead{rows: result.Rows, reads: plan.TotalDBHits}
}

// measure builds the requested execution mode inside one counted fixture and
// profiles the release statement once.
func (f *relationshipEqualityCountedFixture) measure(t *testing.T, explicit bool) relationshipEqualityCountedRead {
	t.Helper()
	ctx := context.Background()
	if explicit {
		ctx = beginRelationshipEqualityTransaction(t, f.exec)
	}
	return f.profile(t, ctx)
}

// TestRelationshipPropertyEqualityCountedReadsStayAtTheSelectedRelationship
// states the accepted slice with counted reads: on each parser, in each
// execution mode, the release statement reads the same entities at 8 and at
// 512 unrelated relationships and returns the one correct row. The no-index
// control returns the identical row and its reads grow with that population, so
// the flat count is the equality lookup and not an artifact of PROFILE.
func TestRelationshipPropertyEqualityCountedReadsStayAtTheSelectedRelationship(t *testing.T) {
	expected := [][]interface{}{{"r289999", "n73", "n128561"}}
	for _, parser := range relationshipEqualityParsers {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, transaction := range relationshipEqualityTransactions {
				t.Run(transaction.name, func(t *testing.T) {
					measure := func(population int, declarations ...string) relationshipEqualityCountedRead {
						t.Helper()
						fixture := newRelationshipEqualityCountedFixture(t, population, declarations...)
						read := fixture.measure(t, transaction.explicit)
						require.Equal(t, expected, read.rows)
						return read
					}
					small := measure(relationshipEqualityAllocationSmallPopulation, relationshipEqualityConstraintDeclaration)
					large := measure(relationshipEqualityAllocationLargePopulation, relationshipEqualityConstraintDeclaration)
					scanSmall := measure(relationshipEqualityAllocationSmallPopulation)
					scanLarge := measure(relationshipEqualityAllocationLargePopulation)
					t.Logf("constrained reads: %d at %d unrelated relationships, %d at %d; complete scan: %d and %d",
						small.reads, relationshipEqualityAllocationSmallPopulation,
						large.reads, relationshipEqualityAllocationLargePopulation,
						scanSmall.reads, scanLarge.reads)
					require.Equal(t, small.reads, large.reads,
						"the constrained lookup must read the same entities at %d and at %d unrelated relationships",
						relationshipEqualityAllocationSmallPopulation, relationshipEqualityAllocationLargePopulation)
					require.Greater(t, scanLarge.reads, scanSmall.reads,
						"the counted reader must grow when the complete type scan enumerates the unrelated population")
					require.Less(t, small.reads, scanLarge.reads,
						"the constrained lookup must read fewer entities than the complete scan")
				})
			}
		})
	}
}

// TestRelationshipPropertyEqualityOrdinaryRelationshipIndexUsesDerivedOwner
// retains the recovered catalog assertions and scan sensitivity while replacing
// the historical no-op observation with the corrected selective lookup contract.
func TestRelationshipPropertyEqualityOrdinaryRelationshipIndexUsesDerivedOwner(t *testing.T) {
	expected := [][]interface{}{{"r289999", "n73", "n128561"}}
	const showIndexQuery = "SHOW INDEXES YIELD name, state, type, entityType, owningConstraint WHERE name = $name RETURN name, state, type, entityType, owningConstraint"
	for _, parser := range relationshipEqualityParsers {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())

			// The accepted-slice declaration owns an index.
			owned := newRelationshipEqualityCountedFixture(t, relationshipEqualityAllocationSmallPopulation, relationshipEqualityConstraintDeclaration)
			ownedShow, err := owned.exec.Execute(context.Background(), showIndexQuery, map[string]interface{}{"name": "pd_relation_id"})
			require.NoError(t, err)
			require.Len(t, ownedShow.Rows, 1)
			ownedRow := rowMap(t, ownedShow, 0)
			require.Equal(t, "RANGE", ownedRow["type"])
			require.Equal(t, "RELATIONSHIP", ownedRow["entityType"])
			require.Equal(t, "pd_relation_id", ownedRow["owningConstraint"], "the constraint owns the index it answers reads from")

			// The ordinary declaration is accepted and reported ONLINE, but no
			// constraint owns it.
			ordinary := newRelationshipEqualityCountedFixture(t, relationshipEqualityAllocationSmallPopulation, relationshipEqualityOrdinaryIndexDeclaration)
			show, err := ordinary.exec.Execute(context.Background(), showIndexQuery, map[string]interface{}{"name": "pd_relation_id_ordinary"})
			require.NoError(t, err)
			require.Len(t, show.Rows, 1, "the ordinary relationship index declaration is accepted")
			row := rowMap(t, show, 0)
			require.Equal(t, "RANGE", row["type"])
			require.Equal(t, "ONLINE", row["state"])
			require.Equal(t, "RELATIONSHIP", row["entityType"])
			require.Nil(t, row["owningConstraint"], "no constraint owns the ordinary relationship index")

			// The ordinary declaration now uses the same derived equality owner,
			// while the undeclared control still measures the complete type scan.
			measured := func(population int, declarations ...string) relationshipEqualityCountedRead {
				t.Helper()
				read := newRelationshipEqualityCountedFixture(t, population, declarations...).measure(t, false)
				require.Equal(t, expected, read.rows, "the complete scan must return the same correct row")
				return read
			}
			ordinarySmall := measured(relationshipEqualityAllocationSmallPopulation, relationshipEqualityOrdinaryIndexDeclaration)
			ordinaryLarge := measured(relationshipEqualityAllocationLargePopulation, relationshipEqualityOrdinaryIndexDeclaration)
			scanSmall := measured(relationshipEqualityAllocationSmallPopulation)
			scanLarge := measured(relationshipEqualityAllocationLargePopulation)
			t.Logf("ordinary index reads: %d at %d unrelated relationships, %d at %d; complete scan: %d and %d",
				ordinarySmall.reads, relationshipEqualityAllocationSmallPopulation,
				ordinaryLarge.reads, relationshipEqualityAllocationLargePopulation,
				scanSmall.reads, scanLarge.reads)
			require.Equal(t, int64(2), ordinarySmall.reads)
			require.Equal(t, ordinarySmall.reads, ordinaryLarge.reads)
			require.Equal(t, int64(11), scanSmall.reads)
			require.Equal(t, int64(515), scanLarge.reads)

		})
	}
}

// TestNOR42OnlineOrdinaryRelationshipIndexSuppliesEqualityLookup is the
// fail-first contract from the ticket's later no-op declaration report. This
// does not assert milliseconds or a general optimizer choice: the only
// declared index is accepted as ONLINE and now uses the canonical build, update
// and read owner. The constraint and no-index controls preserve sensitivity.
func TestNOR42OnlineOrdinaryRelationshipIndexSuppliesEqualityLookup(t *testing.T) {
	for _, parser := range relationshipEqualityParsers {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, transaction := range relationshipEqualityTransactions {
				t.Run(transaction.name, func(t *testing.T) {
					small := newRelationshipEqualityCountedFixture(t, relationshipEqualityAllocationSmallPopulation, relationshipEqualityOrdinaryIndexDeclaration).measure(t, transaction.explicit)
					large := newRelationshipEqualityCountedFixture(t, relationshipEqualityAllocationLargePopulation, relationshipEqualityOrdinaryIndexDeclaration).measure(t, transaction.explicit)
					require.Equal(t, [][]interface{}{{"r289999", "n73", "n128561"}}, small.rows)
					require.Equal(t, small.rows, large.rows)
					require.Equal(t, int64(2), small.reads)
					require.Equal(t, small.reads, large.reads, "an ONLINE equality index must select the matching relationship without enumerating unrelated relationships")
				})
			}
		})
	}
}
