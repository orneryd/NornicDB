package cypher

import (
	"context"
	"errors"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestOrdinaryRelationshipIndexMaintenanceAndDuplicateFallback(t *testing.T) {
	for _, parser := range relationshipEqualityParsers {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, explicit := range []bool{false, true} {
				mode := "auto"
				if explicit {
					mode = "transaction"
				}
				t.Run(mode, func(t *testing.T) {
					f := newRelationshipEqualityCountedFixture(t, 128, relationshipEqualityOrdinaryIndexDeclaration)
					ctx := context.Background()
					if explicit {
						ctx = beginRelationshipEqualityTransaction(t, f.exec)
					}
					query := func(value string) [][]interface{} {
						r, err := f.exec.Execute(ctx, relationshipEqualityAccessQuery, map[string]interface{}{"id": value})
						require.NoError(t, err)
						return r.Rows
					}
					want := [][]interface{}{{"r289999", "n73", "n128561"}}
					require.Equal(t, want, query("r289999"))
					_, err := f.exec.Execute(ctx, "MATCH ()-[r:PD_RELATION {id:'r289999'}]->() SET r.id='moved'", nil)
					require.NoError(t, err)
					require.Empty(t, query("r289999"))
					require.Equal(t, [][]interface{}{{"moved", "n73", "n128561"}}, query("moved"))
					_, err = f.exec.Execute(ctx, "MATCH ()-[r:PD_RELATION {id:'moved'}]->() DELETE r", nil)
					require.NoError(t, err)
					require.Empty(t, query("moved"))
					_, err = f.exec.Execute(ctx, "MATCH (a:PDGraphNode {id:'n73'}), (b:PDGraphNode {id:'n128561'}) CREATE (a)-[:PD_RELATION {id:'r289999'}]->(b)", nil)
					require.NoError(t, err)
					require.Equal(t, want, query("r289999"), "recreated value must be indexed")
					_, err = f.exec.Execute(ctx, "MATCH (a:PDGraphNode {id:'n73'}), (b:PDGraphNode {id:'n128561'}) CREATE (a)-[:PD_RELATION {id:'r289999'}]->(b)", nil)
					require.NoError(t, err, "an ordinary index must not enforce uniqueness")
					duplicate := f.profile(t, ctx)
					require.Equal(t, [][]interface{}{want[0], want[0]}, duplicate.rows, "complete fallback keeps every matching edge")
					require.Greater(t, duplicate.reads, int64(2), "multiple hits retain the current complete scan")
				})
			}
		})
	}
}

func TestOrdinaryRelationshipIndexDropAndRestart(t *testing.T) {
	for _, parser := range relationshipEqualityParsers {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			path := t.TempDir()
			open := func() (*storage.BadgerEngine, storage.Engine) {
				base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(path))
				require.NoError(t, err)
				t.Cleanup(func() { _ = base.Close() })
				return base, storage.NewNamespacedEngine(base, "pd")
			}
			base, store := open()
			initial := populateRelationshipEqualityFixture(t, store, relationshipEqualityFixtureOptions{unrelatedRelationships: 8, ordinaryIndex: true})
			f := &relationshipEqualityCountedFixture{exec: initial.exec}
			before := f.profile(t, t.Context())
			require.Equal(t, int64(2), before.reads)
			require.NoError(t, base.Close())
			_, store = open()
			f.exec = NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
			require.Equal(t, before.rows, f.profile(t, t.Context()).rows, "persisted declaration rebuilds through the same owner")
			require.Equal(t, int64(2), f.profile(t, t.Context()).reads)
			_, err := f.exec.Execute(t.Context(), "DROP INDEX pd_relation_id_ordinary", nil)
			require.NoError(t, err)
			afterDrop := f.profile(t, t.Context())
			require.Equal(t, before.rows, afterDrop.rows)
			require.Equal(t, int64(11), afterDrop.reads, "DROP removes discovery even if derived postings remain retained")
			_, err = f.exec.Execute(t.Context(), relationshipEqualityOrdinaryIndexDeclaration, nil)
			require.NoError(t, err)
			require.Equal(t, int64(2), f.profile(t, t.Context()).reads)
		})
	}
}

type failingOrdinaryRelationshipIndexBuild struct {
	storage.Engine
	failure error
	builds  int
}

func (s *failingOrdinaryRelationshipIndexBuild) GetInnerEngine() storage.Engine { return s.Engine }
func (s *failingOrdinaryRelationshipIndexBuild) BuildRelationshipPropertyEqualityIndex(context.Context, string, string, []string) error {
	s.builds++
	return s.failure
}
func (s *failingOrdinaryRelationshipIndexBuild) LookupEdgesByPropertyEqualityInNamespace(context.Context, string, string, []string, []interface{}) ([]*storage.Edge, bool, error) {
	return nil, false, nil
}

func TestOrdinaryRelationshipIndexBuildFailureAndIfNotExists(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "new declaration"
		if existing {
			name = "existing declaration"
		}
		t.Run(name, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "build_failure")
			if existing {
				require.NoError(t, store.GetSchema().AddRangeIndexForEntity("ordinary", "T", []string{"id"}, storage.ConstraintEntityRelationship))
			}
			sentinel := errors.New("relationship index build failed")
			failed := &failingOrdinaryRelationshipIndexBuild{Engine: store, failure: sentinel}
			exec := NewStorageExecutor(failed)
			_, err := exec.Execute(t.Context(), "CREATE INDEX ordinary IF NOT EXISTS FOR ()-[r:T]-() ON (r.id)", nil)
			if existing {
				require.NoError(t, err)
				require.Zero(t, failed.builds, "IF NOT EXISTS never rebuilds or removes the old declaration")
			} else {
				require.ErrorIs(t, err, sentinel)
				require.Equal(t, 1, failed.builds)
			}
			result, showErr := exec.Execute(t.Context(), "SHOW INDEXES YIELD name WHERE name='ordinary' RETURN name", nil)
			require.NoError(t, showErr)
			if existing {
				require.Equal(t, [][]interface{}{{"ordinary"}}, result.Rows)
			} else {
				require.Empty(t, result.Rows)
			}
		})
	}
}
