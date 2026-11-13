package cypher

import (
	"fmt"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSymmetricEpochIndexSurvivesRestart(t *testing.T) {
	withParserTypes(t, func(t *testing.T) {
		for _, buffered := range []bool{false, true} {
			t.Run(fmt.Sprint("write-behind=", buffered), func(t *testing.T) {
				dir := t.TempDir()
				open := func() (*storage.BadgerEngine, *StorageExecutor) {
					base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, WriteBehind: buffered, WriteBehindInterval: time.Hour}))
					require.NoError(t, err)
					return base, NewStorageExecutor(storage.NewNamespacedEngine(base, "test"))
				}
				base, exec := open()
				for _, q := range []string{
					"CREATE CONSTRAINT epochs FOR (n:Period) REQUIRE temporal.epoch(n.start,n.end) GROUP BY n.key",
					"CREATE (:Period {key:'a',name:'past',start:null,end:datetime('2026-02-01T00:00:00Z')})",
					"CREATE (:Period {key:'a',name:'future',start:datetime('2026-02-01T00:00:00Z'),end:null})",
				} {
					_, err := exec.Execute(t.Context(), q, nil)
					require.NoError(t, err)
				}
				check := func() {
					t.Helper()
					for _, tc := range []struct{ at, name string }{{"1500-01-01", "past"}, {"2026-01-31", "past"}, {"2026-02-01", "future"}} {
						result, err := exec.Execute(t.Context(), "CALL db.temporal.asOf('Period','key','a','start','end',$at) YIELD node RETURN node.name AS name", map[string]interface{}{"at": tc.at})
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{tc.name}}, result.Rows)
					}
					_, err := exec.Execute(t.Context(), "CALL db.temporal.assertNoOverlap('Period','key','start','end','unused',null,datetime('2026-02-01T00:00:00Z')) YIELD ok RETURN ok", nil)
					require.NoError(t, err)
					_, err = exec.Execute(t.Context(), "CALL db.temporal.assertNoOverlap('Period','key','start','end','a',null,datetime('2026-02-01T00:00:00Z')) YIELD ok RETURN ok", nil)
					require.Error(t, err)
				}
				check()
				require.NoError(t, base.Close())
				base, exec = open()
				check()
				require.NoError(t, base.Close())
			})
		}
	})
}

func TestSymmetricEpochConcurrentClaims(t *testing.T) {
	withParserTypes(t, func(t *testing.T) {
		for _, relationship := range []bool{false, true} {
			for _, block := range []bool{false, true} {
				for _, past := range []bool{false, true} {
					t.Run(fmt.Sprintf("relationship=%t/block=%t/openPast=%t", relationship, block, past), func(t *testing.T) {
						owner, engine := newConstraintGapExecutor(t)
						variable, pattern, create := "n", "(n:Period)", "CREATE (:Period {key:'asset',start:%s,end:%s})"
						if relationship {
							variable, pattern, create = "r", "()-[r:PERIOD]-()", "MATCH (a:Endpoint {id:1}),(b:Endpoint {id:2}) CREATE (a)-[:PERIOD {key:'asset',start:%s,end:%s}]->(b)"
							_, err := owner.Execute(t.Context(), "CREATE (:Endpoint {id:1}),(:Endpoint {id:2})", nil)
							require.NoError(t, err)
						}
						requirement := fmt.Sprintf("temporal.epoch(%s.start,%s.end) GROUP BY %s.key", variable, variable, variable)
						if block {
							requirement = "{ " + requirement + " }"
						}
						_, err := owner.Execute(t.Context(), "CREATE CONSTRAINT epochs FOR "+pattern+" REQUIRE "+requirement, nil)
						require.NoError(t, err)
						from, to := "datetime('2026-01-01T00:00:00Z')", "null"
						if past {
							from, to = "null", "datetime('2026-02-01T00:00:00Z')"
						}
						query := fmt.Sprintf(create, from, to)
						start := make(chan struct{})
						outcomes := make(chan error, 2)
						for i := 0; i < 2; i++ {
							exec := NewStorageExecutor(engine)
							go func() { <-start; _, err := exec.Execute(t.Context(), query, nil); outcomes <- err }()
						}
						close(start)
						accepted := 0
						for i := 0; i < 2; i++ {
							if <-outcomes == nil {
								accepted++
							}
						}
						require.Equal(t, 1, accepted, "only one competing unbounded epoch can be acknowledged")
					})
				}
			}
		}
	})
}
