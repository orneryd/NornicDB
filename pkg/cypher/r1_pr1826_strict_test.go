package cypher

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Review lane R1 of PR #1826: the same cases as before against a control that
// reads whole nodes on every route. The first control (r1FullReads) still let a
// filtered label scan pass its projection to the store, so it was narrowed the
// same way as the subject there.

type r1WholeReads struct{ r1FullReads }

func (f r1WholeReads) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	opts.Projection = nil
	opts.PropertyFilter, opts.PropertyFilterProjection, opts.PropertyStringEquals = nil, nil, nil
	return f.Engine.StreamNodesWithOptions(ctx, opts, visit)
}

func r1StrictControl(store storage.Engine, name string) *StorageExecutor {
	return NewStorageExecutor(r1WholeReads{r1FullReads{store, name}}, QueryCachePolicy{MaxEntries: 0})
}

func TestR1PR1826StrictProofAnswersWholeNodeReads(t *testing.T) {
	cases := append(append(r1ProofCases(), r1Round2Cases()...), r1Round4Cases()...)
	for _, indexed := range []bool{true, false} {
		for _, test := range cases {
			t.Run(fmt.Sprintf("indexed=%t/%s", indexed, test.name), func(t *testing.T) {
				ctx := t.Context()
				want := r1Rows(r1StrictControl(r1Round4Store(t, "namespaced", indexed), "r1").Execute(ctx, test.query, r1Params()))
				exec := NewStorageExecutor(r1Round4Store(t, "namespaced", indexed), QueryCachePolicy{MaxEntries: 0})
				for _, stage := range []string{"cold", "repeated"} {
					got := r1Rows(exec.Execute(ctx, test.query, r1Params()))
					if !reflect.DeepEqual(want, got) {
						t.Errorf("%s: %s\n  whole-node answer: %q\n  answer:            %q", stage, test.query, want, got)
					}
				}
			})
		}
	}
}

func TestR1PR1826StrictApplicationCheck(t *testing.T) {
	for _, indexed := range []bool{true, false} {
		t.Run(fmt.Sprintf("indexed=%t", indexed), func(t *testing.T) {
			ctx := t.Context()
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "r1")
			exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
			statements := []string{
				"CREATE (:File {id:1, restricted:true, owner:'ann', body:'restricted text'}), (:File {id:2, restricted:false, owner:'bob', body:'public text'})",
				"CREATE (a:Account {id:1, blocked:true, token:'t1'})-[:MEMBER]->(:Team {name:'x'})",
			}
			if indexed {
				statements = append([]string{
					"CREATE CONSTRAINT r1_file IF NOT EXISTS FOR (n:File) REQUIRE n.id IS UNIQUE",
					"CREATE CONSTRAINT r1_account IF NOT EXISTS FOR (n:Account) REQUIRE n.id IS UNIQUE",
				}, statements...)
			}
			for _, statement := range statements {
				_, err := exec.Execute(ctx, statement, nil)
				require.NoError(t, err)
			}
			for _, test := range []struct {
				query string
				want  []string
			}{
				{"MATCH (f:File {id:1}) WHERE NOT EXISTS { (f:File {restricted:true}) } RETURN f.body", []string{}},
				{"MATCH (f:File) WHERE f.id = 1 AND NOT EXISTS { (f:File {restricted:true}) } RETURN f.body", []string{}},
				{"MATCH (f:File {id:2}) WHERE NOT EXISTS { (f:File {restricted:true}) } RETURN f.body", []string{"'public text'"}},
				{"MATCH (f:File {id:1}) WHERE COUNT { (f:File {restricted:true}) } = 0 RETURN f.body", []string{}},
				{"MATCH (f:File {id:1}) WHERE EXISTS { (f:File {owner:'ann'}) } RETURN f.body", []string{"'restricted text'"}},
				{"MATCH (f:File {id:1}) MATCH (f:File {owner:'ann'}) RETURN f.body", []string{"'restricted text'"}},
				{"MATCH (f:File {id:1}), (f:File {owner:'ann'}) RETURN f.body", []string{"'restricted text'"}},
				{"MATCH (a:Account {id:1}) WHERE NOT (a:Account {blocked:true})-[:MEMBER]->() RETURN a.token", []string{}},
				{"MATCH (a:Account {id:1}) OPTIONAL MATCH (a:Account {blocked:true})-[:MEMBER]->(t) WITH a, t WHERE t IS NULL RETURN a.token", []string{}},
				{"MATCH (f:File) WHERE NOT EXISTS { (f:File {restricted:true}) } RETURN f.body", []string{"'public text'"}},
				{"MATCH (f:File) WHERE EXISTS { (f:File {owner:'ann'}) } RETURN f.body", []string{"'restricted text'"}},
			} {
				t.Run(test.query, func(t *testing.T) {
					for _, stage := range []string{"cold", "repeated"} {
						got := r1Rows(exec.Execute(ctx, test.query, nil))
						if !reflect.DeepEqual(test.want, got) {
							t.Errorf("%s: %s: want %q, got %q", stage, test.query, test.want, got)
						}
					}
				})
			}
		})
	}
}

func TestR1PR1826StrictDifferential(t *testing.T) {
	for _, indexed := range []bool{true, false} {
		controlStore, store := r1Fixture(t, indexed), r1Fixture(t, indexed)
		control := r1StrictControl(controlStore, "r1")
		exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
		ran, bothErrors := 0, 0
		byFirst := map[string]int{}
		var lines []string
		for f, first := range r1Firsts {
			for m, middle := range r1Middles {
				for r, returned := range r1Returns {
					query := strings.Join(strings.Fields(first+" "+middle.text+" RETURN "+strings.ReplaceAll(returned, "@", middle.variable)), " ")
					ran++
					want := r1Rows(control.Execute(t.Context(), query, r1Params()))
					got := r1Rows(exec.Execute(t.Context(), query, r1Params()))
					wantError := len(want) == 1 && strings.HasPrefix(want[0], "ERROR")
					gotError := len(got) == 1 && strings.HasPrefix(got[0], "ERROR")
					if wantError && gotError {
						bothErrors++
						continue
					}
					if reflect.DeepEqual(want, got) {
						continue
					}
					byFirst[first]++
					lines = append(lines, fmt.Sprintf("R1STRICT indexed=%t f%d m%d r%d :: %s :: whole=%q got=%q", indexed, f, m, r, query, want, got))
				}
			}
		}
		firsts := make([]string, 0, len(byFirst))
		for first := range byFirst {
			firsts = append(firsts, first)
		}
		sort.Strings(firsts)
		for _, first := range firsts {
			t.Logf("R1STRICTCOUNT indexed=%t first %q = %d", indexed, first, byFirst[first])
		}
		t.Logf("R1STRICTTOTAL indexed=%t ran=%d bothErrors=%d mismatches=%d", indexed, ran, bothErrors, len(lines))
		for _, line := range lines {
			fmt.Println(line)
		}
		if len(lines) > 0 {
			t.Errorf("indexed=%t: %d of %d statements answer differently from whole-node reads", indexed, len(lines), ran)
		}
	}
}

// The names test again after a restart, when no whole image of the node is
// cached yet: a reader in one database names properties a node does not have; a
// writer commits in another database of the same server.
func TestR1PR1826AbsentNamesAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	open := func() (*storage.BadgerEngine, *multidb.DatabaseManager) {
		base, err := storage.NewBadgerEngine(dir)
		require.NoError(t, err)
		manager, err := multidb.NewDatabaseManager(ctx, base, nil)
		require.NoError(t, err)
		return base, manager
	}
	run := func(manager *multidb.DatabaseManager, database, statement string) {
		store, err := manager.GetStorage(database)
		require.NoError(t, err)
		_, err = NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0}).Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	base, manager := open()
	require.NoError(t, manager.CreateDatabase(ctx, multidb.DatabaseCreateRequest{Name: "tenant"}))
	run(manager, "tenant", "CREATE CONSTRAINT r1_names IF NOT EXISTS FOR (n:Doc) REQUIRE n.key IS UNIQUE")
	run(manager, "tenant", "CREATE (:Doc {key:'one', path:'guide'})")
	run(manager, "nornic", "CREATE (:Counter {id:1, n:0})")
	_ = manager.Close()
	_ = base.Close()
	base, manager = open()
	t.Cleanup(func() { _ = manager.Close(); _ = base.Close() })
	readerStore, err := manager.GetStorage("tenant")
	require.NoError(t, err)
	writerStore, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	reader := NewStorageExecutor(readerStore, QueryCachePolicy{MaxEntries: 0})
	writer := NewStorageExecutor(writerStore, QueryCachePolicy{MaxEntries: 0})
	next := 0
	statement := func(count int) string {
		items := make([]string, count)
		for i := range items {
			items[i] = fmt.Sprintf("n.absent_name_%08d", next)
			next++
		}
		return "MATCH (n:Doc {key:'one'}) RETURN " + strings.Join(items, ", ")
	}
	write := func(count int) (p50, worst time.Duration) {
		var took []time.Duration
		for i := 0; i < count; i++ {
			start := time.Now()
			_, err := writer.Execute(ctx, "MATCH (c:Counter) SET c.n = c.n + 1", nil)
			require.NoError(t, err)
			took = append(took, time.Since(start))
		}
		sort.Slice(took, func(i, j int) bool { return took[i] < took[j] })
		return took[len(took)/2], took[len(took)-1]
	}
	for _, target := range []int{2000, 50000, 150000, 300000} {
		for next < target {
			result, err := reader.Execute(ctx, statement(1000), nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
		}
		var one time.Duration
		for i := 0; i < 5; i++ {
			query := statement(1)
			start := time.Now()
			_, err := reader.Execute(ctx, query, nil)
			require.NoError(t, err)
			if took := time.Since(start); took > one {
				one = took
			}
		}
		stop := make(chan struct{})
		var readers sync.WaitGroup
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = reader.Execute(ctx, statement(1), nil)
				}
			}
		})
		p50, worst := write(60)
		close(stop)
		readers.Wait()
		idle50, idleWorst := write(60)
		t.Logf("R1RESTART names=%d one-more-name statement=%s; commit in the other database meanwhile p50=%s max=%s, idle p50=%s max=%s", next, one, p50, worst, idle50, idleWorst)
	}
}
