package bolt

import (
	"context"
	"strconv"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A ranked db.retrieve / db.rretrieve continuation over Bolt checks its where
// on every page. Each later page is its own RUN, after the RUN that started
// the search has ended: a pattern in the where must keep matching then, so no
// page returns a source the where excludes and none is lost.
func TestBoltRetrieveRetainedWhereOnLaterPages(t *testing.T) {
	const sources = 240
	store := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "nornic")
	server, port := startBoltIntegrationServer(t, store, boltIntegrationAutocommit)
	svc := search.NewService(store, 3, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	server.executor.(*cypherQueryExecutor).executor.SetSearchService(svc)
	conn := openBoltTestConn(t, port)
	runBoltQueryWithParams(t, conn, "CREATE (:Collection {archived: true}), (:Collection {archived: false})", nil)
	runBoltQueryWithParams(t, conn, `UNWIND range(0, $last) AS i
		MATCH (c:Collection {archived: i % 2 = 1})
		CREATE (:Src {id: 'src' + toString(i), content: 'walrus notes ' + toString(i)})-[:MEMBER_OF {hidden: i % 2 = 1}]->(c)`,
		map[string]any{"last": int64(sources - 1)})
	require.NoError(t, svc.BuildIndexes(t.Context()))

	const project = " YIELD page RETURN [r IN page.results | r.node.id] AS ids, page.qid AS qid, page.has_more AS more"
	for _, procedure := range []string{"db.retrieve", "db.rretrieve"} {
		for _, tc := range []struct {
			where    string
			archived bool
		}{
			{where: "NOT EXISTS { (node)-[:MEMBER_OF]->(:Collection {archived: $archived}) }"},
			{where: "EXISTS { (node)-[:MEMBER_OF]->(:Collection {archived: $archived}) }", archived: true},
			{where: "NOT EXISTS { (node)-[m:MEMBER_OF]->(:Collection) WHERE m.hidden = $archived }"},
		} {
			t.Run(procedure+"/"+tc.where, func(t *testing.T) {
				records := runBoltQueryWithParams(t, conn, "CALL "+procedure+"($request)"+project, map[string]any{
					"request":  map[string]any{"query": "walrus", "types": []any{"Src"}, "n": int64(10), "limit": int64(20), "where": tc.where},
					"archived": true,
				})
				seen := map[string]int{}
				for number := 1; ; number++ {
					require.Len(t, records, 1)
					ids, _ := records[0][0].([]any)
					if number == 1 {
						require.Len(t, ids, 10, "first page")
					}
					for _, value := range ids {
						id := value.(string)
						slot, err := strconv.Atoi(strings.TrimPrefix(id, "src"))
						require.NoError(t, err, id)
						require.Equal(t, tc.archived, slot%2 == 1, "page %d returned %s, which the where excludes", number, id)
						_, repeated := seen[id]
						require.False(t, repeated, "page %d repeated %s", number, id)
						seen[id] = number
					}
					if more, _ := records[0][2].(bool); !more {
						require.Greater(t, number, 3, "the fixture must page past the initial depth")
						break
					}
					require.Less(t, number, sources, "paging does not end")
					records = runBoltQueryWithParams(t, conn, "CALL "+procedure+"($request)"+project, map[string]any{
						"request": map[string]any{"qid": records[0][1], "n": int64(10)},
					})
				}
				require.Len(t, seen, sources/2, "every admitted source, on some page")
			})
		}
	}
}

// retainedWhereSessionExecutor is a per-connection Cypher executor with
// explicit transactions, sharing one search service between sessions as the
// database's search resolver does.
type retainedWhereSessionExecutor struct {
	store storage.Engine
	svc   *search.Service
	exec  *cypher.StorageExecutor
}

func (e *retainedWhereSessionExecutor) NewSessionExecutor() QueryExecutor {
	exec := cypher.NewStorageExecutor(e.store)
	exec.SetSearchService(e.svc)
	return &retainedWhereSessionExecutor{store: e.store, svc: e.svc, exec: exec}
}

func (e *retainedWhereSessionExecutor) Execute(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
	result, err := e.exec.Execute(ctx, query, params)
	if err != nil {
		return nil, err
	}
	return &QueryResult{Columns: result.Columns, Rows: result.Rows, Metadata: result.Metadata}, nil
}

func (e *retainedWhereSessionExecutor) BeginTransaction(ctx context.Context, _ map[string]any) error {
	_, err := e.exec.Execute(ctx, "BEGIN", nil)
	return err
}

func (e *retainedWhereSessionExecutor) CommitTransaction(ctx context.Context) error {
	_, err := e.exec.Execute(ctx, "COMMIT", nil)
	return err
}

func (e *retainedWhereSessionExecutor) RollbackTransaction(ctx context.Context) error {
	_, err := e.exec.Execute(ctx, "ROLLBACK", nil)
	return err
}

// A retrieval started in an explicit Bolt transaction pages on after that
// transaction commits: its later pages check the where in their own
// statements, auto-commit or each in a transaction of its own (a driver's
// managed read), never through the committed transaction's storage.
func TestBoltRetrieveRetainedWhereAcrossExplicitTransactions(t *testing.T) {
	const sources = 240
	store := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "nornic")
	setup := cypher.NewStorageExecutor(store)
	_, err := setup.Execute(t.Context(), "CREATE (:Collection {archived: true}), (:Collection {archived: false})", nil)
	require.NoError(t, err)
	_, err = setup.Execute(t.Context(), `UNWIND range(0, $last) AS i
		MATCH (c:Collection {archived: i % 2 = 1})
		CREATE (:Src {id: 'src' + toString(i), content: 'walrus notes ' + toString(i)})-[:MEMBER_OF]->(c)`,
		map[string]any{"last": int64(sources - 1)})
	require.NoError(t, err)
	svc := search.NewService(store, 3, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.BuildIndexes(t.Context()))
	server := New(&Config{Port: 0, MaxConnections: 10, ReadBufferSize: 8192, WriteBufferSize: 8192}, &retainedWhereSessionExecutor{store: store, svc: svc})
	port := serveBoltIntegrationServer(t, server)

	const project = " YIELD page RETURN [r IN page.results | r.node.id] AS ids, page.qid AS qid, page.has_more AS more"
	for _, route := range []string{"commit then auto-commit pages", "every page in a read transaction"} {
		for _, tc := range []struct {
			where    string
			archived bool
		}{
			{where: "NOT EXISTS { (node)-[:MEMBER_OF]->(:Collection {archived: $archived}) }"},
			{where: "EXISTS { (node)-[:MEMBER_OF]->(:Collection {archived: $archived}) }", archived: true},
		} {
			t.Run(route+"/"+tc.where, func(t *testing.T) {
				conn := openBoltTestConn(t, port)
				// The first route begins an ordinary (write) transaction;
				// a driver's managed read begins each one in read mode.
				var begin map[string]any
				if route == "every page in a read transaction" {
					begin = map[string]any{"mode": "r"}
				}
				inTransaction := func(run func() [][]any) [][]any {
					require.NoError(t, SendBegin(t, conn, begin))
					require.NoError(t, ReadSuccess(t, conn))
					records := run()
					require.NoError(t, SendCommit(t, conn))
					require.NoError(t, ReadSuccess(t, conn))
					return records
				}
				records := inTransaction(func() [][]any {
					return runBoltQueryWithParams(t, conn, "CALL db.retrieve($request)"+project, map[string]any{
						"request":  map[string]any{"query": "walrus", "types": []any{"Src"}, "n": int64(10), "limit": int64(20), "where": tc.where},
						"archived": true,
					})
				})
				seen := map[string]int{}
				for number := 1; ; number++ {
					require.Len(t, records, 1)
					ids, _ := records[0][0].([]any)
					for _, value := range ids {
						id := value.(string)
						slot, err := strconv.Atoi(strings.TrimPrefix(id, "src"))
						require.NoError(t, err, id)
						require.Equal(t, tc.archived, slot%2 == 1, "page %d returned %s, which the where excludes", number, id)
						_, repeated := seen[id]
						require.False(t, repeated, "page %d repeated %s", number, id)
						seen[id] = number
					}
					if more, _ := records[0][2].(bool); !more {
						require.Greater(t, number, 3, "the fixture must page past the initial depth")
						break
					}
					require.Less(t, number, sources, "paging does not end")
					pull := func() [][]any {
						return runBoltQueryWithParams(t, conn, "CALL db.retrieve($request)"+project, map[string]any{
							"request": map[string]any{"qid": records[0][1], "n": int64(10)},
						})
					}
					if route == "every page in a read transaction" {
						records = inTransaction(pull)
					} else {
						records = pull()
					}
				}
				require.Len(t, seen, sources/2, "every admitted source, on some page")
			})
		}
	}
}
