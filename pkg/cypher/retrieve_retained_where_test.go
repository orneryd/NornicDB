package cypher

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// retainedWhereSources is the size of the retained-where fixture: every
// second source is a member of an archived collection.
const retainedWhereSources = 240

// newRetainedWhereFixture is an executor over retainedWhereSources :Src
// nodes with the same text. Source i is a MEMBER_OF the archived collection
// (with hidden: true) when i is odd, and of the open one otherwise.
func newRetainedWhereFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(context.Background(), `CREATE (:Collection {name: 'archive', archived: true}), (:Collection {name: 'open', archived: false})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(context.Background(), `UNWIND range(0, $last) AS i
		MATCH (c:Collection {archived: i % 2 = 1})
		CREATE (:Src {id: 'src' + toString(i), slot: i, content: 'walrus notes ' + toString(i)})-[:MEMBER_OF {hidden: i % 2 = 1}]->(c)`,
		map[string]interface{}{"last": int64(retainedWhereSources - 1)})
	require.NoError(t, err)
	service := search.NewService(store, 3, nil)
	t.Cleanup(func() { _ = service.Close() })
	require.NoError(t, service.BuildIndexes(context.Background()))
	exec.SetSearchService(service)
	return exec
}

// retainedWhereCase is a where over the fixture and the sources it admits.
type retainedWhereCase struct {
	name  string
	where string
	// archived admits the sources of the archived collection (odd slots);
	// otherwise the where admits the open collection's (even slots).
	archived bool
}

var retainedWhereCases = []retainedWhereCase{
	{name: "not exists", where: "NOT EXISTS { (node)-[:MEMBER_OF]->(:Collection {archived: $archived}) }"},
	{name: "exists", where: "EXISTS { (node)-[:MEMBER_OF]->(:Collection {archived: $archived}) }", archived: true},
	{name: "relationship property", where: "NOT EXISTS { (node)-[m:MEMBER_OF]->(:Collection) WHERE m.hidden = $archived }"},
}

// retainedWherePage is one page of a paged retrieval: the source ids of its
// rows, the next page's qid and whether more rows remain.
type retainedWherePage struct {
	ids  []string
	qid  string
	more bool
}

func retainedWhereSlot(t *testing.T, id string) int {
	t.Helper()
	slot, err := strconv.Atoi(strings.TrimPrefix(id, "src"))
	require.NoError(t, err, id)
	return slot
}

// requireRetainedWherePages pages a ranked retrieval through to its end and
// requires every page to apply the where as the first one does: each row is
// admitted, no admitted source is lost or repeated, and the first page is a
// full page with more to come.
func requireRetainedWherePages(t *testing.T, tc retainedWhereCase, first retainedWherePage, next func(qid string) retainedWherePage) {
	t.Helper()
	require.Len(t, first.ids, 10, "first page")
	require.True(t, first.more, "first page")
	seen := map[string]int{}
	page, number := first, 1
	for {
		for _, id := range page.ids {
			require.Equal(t, tc.archived, retainedWhereSlot(t, id)%2 == 1, "page %d returned %s, which the where excludes", number, id)
			_, repeated := seen[id]
			require.False(t, repeated, "page %d repeated %s", number, id)
			seen[id] = number
		}
		if !page.more {
			break
		}
		require.Less(t, number, retainedWhereSources, "paging does not end")
		page, number = next(page.qid), number+1
	}
	require.Len(t, seen, retainedWhereSources/2, "every admitted source, on some page")
	require.Greater(t, number, 3, "the fixture must page past the initial depth")
}

func retainedWherePageFromMap(t *testing.T, value interface{}) retainedWherePage {
	t.Helper()
	page, ok := value.(map[string]interface{})
	require.True(t, ok, "page %T", value)
	var out retainedWherePage
	results, _ := page["results"].([]interface{})
	for _, item := range results {
		node, ok := item.(map[string]interface{})["node"].(*storage.Node)
		require.True(t, ok, "result node %T", item.(map[string]interface{})["node"])
		out.ids = append(out.ids, node.Properties["id"].(string))
	}
	out.qid, _ = page["qid"].(string)
	out.more, _ = page["has_more"].(bool)
	return out
}

// A ranked db.retrieve / db.rretrieve continuation checks its where on every
// page. Its later pages are statements of their own, which run after the
// statement that started the search has ended and its request context is
// cancelled, as Bolt and HTTP cancel a request's context when it completes. A
// pattern in the where must keep matching then: EXISTS must not read false,
// nor NOT EXISTS true, because the starting request has gone.
func TestRetrieveRetainedWhereAfterStartingStatementEnds(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newRetainedWhereFixture(t)
		// statement runs one statement in a request context of its own,
		// cancelled when the statement returns.
		statement := func(query string, params map[string]interface{}) *ExecuteResult {
			t.Helper()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result, err := exec.Execute(ctx, query, params)
			require.NoError(t, err, query)
			return result
		}
		for _, procedure := range []string{"db.retrieve", "db.rretrieve"} {
			for _, tc := range retainedWhereCases {
				t.Run(procedure+"/"+tc.name, func(t *testing.T) {
					start := statement("CALL "+procedure+"({query: 'walrus', types: ['Src'], n: 10, limit: 20, where: $where}) YIELD page RETURN page",
						map[string]interface{}{"where": tc.where, "archived": true})
					requireRetainedWherePages(t, tc, retainedWherePageFromMap(t, start.Rows[0][0]), func(qid string) retainedWherePage {
						result := statement("CALL "+procedure+"({qid: $qid, n: 10}) YIELD page RETURN page", map[string]interface{}{"qid": qid})
						return retainedWherePageFromMap(t, result.Rows[0][0])
					})
				})
			}
		}
		// The calling row's variables are the where's on later pages too.
		t.Run("calling row", func(t *testing.T) {
			start := statement(`WITH true AS archived
				CALL db.retrieve({query: 'walrus', types: ['Src'], n: 10, limit: 20, where: 'NOT EXISTS { (node)-[:MEMBER_OF]->(:Collection {archived: archived}) }'}) YIELD page
				RETURN page`, nil)
			requireRetainedWherePages(t, retainedWhereCases[0], retainedWherePageFromMap(t, start.Rows[0][0]), func(qid string) retainedWherePage {
				result := statement("CALL db.retrieve({qid: $qid, n: 10}) YIELD page RETURN page", map[string]interface{}{"qid": qid})
				return retainedWherePageFromMap(t, result.Rows[0][0])
			})
		})
	})
}

// retainedWhereCancellingEngine cancels a statement's context from inside its
// own traversal: the at-th relationship read after arm cancels it.
type retainedWhereCancellingEngine struct {
	storage.Engine
	mu     sync.Mutex
	cancel context.CancelFunc
	reads  int
	at     int
}

func (e *retainedWhereCancellingEngine) arm(cancel context.CancelFunc, at int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cancel, e.reads, e.at = cancel, 0, at
}

func (e *retainedWhereCancellingEngine) armedReads() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.reads
}

func (e *retainedWhereCancellingEngine) read() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.cancel == nil {
		return
	}
	e.reads++
	if e.reads == e.at {
		e.cancel()
		e.cancel = nil
	}
}

func (e *retainedWhereCancellingEngine) GetOutgoingEdges(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	e.read()
	return e.Engine.GetOutgoingEdges(ctx, id)
}

func (e *retainedWhereCancellingEngine) GetIncomingEdges(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	e.read()
	return e.Engine.GetIncomingEdges(ctx, id)
}

// Cancelling a continuation page while its where traverses stops the page with
// the cancellation, not with rows the where checked only in part; the stream
// then gives that page again, with the where intact, to a fresh request.
func TestRetrieveRetainedWhereCancelledPageFails(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
		setup := NewStorageExecutor(store)
		_, err := setup.Execute(context.Background(), `CREATE (:Collection {name: 'archive', archived: true}), (:Collection {name: 'open', archived: false})`, nil)
		require.NoError(t, err)
		_, err = setup.Execute(context.Background(), `UNWIND range(0, $last) AS i
			MATCH (c:Collection {archived: i % 2 = 1})
			CREATE (:Src {id: 'src' + toString(i), slot: i, content: 'walrus notes ' + toString(i)})-[:MEMBER_OF {hidden: i % 2 = 1}]->(c)`,
			map[string]interface{}{"last": int64(retainedWhereSources - 1)})
		require.NoError(t, err)
		// The retrievals read through the cancelling engine; they write nothing.
		engine := &retainedWhereCancellingEngine{Engine: store}
		exec := NewStorageExecutor(engine)
		service := search.NewService(store, 3, nil)
		t.Cleanup(func() { _ = service.Close() })
		require.NoError(t, service.BuildIndexes(context.Background()))
		exec.SetSearchService(service)

		seen := map[string]bool{}
		admit := func(page retainedWherePage) {
			t.Helper()
			for _, id := range page.ids {
				require.Equal(t, 0, retainedWhereSlot(t, id)%2, "returned %s, which the where excludes", id)
				require.False(t, seen[id], "repeated %s", id)
				seen[id] = true
			}
		}
		pull := func(ctx context.Context, qid string) (retainedWherePage, error) {
			result, err := exec.Execute(ctx, "CALL db.retrieve({qid: $qid, n: 10}) YIELD page RETURN page", map[string]interface{}{"qid": qid})
			if err != nil {
				return retainedWherePage{}, err
			}
			return retainedWherePageFromMap(t, result.Rows[0][0]), nil
		}
		fresh := func(qid string) retainedWherePage {
			t.Helper()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			page, err := pull(ctx, qid)
			require.NoError(t, err)
			return page
		}

		startCtx, endStart := context.WithCancel(context.Background())
		start, err := exec.Execute(startCtx, "CALL db.retrieve({query: 'walrus', types: ['Src'], n: 10, limit: 20, where: $where}) YIELD page RETURN page",
			map[string]interface{}{"where": retainedWhereCases[0].where, "archived": true})
		endStart()
		require.NoError(t, err)
		page := retainedWherePageFromMap(t, start.Rows[0][0])
		admit(page)
		// The initial depth is read, so the next pull expands the search.
		page = fresh(page.qid)
		admit(page)
		require.True(t, page.more)

		pullCtx, cancelPull := context.WithCancel(context.Background())
		defer cancelPull()
		engine.arm(cancelPull, 3)
		cancelled, err := pull(pullCtx, page.qid)
		reads := engine.armedReads()
		engine.arm(nil, 0)
		require.GreaterOrEqual(t, reads, 3, "the pull must reach the where's traversal")
		if err == nil {
			admit(cancelled)
			t.Fatalf("a page cancelled inside its where's traversal returned %d rows", len(cancelled.ids))
		}
		require.ErrorIs(t, err, context.Canceled)

		for number := 0; ; number++ {
			require.Less(t, number, retainedWhereSources, "paging does not end")
			page = fresh(page.qid)
			admit(page)
			if !page.more {
				break
			}
		}
		require.Len(t, seen, retainedWhereSources/2, "every admitted source, on some page")
	})
}

// A relationship candidate's where reads its pattern under the statement that
// runs it, with the same answers as before: a retrieval of relationships checks
// it only within its own statement (it does not page), and EXISTS and NOT
// EXISTS from an endpoint keep their meaning.
func TestRetrieveRelationshipWherePatternsAfterRequestEnds(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
		exec := NewStorageExecutor(store)
		svc := search.NewService(store, 3, nil)
		t.Cleanup(func() { _ = svc.Close() })
		svc.EnableRelationshipSearch(nil)
		svc.StartRelationshipIndex()
		exec.SetSearchService(svc)
		_, err := exec.Execute(context.Background(), `CREATE (archive:Collection {archived: true}), (open:Collection {archived: false})
			WITH archive, open UNWIND range(0, 19) AS i
			CREATE (a:Doc {slot: i})-[:CITES {slot: i, reason: 'zephyrine'}]->(:Doc {slot: i + 100})
			WITH a, i, archive, open
			FOREACH (c IN CASE WHEN i % 2 = 1 THEN [archive] ELSE [open] END | CREATE (a)-[:MEMBER_OF]->(c))`, nil)
		require.NoError(t, err)
		require.NoError(t, svc.BuildIndexes(context.Background()))
		slots := func(where string) []int {
			t.Helper()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var result *ExecuteResult
			require.Eventually(t, func() bool {
				result, err = exec.Execute(ctx, "CALL db.retrieveRelationships({query: 'zephyrine', limit: 20, where: $where}) YIELD relationship RETURN relationship.slot", map[string]interface{}{"where": where})
				return err == nil && len(result.Rows) > 0
			}, 5*time.Second, 20*time.Millisecond)
			var got []int
			for _, row := range result.Rows {
				got = append(got, int(row[0].(int64)))
			}
			sort.Ints(got)
			return got
		}
		var odd, even []int
		for i := 0; i < 20; i++ {
			if i%2 == 1 {
				odd = append(odd, i)
			} else {
				even = append(even, i)
			}
		}
		require.Equal(t, odd, slots("EXISTS { (start)-[:MEMBER_OF]->(:Collection {archived: true}) }"))
		require.Equal(t, even, slots("NOT EXISTS { (start)-[:MEMBER_OF]->(:Collection {archived: true}) }"))
		_, err = exec.Execute(context.Background(), "CALL db.retrieveRelationships({query: 'zephyrine', qid: 'x', n: 1})", nil)
		require.Error(t, err, "relationship retrieval does not page")
	})
}

// A retrieval started in an explicit transaction pages on after COMMIT. Its
// later pages check the where through their own statement's execution, auto-
// commit or in a transaction of their own, never through the committed
// transaction's storage; and its first page, inside the transaction, reads
// that transaction's own writes.
func TestRetrieveRetainedWhereAcrossExplicitCommit(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range retainedWhereCases[:2] {
			for _, later := range []string{"auto-commit", "explicit transaction"} {
				t.Run(tc.name+"/"+later, func(t *testing.T) {
					exec := newRetainedWhereFixture(t)
					statement := func(query string, params map[string]interface{}) *ExecuteResult {
						t.Helper()
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						result, err := exec.Execute(ctx, query, params)
						require.NoError(t, err, query)
						return result
					}
					statement("BEGIN", nil)
					start := statement("CALL db.retrieve({query: 'walrus', types: ['Src'], n: 10, limit: 20, where: $where}) YIELD page RETURN page",
						map[string]interface{}{"where": tc.where, "archived": true})
					statement("COMMIT", nil)
					requireRetainedWherePages(t, tc, retainedWherePageFromMap(t, start.Rows[0][0]), func(qid string) retainedWherePage {
						if later == "explicit transaction" {
							statement("BEGIN", nil)
							defer statement("COMMIT", nil)
						}
						result := statement("CALL db.retrieve({qid: $qid, n: 10}) YIELD page RETURN page", map[string]interface{}{"qid": qid})
						return retainedWherePageFromMap(t, result.Rows[0][0])
					})
				})
			}
		}
		t.Run("first page reads its transaction", func(t *testing.T) {
			exec := newRetainedWhereFixture(t)
			_, err := exec.Execute(context.Background(), "BEGIN", nil)
			require.NoError(t, err)
			// Archive the open collection in the transaction: from inside
			// it, every source is in an archived collection.
			_, err = exec.Execute(context.Background(), "MATCH (c:Collection {archived: false}) SET c.archived = true", nil)
			require.NoError(t, err)
			start, err := exec.Execute(context.Background(), "CALL db.retrieve({query: 'walrus', types: ['Src'], n: 10, limit: 20, where: $where}) YIELD page RETURN page",
				map[string]interface{}{"where": retainedWhereCases[0].where, "archived": true})
			require.NoError(t, err)
			require.Empty(t, retainedWherePageFromMap(t, start.Rows[0][0]).ids, "the transaction archived every collection")
			_, err = exec.Execute(context.Background(), "ROLLBACK", nil)
			require.NoError(t, err)
		})
	})
}

// A where that fails on a later page fails that page with its own error. No
// fallback search (vector-only, full-text, another query chunk) replaces the
// failed search with a candidate set that leaves out the node the where could
// not decide.
func TestRetrieveRetainedWhereErrorIsNotReplacedByFallback(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
		exec := NewStorageExecutor(store)
		_, err := exec.Execute(t.Context(), "CREATE (:Gate {id: 'safe', divisor: 1}), (:Gate {id: 'changing', divisor: 1})", nil)
		require.NoError(t, err)
		// src59 has a vector but not the query's term: only the vector
		// search reaches it, the full-text fallback never does.
		for i := 0; i < 60; i++ {
			content := "walrus notes"
			if i == 59 {
				content = "unrelated topic"
			}
			_, err := store.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(fmt.Sprintf("src%02d", i)), Labels: []string{"Src"},
				Properties: map[string]interface{}{"id": fmt.Sprintf("src%02d", i), "content": content}, ChunkEmbeddings: [][]float32{{1, 0, 0}}})
			require.NoError(t, err)
		}
		_, err = exec.Execute(t.Context(), "MATCH (n:Src), (c:Gate {id: 'safe'}) WHERE n.id <> 'src59' CREATE (n)-[:MEMBER_OF]->(c)", nil)
		require.NoError(t, err)
		_, err = exec.Execute(t.Context(), "MATCH (n:Src {id: 'src59'}), (c:Gate {id: 'changing'}) CREATE (n)-[:MEMBER_OF]->(c)", nil)
		require.NoError(t, err)
		service := search.NewService(store, 3, nil)
		t.Cleanup(func() { _ = service.Close() })
		require.NoError(t, service.BuildIndexes(t.Context()))
		exec.SetSearchService(service)
		statement := func(query string, params map[string]interface{}) (*ExecuteResult, error) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			return exec.Execute(ctx, query, params)
		}

		const where = "EXISTS { (node)-[:MEMBER_OF]->(c:Gate) WHERE 1 / c.divisor > 0 }"
		result, err := statement("CALL db.retrieve({query: 'walrus', embedding: [1.0, 0.0, 0.0], types: ['Src'], n: 10, limit: 20, where: $where}) YIELD page RETURN page",
			map[string]interface{}{"where": where})
		require.NoError(t, err)
		page := retainedWherePageFromMap(t, result.Rows[0][0])
		require.True(t, page.more)
		result, err = statement("CALL db.retrieve({qid: $qid, n: 10}) YIELD page RETURN page", map[string]interface{}{"qid": page.qid})
		require.NoError(t, err)
		page = retainedWherePageFromMap(t, result.Rows[0][0])
		require.True(t, page.more)

		_, err = statement("MATCH (c:Gate {id: 'changing'}) SET c.divisor = 0", nil)
		require.NoError(t, err)
		_, predicateErr := statement("MATCH (node:Src {id: 'src59'}) WHERE "+where+" RETURN node.id", nil)
		require.ErrorContains(t, predicateErr, "/ by zero", "the fixture's where must fail on src59")

		for number := 3; ; number++ {
			require.Less(t, number, 12, "a later page must fail with the where's error")
			result, err = statement("CALL db.retrieve({qid: $qid, n: 10}) YIELD page RETURN page", map[string]interface{}{"qid": page.qid})
			if err != nil {
				break
			}
			metadata := result.Rows[0][0].(map[string]interface{})
			page = retainedWherePageFromMap(t, metadata)
			require.True(t, page.more, "page %d ended the search without the where's error (search_method %v, fallback %v)",
				number, metadata["search_method"], metadata["fallback_reason"])
		}
		require.ErrorContains(t, err, "/ by zero")
		require.Contains(t, statusText(err), "ArithmeticError")
	})
}

// A search no Cypher statement asks for (a page pulled through another
// adapter) evaluates a retained where through the compiling executor's view
// of committed storage: a where compiled in an explicit transaction keeps
// deciding candidates after COMMIT, through neither the committed
// transaction's storage nor its node lookup cache.
func TestRetainedWhereOutsideStatementReadsCommittedStorage(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newRetainedWhereFixture(t)
		ctx := context.Background()
		_, err := exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, "MATCH (c:Collection) RETURN count(c)", nil)
		require.NoError(t, err)
		require.NotNil(t, exec.txContext.storageWrapper)
		inTransaction := exec.cloneWithStorage(exec.txContext.storageWrapper)
		node, relationship := inTransaction.compileRetainedWhere(withParams(ctx, map[string]interface{}{"archived": true}), retainedWhereCases[0].where, false)
		require.Nil(t, relationship)
		_, err = exec.Execute(ctx, "COMMIT", nil)
		require.NoError(t, err)

		result, err := exec.Execute(ctx, "MATCH (n:Src) RETURN n", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, retainedWhereSources)
		for _, row := range result.Rows {
			candidate := row[0].(*storage.Node)
			admitted, err := node(ctx, candidate)
			require.NoError(t, err, candidate.ID)
			require.Equal(t, retainedWhereSlot(t, candidate.Properties["id"].(string))%2 == 0, admitted, candidate.ID)
		}
	})
}
