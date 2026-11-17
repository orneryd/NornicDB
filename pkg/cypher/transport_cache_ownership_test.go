package cypher

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/transportread"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The transport result-cache delivery: a first-party transport claims the result
// it reads once (internal/transportread), and the cache may then hand a certified
// entry over as a shell it owns around the entry's own rows. These tests hold the
// two contracts that make that safe: a Go caller always receives its own result,
// and a borrowed row is only ever read.

// transportCacheQuery is the cached read these tests repeat: a scalar and a list
// of scalars, the shapes a transport delivery may borrow.
const transportCacheQuery = "MATCH (n:TransportCache) RETURN n.name AS name, n.tags AS tags"

// transportCacheNodeQuery returns a known node image with inert properties.
const transportCacheNodeQuery = "MATCH (n:TransportCache) RETURN n"

// transportCacheExecutor builds one executor with a result cache over a store
// that holds one TransportCache node.
func transportCacheExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	t.Cleanup(ClearUserProcedures)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "transport"),
		QueryCachePolicy{MaxEntries: 64, TTL: time.Hour})
	_, err := exec.Execute(t.Context(), "CREATE (n:TransportCache {name: 'alpha', tags: ['a', 'b']})", nil)
	require.NoError(t, err)
	return exec
}

// transportHit executes query as a first-party transport does, and requires that
// the statement was served from the result cache.
func transportHit(t *testing.T, exec *StorageExecutor, query string) *ExecuteResult {
	t.Helper()
	before, _, _, _, _ := exec.cache.Stats()
	result, err := exec.Execute(transportread.Claim(t.Context()), query, nil)
	require.NoError(t, err)
	after, _, _, _, _ := exec.cache.Stats()
	require.Greater(t, after, before, "the claimed read must be served from the result cache")
	return result
}

// goCallerHit executes query as an embedded Go caller does, and requires that the
// statement was served from the result cache.
func goCallerHit(t *testing.T, exec *StorageExecutor, query string) *ExecuteResult {
	t.Helper()
	before, _, _, _, _ := exec.cache.Stats()
	result, err := exec.Execute(t.Context(), query, nil)
	require.NoError(t, err)
	after, _, _, _, _ := exec.cache.Stats()
	require.Greater(t, after, before, "the read must be served from the result cache")
	return result
}

// transportHitTags returns the list cell of a one-row transportCacheQuery result.
func transportHitTags(t *testing.T, result *ExecuteResult) []any {
	t.Helper()
	require.Len(t, result.Rows, 1)
	tags, ok := result.Rows[0][1].([]any)
	require.True(t, ok, "a list cell keeps its list type")
	return tags
}

// borrowedRowIdentity is one row value's identity: two deliveries of one
// certified entry share it, a copy does not.
func borrowedRowIdentity(value any) uintptr {
	return uintptr(reflect.ValueOf(value).UnsafePointer())
}

// TestTransportCacheHitBorrowsCertifiedRows covers the delivery a first-party
// transport receives: it owns the result it reads once, and it reads the cache's
// rows in place instead of receiving a copy of them.
func TestTransportCacheHitBorrowsCertifiedRows(t *testing.T) {
	exec := transportCacheExecutor(t)
	_, err := exec.Execute(t.Context(), transportCacheQuery, nil)
	require.NoError(t, err)

	first := transportHit(t, exec, transportCacheQuery)
	second := transportHit(t, exec, transportCacheQuery)

	require.NotSame(t, first, second, "each delivery is the caller's own result")
	require.Equal(t, []string{"name", "tags"}, second.Columns)
	require.Equal(t, "alpha", second.Rows[0][0])
	require.Equal(t, []any{"a", "b"}, transportHitTags(t, second))
	require.Equal(t, borrowedRowIdentity(transportHitTags(t, first)), borrowedRowIdentity(transportHitTags(t, second)),
		"a certified row is read in place, not copied for every delivery")

	// Everything the shell owns around the rows is the caller's: changing it
	// leaves the entry, and so the next delivery, as it was.
	first.Columns[0] = "changed"
	first.Metadata = nil
	first.Stats = nil
	first.Rows = nil

	third := transportHit(t, exec, transportCacheQuery)
	require.Equal(t, []string{"name", "tags"}, third.Columns)
	require.Equal(t, "alpha", third.Rows[0][0])
	require.Equal(t, []any{"a", "b"}, transportHitTags(t, third))
}

// TestExecuteGoCallerHitStaysOwned covers the embedded caller's contract through
// the public Execute: its hit is its own copy, whichever transport delivery the
// same statement also uses.
func TestExecuteGoCallerHitStaysOwned(t *testing.T) {
	exec := transportCacheExecutor(t)
	_, err := exec.Execute(t.Context(), transportCacheQuery, nil)
	require.NoError(t, err)

	first := goCallerHit(t, exec, transportCacheQuery)
	second := goCallerHit(t, exec, transportCacheQuery)
	require.NotEqual(t, borrowedRowIdentity(transportHitTags(t, first)), borrowedRowIdentity(transportHitTags(t, second)),
		"a Go caller's hit is its own copy of the rows")

	transportHitTags(t, first)[0] = "changed by caller"
	third := goCallerHit(t, exec, transportCacheQuery)
	require.Equal(t, []any{"a", "b"}, transportHitTags(t, third))
}

// Known node images may be borrowed by serializers; Go consumers stay owned.
func TestTransportCacheHitBorrowsNodeImages(t *testing.T) {
	exec := transportCacheExecutor(t)
	_, err := exec.Execute(t.Context(), transportCacheNodeQuery, nil)
	require.NoError(t, err)
	first := transportHit(t, exec, transportCacheNodeQuery)
	second := transportHit(t, exec, transportCacheNodeQuery)
	firstNode := first.Rows[0][0].(*storage.Node)
	require.Same(t, firstNode, second.Rows[0][0])
	owned := goCallerHit(t, exec, transportCacheNodeQuery)
	ownedNode := owned.Rows[0][0].(*storage.Node)
	require.NotSame(t, firstNode, ownedNode)
	ownedNode.Properties["name"] = "changed by Go caller"
	require.Equal(t, "alpha", firstNode.Properties["name"])
}

func TestTransportCacheHitRefusesCustomEntityProperties(t *testing.T) {
	cache := NewSmartQueryCache(4)
	node := &storage.Node{ID: "transport:node", Properties: map[string]any{"name": "alpha", "custom": transportMarshalProbe{value: "custom"}}}
	cache.putWithLabels("key", &ExecuteResult{Rows: [][]any{{node}}}, time.Hour, nil)
	first, _, _, found := cache.getWithTraceForEmbedding("key", 0, transportDelivery)
	require.True(t, found)
	first.Rows[0][0].(*storage.Node).Properties["name"] = "changed"
	second, _, _, found := cache.getWithTraceForEmbedding("key", 0, transportDelivery)
	require.True(t, found)
	require.NotSame(t, first.Rows[0][0], second.Rows[0][0])
	require.Equal(t, "alpha", second.Rows[0][0].(*storage.Node).Properties["name"])
}

// transportCopyProbe is a caller-declared value with the copy protocol storage
// asks before it walks a value: a row carrying one is not an inert image.
type transportCopyProbe struct{ value string }

func (probe transportCopyProbe) CallerCopy() any { return probe }

// transportMarshalProbe declares its own JSON form, so its content is produced by
// its own code rather than read as a value.
type transportMarshalProbe struct{ value string }

func (probe transportMarshalProbe) MarshalJSON() ([]byte, error) {
	return []byte(`"` + probe.value + `"`), nil
}

// TestTransportRowsInertCertifiesTheClosedValueSet covers the certification
// itself: the closed inert value shapes a transport reads in place are accepted,
// and every value whose content is not a plain value is refused.
func TestTransportRowsInertCertifiesTheClosedValueSet(t *testing.T) {
	cycle := map[string]any{}
	cycle["self"] = cycle

	for _, tc := range []struct {
		name  string
		row   []any
		inert bool
	}{
		{
			name:  "scalars and temporal values",
			row:   []any{nil, "text", true, int(1), int8(2), int16(3), int32(4), int64(5), uint(6), uint8(7), uint16(8), uint32(9), uint64(10), float32(11), float64(12), time.Unix(1, 0).UTC(), time.Second},
			inert: true,
		},
		{name: "list of scalars", row: []any{[]any{"a", int64(1)}}, inert: true},
		{name: "matrix of scalars", row: []any{[][]any{{"a"}, {int64(2)}}}, inert: true},
		{
			name:  "typed scalar lists",
			row:   []any{[]string{"a"}, []int{1}, []int32{2}, []int64{3}, []float32{4}, []float64{5}, []bool{true}, []byte{7}},
			inert: true,
		},
		{name: "string-keyed map", row: []any{map[string]any{"name": "alpha", "tags": []any{"a"}}}, inert: true},
		{name: "empty containers", row: []any{[]any{}, map[string]any{}, []string{}}, inert: true},
		{name: "returned node", row: []any{&storage.Node{ID: "transport:node"}}, inert: true},
		{name: "returned relationship", row: []any{&storage.Edge{ID: "transport:edge"}}, inert: true},
		{name: "list of maps", row: []any{[]map[string]any{{"name": "alpha"}}}},
		{name: "custom copy protocol", row: []any{transportCopyProbe{value: "probe"}}},
		{name: "custom copy protocol point", row: []any{&transportCopyProbe{value: "probe"}}},
		{name: "custom marshaler", row: []any{transportMarshalProbe{value: "probe"}}},
		{name: "cycle", row: []any{cycle}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.inert, transportRowsInert(&ExecuteResult{Rows: [][]any{tc.row}}))
		})
	}
}

// TestTransportDeliveryShellOwnsFieldsAndMapOrder covers what one certified
// delivery borrows and what it copies: the rows and the map-order identities are
// the entry's, and every field a reader changes is the shell's own.
func TestTransportDeliveryShellOwnsFieldsAndMapOrder(t *testing.T) {
	const query = "RETURN transport"
	cache := NewSmartQueryCache(8)
	row := map[string]any{"b": int64(2), "a": int64(1)}
	cache.PutWithLabels(query, nil, &ExecuteResult{
		Columns:        []string{"first", "second"},
		Rows:           [][]any{{row, "alpha"}},
		Stats:          &QueryStats{PropertiesSet: 3},
		Metadata:       map[string]any{"db": "transport", "nested": map[string]any{"values": []any{"original"}}},
		Administration: &AdministrationOutcome{CreatedDatabase: "transportdb"},
		MapKeyOrders:   map[uintptr][]string{borrowedRowIdentity(row): {"b", "a"}},
	}, time.Hour, []string{"Transport"})

	key := resultCacheEntryKey(context.Background(), query, nil)
	first, _, entities, found := cache.getWithTraceForEmbedding(key, 0, transportDelivery)
	require.True(t, found)
	require.False(t, entities, "a certified delivery returns no entity access to record")
	firstRow, ok := first.Rows[0][0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, []string{"b", "a"}, first.MapKeyOrders[borrowedRowIdentity(firstRow)],
		"an order is resolved by the map the transport reads")

	// The shell owns the fields around the rows.
	first.Columns[0] = "changed"
	first.Metadata["db"] = "changed"
	first.Metadata["nested"].(map[string]any)["values"].([]any)[0] = "changed"
	first.Stats.PropertiesSet = 99
	first.Administration.CreatedDatabase = "changed"

	second, _, _, found := cache.getWithTraceForEmbedding(key, 0, transportDelivery)
	require.True(t, found)
	require.Equal(t, []string{"first", "second"}, second.Columns)
	require.Equal(t, "transport", second.Metadata["db"])
	require.Equal(t, "original", second.Metadata["nested"].(map[string]any)["values"].([]any)[0])
	require.Equal(t, 3, second.Stats.PropertiesSet)
	require.Equal(t, "transportdb", second.Administration.CreatedDatabase)

	// It borrows the rows, and keeps their order identities.
	secondRow, ok := second.Rows[0][0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, borrowedRowIdentity(firstRow), borrowedRowIdentity(secondRow), "a certified row is borrowed")
	require.Equal(t, []string{"b", "a"}, second.MapKeyOrders[borrowedRowIdentity(secondRow)])

	// A Go caller's delivery of the same entry is its own copy, through the
	// internal delivery and through the public getter.
	owned, _, _, found := cache.getWithTraceForEmbedding(key, 0, ownedDelivery)
	require.True(t, found)
	ownedRow, ok := owned.Rows[0][0].(map[string]any)
	require.True(t, ok)
	require.NotEqual(t, borrowedRowIdentity(firstRow), borrowedRowIdentity(ownedRow))
	ownedRow["a"] = "changed"
	require.Equal(t, int64(1), firstRow["a"], "a Go caller's change never reaches the entry")

	public, found := cache.Get(query, nil)
	require.True(t, found)
	require.NotEqual(t, borrowedRowIdentity(firstRow), borrowedRowIdentity(public.Rows[0][0].(map[string]any)),
		"the public getter stays owned")
}

// TestTransportClaimDoesNotReachCustomCallbacks covers the boundary the claim
// stops at: the statement a custom callback runs on its own is a Go caller's
// statement, so it receives its own result even inside a transport's statement.
func TestTransportClaimDoesNotReachCustomCallbacks(t *testing.T) {
	exec := transportCacheExecutor(t)
	_, err := exec.Execute(t.Context(), transportCacheQuery, nil)
	require.NoError(t, err)

	var nested *ExecuteResult
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name:    "transport.NestedRead",
		Mode:    ProcedureModeRead,
		MaxArgs: 0,
		Returns: []ProcedureColumn{{Name: "v", Type: "STRING"}},
	}, func(ctx context.Context, procedureExec *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
		result, executeErr := procedureExec.Execute(ctx, transportCacheQuery, nil)
		if executeErr != nil {
			return nil, executeErr
		}
		nested = result
		// The callback is a Go caller: what it received is its own result, not
		// the rows the cache keeps.
		if tags, ok := result.Rows[0][1].([]any); ok && len(tags) > 0 {
			tags[0] = "changed by callback"
		}
		return &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{"callback"}}}, nil
	}))

	before, _, _, _, _ := exec.cache.Stats()
	result, err := exec.Execute(transportread.Claim(t.Context()), "CALL transport.NestedRead() YIELD v RETURN v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"callback"}}, result.Rows)
	after, _, _, _, _ := exec.cache.Stats()
	require.Greater(t, after, before, "the callback's read is served from the result cache")
	require.NotNil(t, nested)

	// The callback's change never reached the cached row: the next delivery
	// still reads the statement's result.
	require.Equal(t, []any{"a", "b"}, transportHitTags(t, transportHit(t, exec, transportCacheQuery)))
}

// TestTransportDeliverySurvivesConcurrentInvalidation covers a delivered row's
// lifetime: an entry may be invalidated while a transport reads what it delivered,
// and the rows it reads stay the ones it received.
func TestTransportDeliverySurvivesConcurrentInvalidation(t *testing.T) {
	exec := transportCacheExecutor(t)
	_, err := exec.Execute(t.Context(), transportCacheQuery, nil)
	require.NoError(t, err)

	const readers, rounds = 8, 25
	failures := make(chan string, readers*rounds)
	var wait sync.WaitGroup
	for reader := 0; reader < readers; reader++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for round := 0; round < rounds; round++ {
				result, executeErr := exec.Execute(transportread.Claim(context.Background()), transportCacheQuery, nil)
				if executeErr != nil {
					failures <- executeErr.Error()
					return
				}
				if len(result.Rows) != 1 || result.Rows[0][0] != "alpha" {
					failures <- "a delivered cell changed while the entry was invalidated"
					return
				}
				tags, ok := result.Rows[0][1].([]any)
				if !ok || len(tags) != 2 || tags[0] != "a" || tags[1] != "b" {
					failures <- "a delivered list changed while the entry was invalidated"
					return
				}
			}
		}()
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		for round := 0; round < rounds; round++ {
			exec.cache.Invalidate()
		}
	}()
	wait.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
}
