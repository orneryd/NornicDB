// apoc.search through the registered handlers.
//
// The handler is the one the Cypher adapter and the APOC plugin receive from
// apoc.GetInfo, so these tests reproduce and then pin the behaviour a statement
// call reaches: any-criteria matching, equality over every Cypher value shape, a
// list or map property instead of a panic, and the caller's own cancellation and
// deadline. Every backend here is a real apoc/storage implementation bound to a
// *Functions instance the registry published.
package search_test

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/apoc"
	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/orneryd/nornicdb/apoc/search"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
)

// handlerScanStorage returns one fixed, ordered node slice from a whole-graph
// read, counts the reads, and can act after a successful read, so a test
// controls what the caller context looks like once the scan starts. Every other
// storage method keeps its in-memory behaviour.
type handlerScanStorage struct {
	*apocstorage.InMemoryStorage
	nodes     []*apocstorage.Node
	readErr   error
	afterRead func()
	reads     int
}

func (s *handlerScanStorage) AllNodes(context.Context) ([]*apocstorage.Node, error) {
	s.reads++
	if s.readErr != nil {
		return nil, s.readErr
	}
	if s.afterRead != nil {
		s.afterRead()
	}
	return s.nodes, nil
}

// installHandlerScanStorage publishes a registry bound to the storage, so the
// registered handlers under test answer from it, and restores the registry the
// test found afterwards.
func installHandlerScanStorage(t *testing.T, storage apocstorage.Storage) {
	t.Helper()
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
	previous := apoc.GetStorage()
	require.NotNil(t, previous, "the default registry must be published first")
	require.NoError(t, apoc.SetStorage(storage))
	t.Cleanup(func() {
		require.NoError(t, apoc.SetStorage(previous))
	})
}

// apocHandlerTakesContext reports whether a handler's first parameter is the
// statement context the Cypher adapter binds.
func apocHandlerTakesContext(signature reflect.Type) bool {
	return signature != nil && signature.NumIn() > 0 &&
		signature.In(0) == reflect.TypeOf((*context.Context)(nil)).Elem()
}

// callRegisteredSearch invokes a registered apoc.search handler through the same
// checked binding the Cypher adapter uses, with the caller's context.
func callRegisteredSearch(t *testing.T, name string, ctx context.Context, wantErr error, args ...interface{}) interface{} {
	t.Helper()
	info, found := apoc.GetInfo(name)
	require.True(t, found, "%s must be registered", name)
	require.NotNil(t, info.Handler, name)
	require.True(t, apocHandlerTakesContext(info.Handler.Signature()),
		"%s takes the caller context first", name)
	result, err := info.Handler.CallWithPolicy(args, []reflect.Value{reflect.ValueOf(ctx)}, registry.CypherArguments)
	if wantErr != nil {
		require.ErrorIs(t, err, wantErr, name)
		require.Nil(t, result, name)
		return nil
	}
	require.NoError(t, err, name)
	return result
}

// matchedNodeIDs reports the IDs a handler returned, in the order it returned
// them, so a test pins membership and order together.
func matchedNodeIDs(t *testing.T, result interface{}) []int64 {
	t.Helper()
	nodes, ok := result.([]*search.NodeType)
	require.True(t, ok, "apoc.search returns nodes, got %T", result)
	ids := make([]int64, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.ID)
	}
	return ids
}

// TestRegisteredSearchAnyCriteriaMatchesEitherCriterion reproduces the parent
// report on the registered scalar path: multiSearchAny with the criteria
// {name: Alice, missing: x} selected nothing, because it answered with
// all-criteria semantics where the missing property matches nothing. Any-criteria
// matching selects the node that satisfies at least one criterion.
func TestRegisteredSearchAnyCriteriaMatchesEitherCriterion(t *testing.T) {
	backend := apocstorage.NewInMemoryStorage()
	_, err := backend.CreateNode(t.Context(), []string{"Person"}, map[string]interface{}{
		"name": "Alice",
		"tags": []interface{}{"one"},
	})
	require.NoError(t, err)
	_, err = backend.CreateNode(t.Context(), []string{"Person"}, map[string]interface{}{"name": "Bob"})
	require.NoError(t, err)
	installHandlerScanStorage(t, backend)
	ctx := context.Background()
	mixed := map[string]interface{}{"name": "Alice", "missing": "x"}

	nodes, ok := callRegisteredSearch(t, "apoc.search.multiSearchAny", ctx, nil, []string{"Person"}, mixed).([]*search.NodeType)
	require.True(t, ok)
	require.Len(t, nodes, 1, "multiSearchAny must select the node that satisfies one of the criteria")
	require.Equal(t, "Alice", nodes[0].Properties["name"])

	anyNodes, ok := callRegisteredSearch(t, "apoc.search.nodeAny", ctx, nil, "Person", mixed).([]*search.NodeType)
	require.True(t, ok)
	require.Len(t, anyNodes, 1)
	allNodes, ok := callRegisteredSearch(t, "apoc.search.nodeAll", ctx, nil, "Person", mixed).([]*search.NodeType)
	require.True(t, ok)
	require.Empty(t, allNodes, "nodeAll keeps all-criteria semantics")
}

// listAndMapPropertyNodes is the population the equality cases read: two nodes
// whose property value is a list, one whose value is a map, one list value under
// another label so the label filter stays visible, and one node with no matching
// property at all.
func listAndMapPropertyNodes() []*apocstorage.Node {
	return []*apocstorage.Node{
		{ID: 1, Labels: []string{"Person"}, Properties: map[string]interface{}{
			"tags": []interface{}{"a", "b"},
		}},
		{ID: 2, Labels: []string{"Person"}, Properties: map[string]interface{}{
			"meta": map[string]interface{}{"k": "v"},
		}},
		{ID: 3, Labels: []string{"Person"}, Properties: map[string]interface{}{
			"tags": []interface{}{"c"},
		}},
		{ID: 4, Labels: []string{"Other"}, Properties: map[string]interface{}{
			"tags": []interface{}{"a", "b"},
		}},
		{ID: 5, Labels: []string{"Person"}, Properties: map[string]interface{}{
			"name": "Alice",
		}},
	}
}

// TestRegisteredSearchComparesListAndMapValues reproduces the parent report and
// keeps the membership population intact: apoc.search.node compared a list or
// map property with Go's ==, which panics with "comparing uncomparable type
// []interface {}". Every helper that compares values must answer through the
// package's equality owner instead, and each case reaches the registered handler,
// so an unrecovered panic fails the test instead of escaping into a statement.
func TestRegisteredSearchComparesListAndMapValues(t *testing.T) {
	ctx := context.Background()
	for _, testCase := range []struct {
		name   string
		helper string
		args   []interface{}
		want   []int64
	}{
		{
			name:   "node: list property against an equal list",
			helper: "apoc.search.node",
			args:   []interface{}{"Person", "tags", []interface{}{"a", "b"}},
			want:   []int64{1},
		},
		{
			name:   "node: list property against a scalar value",
			helper: "apoc.search.node",
			args:   []interface{}{"Person", "tags", "a"},
			want:   []int64{},
		},
		{
			name:   "node: map property against an equal map",
			helper: "apoc.search.node",
			args:   []interface{}{"Person", "meta", map[string]interface{}{"k": "v"}},
			want:   []int64{2},
		},
		{
			name:   "node: map property against another map",
			helper: "apoc.search.node",
			args:   []interface{}{"Person", "meta", map[string]interface{}{"k": "w"}},
			want:   []int64{},
		},
		{
			name:   "nodeAll: list criterion",
			helper: "apoc.search.nodeAll",
			args:   []interface{}{"Person", map[string]interface{}{"tags": []interface{}{"a", "b"}}},
			want:   []int64{1},
		},
		{
			name:   "nodeAny: list or scalar criterion",
			helper: "apoc.search.nodeAny",
			args: []interface{}{"Person", map[string]interface{}{
				"tags": []interface{}{"c"}, "name": "Alice",
			}},
			want: []int64{3, 5},
		},
		{
			name:   "in: list property against scalar values",
			helper: "apoc.search.in",
			args:   []interface{}{"Person", "tags", []interface{}{"a"}},
			want:   []int64{},
		},
		{
			name:   "in: list property against an equal list value",
			helper: "apoc.search.in",
			args:   []interface{}{"Person", "tags", []interface{}{[]interface{}{"a", "b"}}},
			want:   []int64{1},
		},
		{
			name:   "in: map property against an equal map value",
			helper: "apoc.search.in",
			args:   []interface{}{"Person", "meta", []interface{}{map[string]interface{}{"k": "v"}}},
			want:   []int64{2},
		},
		{
			name:   "in: mixed list and map values",
			helper: "apoc.search.in",
			args: []interface{}{"Person", "tags", []interface{}{
				[]interface{}{"c"}, map[string]interface{}{"k": "v"}, "missing",
			}},
			want: []int64{3},
		},
		{
			name:   "notIn: list property against scalar values",
			helper: "apoc.search.notIn",
			args:   []interface{}{"Person", "tags", []interface{}{"a"}},
			want:   []int64{1, 3},
		},
		{
			name:   "notIn: map property against an equal map value",
			helper: "apoc.search.notIn",
			args:   []interface{}{"Person", "meta", []interface{}{map[string]interface{}{"k": "v"}}},
			want:   []int64{},
		},
		{
			name:   "notIn: map property against a scalar value",
			helper: "apoc.search.notIn",
			args:   []interface{}{"Person", "meta", []interface{}{"x"}},
			want:   []int64{2},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			installHandlerScanStorage(t, &handlerScanStorage{nodes: listAndMapPropertyNodes()})
			require.NotPanics(t, func() {
				result := callRegisteredSearch(t, testCase.helper, ctx, nil, testCase.args...)
				require.Equal(t, testCase.want, matchedNodeIDs(t, result))
			})
		})
	}
}

// TestRegisteredSearchMembershipMatchesScalarValues pins the population the
// membership and equality predicates already matched before the scan converged,
// so the list/map comparison above cannot pass by widening or narrowing scalar
// matching.
func TestRegisteredSearchMembershipMatchesScalarValues(t *testing.T) {
	nodes := []*apocstorage.Node{
		{ID: 1, Labels: []string{"Person"}, Properties: map[string]interface{}{"status": "active"}},
		{ID: 2, Labels: []string{"Person"}, Properties: map[string]interface{}{"status": "pending"}},
		{ID: 3, Labels: []string{"Person"}, Properties: map[string]interface{}{"status": "deleted"}},
		{ID: 4, Labels: []string{"Person"}, Properties: map[string]interface{}{"status": int64(7)}},
		{ID: 5, Labels: []string{"Person"}, Properties: map[string]interface{}{"status": nil}},
		{ID: 6, Labels: []string{"Person"}, Properties: map[string]interface{}{"other": "active"}},
		{ID: 7, Labels: []string{"Company"}, Properties: map[string]interface{}{"status": "active"}},
	}

	for _, testCase := range []struct {
		name   string
		helper string
		args   []interface{}
		want   []int64
	}{
		{name: "in: strings", helper: "apoc.search.in", args: []interface{}{"Person", "status", []interface{}{"active", "pending"}}, want: []int64{1, 2}},
		{name: "in: integer", helper: "apoc.search.in", args: []interface{}{"Person", "status", []interface{}{int64(7)}}, want: []int64{4}},
		{name: "in: null value", helper: "apoc.search.in", args: []interface{}{"Person", "status", []interface{}{nil}}, want: []int64{5}},
		{name: "in: no listed value matches", helper: "apoc.search.in", args: []interface{}{"Person", "status", []interface{}{"archived"}}, want: []int64{}},
		{name: "in: empty list", helper: "apoc.search.in", args: []interface{}{"Person", "status", []interface{}{}}, want: []int64{}},
		{name: "notIn: strings", helper: "apoc.search.notIn", args: []interface{}{"Person", "status", []interface{}{"active"}}, want: []int64{2, 3, 4, 5}},
		{name: "notIn: empty list keeps every node with the property", helper: "apoc.search.notIn", args: []interface{}{"Person", "status", []interface{}{}}, want: []int64{1, 2, 3, 4, 5}},
		{name: "notIn: null value excluded", helper: "apoc.search.notIn", args: []interface{}{"Person", "status", []interface{}{nil}}, want: []int64{1, 2, 3, 4}},
		{name: "node: scalar equality is strict by dynamic type", helper: "apoc.search.node", args: []interface{}{"Person", "status", int64(7)}, want: []int64{4}},
		{name: "node: the other scalar type does not match", helper: "apoc.search.node", args: []interface{}{"Person", "status", 7}, want: []int64{}},
		{name: "node: nil matches a null property", helper: "apoc.search.node", args: []interface{}{"Person", "status", nil}, want: []int64{5}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			installHandlerScanStorage(t, &handlerScanStorage{nodes: nodes})
			result := callRegisteredSearch(t, testCase.helper, context.Background(), nil, testCase.args...)
			require.Equal(t, testCase.want, matchedNodeIDs(t, result))
		})
	}
}

// TestRegisteredSearchFollowsCallerCancel hands the handlers a real CancelFunc,
// canceled before the call and again after the whole-graph read returned, and
// requires the documented empty result instead of a partial collection: the scan
// checks its caller while it runs, not only when the read starts.
func TestRegisteredSearchFollowsCallerCancel(t *testing.T) {
	for _, testCase := range []struct {
		helper string
		args   []interface{}
	}{
		{helper: "apoc.search.in", args: []interface{}{"Person", "tags", []interface{}{[]interface{}{"a", "b"}}}},
		{helper: "apoc.search.multiSearchAny", args: []interface{}{[]string{"Person", "Company"}, map[string]interface{}{"tags": []interface{}{"a", "b"}}}},
		{helper: "apoc.search.nodeReduced", args: []interface{}{"Person", map[string]interface{}{"tags": []interface{}{"a", "b"}}, 10}},
	} {
		helper, args := testCase.helper, testCase.args
		t.Run(helper+"/canceled before the read", func(t *testing.T) {
			installHandlerScanStorage(t, &handlerScanStorage{nodes: listAndMapPropertyNodes()})
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			result := callRegisteredSearch(t, helper, ctx, context.Canceled, args...)
			require.Nil(t, result)
			require.Nil(t, result)
			require.ErrorIs(t, ctx.Err(), context.Canceled)
		})

		t.Run(helper+"/canceled after the read", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			installHandlerScanStorage(t, &handlerScanStorage{
				nodes:     listAndMapPropertyNodes(),
				afterRead: cancel,
			})
			result := callRegisteredSearch(t, helper, ctx, context.Canceled, args...)
			require.Nil(t, result)
			require.Nil(t, result,
				"a scan whose caller stopped after the read must not publish matches")
			require.ErrorIs(t, ctx.Err(), context.Canceled)
		})
	}
}

// TestRegisteredSearchFollowsCallerDeadline hands the handlers a real deadline
// that expires while the scan runs and requires the empty result.
func TestRegisteredSearchFollowsCallerDeadline(t *testing.T) {
	for _, helper := range []string{"apoc.search.in", "apoc.search.notIn"} {
		t.Run(helper, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			installHandlerScanStorage(t, &handlerScanStorage{
				nodes: listAndMapPropertyNodes(),
				afterRead: func() {
					<-ctx.Done()
				},
			})
			result := callRegisteredSearch(t, helper, ctx, context.DeadlineExceeded, "Person", "tags", []interface{}{"a"})
			require.Nil(t, result)
			require.Nil(t, result)
			require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
		})
	}
}

// TestRegisteredSearchPropagatesStorageFailure pins the
// error contract: a failed storage read preserves its original error and
// publishes no result.
func TestRegisteredSearchPropagatesStorageFailure(t *testing.T) {
	installHandlerScanStorage(t, &handlerScanStorage{readErr: apocstorage.ErrNodeNotFound})
	for _, helper := range []string{"apoc.search.in", "apoc.search.notIn", "apoc.search.multiSearchAll"} {
		helper := helper
		t.Run(helper, func(t *testing.T) {
			var args []interface{}
			switch helper {
			case "apoc.search.multiSearchAll":
				args = []interface{}{[]string{"Person"}, map[string]interface{}{"status": "active"}}
			default:
				args = []interface{}{"Person", "tags", []interface{}{"a"}}
			}
			result := callRegisteredSearch(t, helper, context.Background(), apocstorage.ErrNodeNotFound, args...)
			require.Nil(t, result)
			require.Nil(t, result)
		})
	}
}

// cancelOnDoneContext is the registered handler's fixture for a caller that
// stops while the scan it started is still running: it hands the scan the
// caller's own Done channel and fires the caller's real CancelFunc the first
// time the scan asks for that channel, which is where the traversal caches it -
// after the whole-graph read returned and before the first record is compared.
// A statement call reaches the same traversal through the registered handler, so
// this is the earliest point of the run at which a public call can be stopped
// deterministically.
type cancelOnDoneContext struct {
	context.Context
	cancel context.CancelFunc
	asked  int
}

func (c *cancelOnDoneContext) Done() <-chan struct{} {
	c.asked++
	if c.asked == 1 {
		c.cancel()
	}
	return c.Context.Done()
}

// TestRegisteredSearchDoesNotPublishWhenItsTraversalIsCanceled stops the caller
// of the registered apoc.search.in handler with its own CancelFunc at the first
// observation the traversal makes, and requires the documented empty result
// instead of the match the scan would otherwise have published. The whole-graph
// read has already returned when the cancellation lands, so the guard under test
// is the scan's own observation of its caller, not the bulk read contract.
//
// Proof limit: a statement call caches its caller's Done channel once per
// traversal and reads Err at admission and before publishing, so a cancellation
// that lands between two records of the same block cannot be injected through a
// public call without adding an observation to production code. The in-block
// bound and the publication guard after a mid-block cancellation are therefore
// proven on the scan owner itself, by
// TestLabeledScanStopsWithinOneIntervalOfCancelInYield and
// TestLabeledScanDoesNotPublishAfterCancelInFinalBlock in
// search_convergence_test.go; this test proves that the registered path reaches
// that owner's own observation of its caller.
func TestRegisteredSearchDoesNotPublishWhenItsTraversalIsCanceled(t *testing.T) {
	backend := &handlerScanStorage{nodes: listAndMapPropertyNodes()}
	installHandlerScanStorage(t, backend)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopping := &cancelOnDoneContext{Context: ctx, cancel: cancel}

	result := callRegisteredSearch(t, "apoc.search.in", stopping, context.Canceled,
		"Person", "tags", []interface{}{[]interface{}{"a", "b"}})

	require.Equal(t, 1, stopping.asked,
		"the traversal asks its caller for the Done channel once and caches it")
	require.Equal(t, 1, backend.reads,
		"the whole-graph read returned before the cancellation landed")
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Nil(t, result)
	require.Nil(t, result,
		"a caller that stopped while its scan ran must not receive the match the scan had found")

	healthy := &handlerScanStorage{nodes: listAndMapPropertyNodes()}
	installHandlerScanStorage(t, healthy)
	require.Equal(t, []int64{1}, matchedNodeIDs(t, callRegisteredSearch(t, "apoc.search.in", context.Background(), nil,
		"Person", "tags", []interface{}{[]interface{}{"a", "b"}})),
		"the healthy control of the same call publishes the match the stopped caller must not receive")
}

// registeredSearchOwners pins the canonical Go helper behind every registered
// apoc.search name, resolved from the admitted handler's own runtime symbol. Two
// Cypher names may share one owner (suggest and autocomplete do), but a name that
// no declared helper answers cannot be claimed as wire-supported, and no name may
// answer from a forwarding duplicate.
var registeredSearchOwners = map[string]string{
	"apoc.search.autocomplete":   "(*Functions).Suggest",
	"apoc.search.contains":       "(*Functions).Contains",
	"apoc.search.didYouMean":     "(*Functions).DidYouMean",
	"apoc.search.dropIndex":      "DropIndex",
	"apoc.search.exists":         "(*Functions).Exists",
	"apoc.search.fullText":       "(*Functions).FullText",
	"apoc.search.fuzzy":          "(*Functions).Fuzzy",
	"apoc.search.highlight":      "Highlight",
	"apoc.search.in":             "(*Functions).In",
	"apoc.search.index":          "Index",
	"apoc.search.match":          "(*Functions).Match",
	"apoc.search.missing":        "(*Functions).Missing",
	"apoc.search.multiSearchAll": "(*Functions).MultiSearchAll",
	"apoc.search.multiSearchAny": "(*Functions).MultiSearchAny",
	"apoc.search.node":           "(*Functions).Node",
	"apoc.search.nodeAll":        "(*Functions).NodeAll",
	"apoc.search.nodeAny":        "(*Functions).NodeAny",
	"apoc.search.nodeReduced":    "(*Functions).NodeReduced",
	"apoc.search.notIn":          "(*Functions).NotIn",
	"apoc.search.notNull":        "(*Functions).NotNull",
	"apoc.search.null":           "(*Functions).Null",
	"apoc.search.parallel":       "(*Functions).Parallel",
	"apoc.search.prefix":         "(*Functions).Prefix",
	"apoc.search.range":          "(*Functions).Range",
	"apoc.search.regex":          "(*Functions).Regex",
	"apoc.search.reindex":        "Reindex",
	"apoc.search.score":          "(*Functions).Score",
	"apoc.search.suggest":        "(*Functions).Suggest",
	"apoc.search.suffix":         "(*Functions).Suffix",
}

// registeredSearchStorageFree are the registered apoc.search names whose helper
// reads no storage: an index placeholder or a text transformation. They take no
// caller context because they perform no read that could outlive the caller.
var registeredSearchStorageFree = map[string]bool{
	"apoc.search.highlight": true,
	"apoc.search.index":     true,
	"apoc.search.dropIndex": true,
	"apoc.search.reindex":   true,
}

// searchOwnerSymbol reduces an admitted handler's Go symbol to the helper the
// apoc/search package declares: a bound helper is a method value
// (apoc/search.(*Functions).Prefix-fm) and a free helper is a plain function
// (apoc/search.Highlight).
func searchOwnerSymbol(t *testing.T, handler *registry.Callable) string {
	t.Helper()
	symbol := handler.Symbol()
	const prefix = "/apoc/search."
	at := strings.LastIndex(symbol, prefix)
	require.GreaterOrEqual(t, at, 0, "handler symbol %q must name an apoc/search helper", symbol)
	return strings.TrimSuffix(symbol[at+len(prefix):], "-fm")
}

// TestRegisteredSearchNamesReachTheirCanonicalOwner checks the registered
// apoc.search population against the source it claims: every name resolves to a
// helper this package declares, suggest and autocomplete resolve to the same one,
// and every helper that reads storage takes the caller's context first.
func TestRegisteredSearchNamesReachTheirCanonicalOwner(t *testing.T) {
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))

	var registered []string
	for _, name := range apoc.List() {
		if strings.HasPrefix(name, "apoc.search.") {
			registered = append(registered, name)
		}
	}
	sort.Strings(registered)
	pinned := make([]string, 0, len(registeredSearchOwners))
	for name := range registeredSearchOwners {
		pinned = append(pinned, name)
	}
	sort.Strings(pinned)
	require.Equal(t, pinned, registered,
		"every registered apoc.search name must be pinned to the helper that answers it")

	for _, name := range registered {
		info, found := apoc.GetInfo(name)
		require.True(t, found, name)
		require.NotNil(t, info.Handler, name)
		require.Equal(t, registeredSearchOwners[name], searchOwnerSymbol(t, info.Handler), name)
		if registeredSearchStorageFree[name] {
			require.False(t, apocHandlerTakesContext(info.Handler.Signature()),
				"%s reads no storage, so it takes no caller context", name)
			continue
		}
		require.True(t, apocHandlerTakesContext(info.Handler.Signature()),
			"%s reads the whole graph, so it takes the caller's context first", name)
	}

	suggest, found := apoc.GetInfo("apoc.search.suggest")
	require.True(t, found)
	autocomplete, found := apoc.GetInfo("apoc.search.autocomplete")
	require.True(t, found)
	require.Equal(t, suggest.Handler.Symbol(), autocomplete.Handler.Symbol(),
		"both suggestion names are registered against the one canonical owner")
	require.True(t, suggest.Handler.Signature().IsVariadic(),
		"the canonical suggestion owner carries its optional limit, so autocomplete needs no wrapper")
}

// TestRegisteredSearchSuggestionNamesKeepTheirLimits pins the behaviour the two
// suggestion names keep: suggest passes the limit it was given, and autocomplete,
// which passes none, keeps its default of ten.
func TestRegisteredSearchSuggestionNamesKeepTheirLimits(t *testing.T) {
	nodes := make([]*apocstorage.Node, 0, 12)
	for index := 0; index < 12; index++ {
		nodes = append(nodes, &apocstorage.Node{
			ID:         int64(index + 1),
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": "Al" + strings.Repeat("i", index) + "ce"},
		})
	}
	installHandlerScanStorage(t, &handlerScanStorage{nodes: nodes})
	ctx := context.Background()

	limited := callRegisteredSearch(t, "apoc.search.suggest", ctx, nil, "Person", "name", "Al", int64(5))
	limitedSuggestions, ok := limited.([]string)
	require.True(t, ok)
	require.Len(t, limitedSuggestions, 5)

	unlimited := callRegisteredSearch(t, "apoc.search.suggest", ctx, nil, "Person", "name", "Al")
	defaultSuggestions, ok := unlimited.([]string)
	require.True(t, ok)
	require.Len(t, defaultSuggestions, 10, "a suggest call without a limit keeps the default")

	completed := callRegisteredSearch(t, "apoc.search.autocomplete", ctx, nil, "Person", "name", "Al")
	completions, ok := completed.([]string)
	require.True(t, ok)
	require.Equal(t, defaultSuggestions, completions,
		"autocomplete keeps the ten-suggestion default the retired Go wrapper used")
}
