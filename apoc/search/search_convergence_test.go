// The apoc.search labeled scan: one owner, one range-function traversal, caller
// lifetime.
//
// These tests reach the scan owner directly, so they can observe where it looks
// at its caller and what it publishes when the caller stops. The caller contexts
// are real: a CancelFunc from context.WithCancel and a deadline from
// context.WithTimeout, never a fabricated error value.
package search

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/apoc/coll"
	"github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
)

// probeStorage returns one fixed, ordered node slice from every whole-graph
// read, counts the reads, and follows the bulk read contract: a read whose
// caller has stopped returns that caller's error instead of a collection. It
// observes its caller through Done rather than Err, so a test can tell the
// scan's own observations of its caller apart from the read's, and can act after
// a successful read.
type probeStorage struct {
	*storage.InMemoryStorage
	nodes     []*storage.Node
	readErr   error
	afterRead func()
	reads     int
}

func (s *probeStorage) AllNodes(ctx context.Context) ([]*storage.Node, error) {
	s.reads++
	if s.readErr != nil {
		return nil, s.readErr
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if s.afterRead != nil {
		s.afterRead()
	}
	return s.nodes, nil
}

// installProbe binds one instance to the probe, so every helper call below
// answers from the nodes the test controls.
func installProbe(t *testing.T, probe *probeStorage) *Functions {
	t.Helper()
	return New(probe)
}

// labeledScanNodes returns size ordered Person nodes, each carrying k.
func labeledScanNodes(size int) []*storage.Node {
	return labeledNodes(size, func(index int) map[string]interface{} {
		return map[string]interface{}{"k": int64(index)}
	})
}

// labeledNodes returns size ordered Person nodes whose properties the caller
// decides.
func labeledNodes(size int, properties func(index int) map[string]interface{}) []*storage.Node {
	nodes := make([]*storage.Node, 0, size)
	for index := 0; index < size; index++ {
		nodes = append(nodes, &storage.Node{
			ID:         int64(index + 1),
			Labels:     []string{"Person"},
			Properties: properties(index),
		})
	}
	return nodes
}

// scanNodeIDs is the ID sequence a scan returned, in scan order.
func scanNodeIDs(nodes []*NodeType) []int64 {
	ids := make([]int64, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.ID)
	}
	return ids
}

// intervalNodes returns size ordered records whose label alternates: the scanned
// label on an even index, another label on an odd one. Half of the records a
// traversal examines therefore never match, so the interval checks measured
// below have to count unmatched records as well as matches.
func intervalNodes(size int) []*storage.Node {
	nodes := labeledNodes(size, func(index int) map[string]interface{} {
		return map[string]interface{}{"k": int64(index)}
	})
	for index, node := range nodes {
		if index%2 == 1 {
			node.Labels = []string{"Unmatched"}
		}
	}
	return nodes
}

// matchedBetween counts the records of intervalNodes in [from,to) that carry the
// scanned label.
func matchedBetween(from, to int) int {
	matched := 0
	for index := from; index < to; index++ {
		if index%2 == 0 {
			matched++
		}
	}
	return matched
}

// observingContext counts how often the scan observes its caller and can fire
// the caller's real CancelFunc at one chosen observation. Every call is
// delegated to the real context, so the scan sees the caller's own state. The
// observation that fires the cancellation returns what it read before the
// cancellation landed, which is what an observation racing a cancellation
// returns; every later observation reads the canceled context.
//
// The traversal caches its caller's Done channel once and reads that channel at
// every scanInterval boundary, so Err is only called at admission and before
// publication: the count below pins those two guards, while doneAsked pins the
// one cached channel observation instead of a channel lookup per boundary (the
// probe asks for the channel once per read too).
type observingContext struct {
	context.Context
	cancel    context.CancelFunc
	fireAt    int
	firedAt   int
	observed  int
	doneAsked int
}

func (c *observingContext) Err() error {
	err := c.Context.Err()
	c.observed++
	if c.fireAt > 0 && c.firedAt == 0 && c.observed == c.fireAt {
		c.firedAt = c.observed
		c.cancel()
	}
	return err
}

func (c *observingContext) Done() <-chan struct{} {
	c.doneAsked++
	return c.Context.Done()
}

// TestLabeledScanKeepsScanOrderAndDocumentedResults pins the results every
// helper depends on: matches come back in the order the read returned them, a
// scan without matches returns an empty non-nil slice, and a failed read returns
// the same empty slice rather than a partial collection.
func TestLabeledScanKeepsScanOrderAndDocumentedResults(t *testing.T) {
	functions := installProbe(t, &probeStorage{nodes: []*storage.Node{
		{ID: 4, Labels: []string{"Person"}},
		{ID: 9, Labels: []string{"Company", "Person"}},
		{ID: 2, Labels: []string{"Person"}},
		{ID: 7, Labels: []string{"Company"}},
		{ID: 5, Labels: []string{"Company"}},
		{ID: 1, Labels: []string{"Person"}},
	}})

	t.Run("order and label filter", func(t *testing.T) {
		require.Equal(t, []int64{4, 9, 2, 1}, scanNodeIDs(functions.Missing(context.Background(), "Person", "unset")))
	})

	t.Run("no label match returns an empty non-nil slice", func(t *testing.T) {
		results := functions.Missing(context.Background(), "Missing", "unset")
		require.NotNil(t, results)
		require.Empty(t, results)
	})

	t.Run("no operation match returns an empty non-nil slice", func(t *testing.T) {
		results := functions.Exists(context.Background(), "Person", "unset")
		require.NotNil(t, results)
		require.Empty(t, results)
	})

	t.Run("failed read returns an empty non-nil slice", func(t *testing.T) {
		failed := installProbe(t, &probeStorage{readErr: storage.ErrNodeNotFound})
		results := failed.Missing(context.Background(), "Person", "unset")
		require.NotNil(t, results)
		require.Empty(t, results)
	})

	t.Run("empty store returns an empty non-nil slice", func(t *testing.T) {
		empty := installProbe(t, &probeStorage{nodes: nil})
		results := empty.Missing(context.Background(), "Person", "unset")
		require.NotNil(t, results)
		require.Empty(t, results)
	})
}

// TestLabeledScanObservesItsCallerAtAdmissionAndBeforePublishing pins where the
// scan looks at its caller: once before it reads, and once before it publishes,
// with the caller's Done channel cached once for the whole traversal and read at
// every scanInterval boundary. A caller that has already stopped reads no
// storage; a caller that stops after the read publishes nothing.
func TestLabeledScanObservesItsCallerAtAdmissionAndBeforePublishing(t *testing.T) {
	const population = 1024
	probe := &probeStorage{nodes: labeledScanNodes(population)}
	functions := installProbe(t, probe)

	t.Run("live caller is observed at admission and before publishing", func(t *testing.T) {
		probe.reads = 0
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		observing := &observingContext{Context: ctx}
		results := functions.Missing(observing, "Person", "unset")
		require.Equal(t, population, len(results))
		require.Equal(t, 1, probe.reads)
		require.Equal(t, 2, observing.observed,
			"the scan observes its caller once before the read and once before publishing")
		require.Equal(t, probe.reads+1, observing.doneAsked,
			"the traversal caches its caller's Done channel once instead of asking at every boundary")
	})

	t.Run("caller that stopped before the scan reads no storage", func(t *testing.T) {
		probe.reads = 0
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		observing := &observingContext{Context: ctx}
		results := functions.Missing(observing, "Person", "unset")
		require.Equal(t, 2, observing.observed,
			"admission and publication both observe the stopped caller; neither reads storage")
		require.Zero(t, probe.reads, "a stopped caller must not read storage")
		require.NotNil(t, results)
		require.Empty(t, results)
	})

	t.Run("caller that stops during the read publishes nothing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stopping := installProbe(t, &probeStorage{nodes: labeledScanNodes(population), afterRead: cancel})
		observing := &observingContext{Context: ctx}
		results := stopping.Missing(observing, "Person", "unset")
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		require.Equal(t, 2, observing.observed,
			"admission and publication both observe the caller; the cached Done channel stops the traversal")
		require.NotNil(t, results)
		require.Empty(t, results, "a stopped scan must not publish the matches it had collected")
	})
}

// TestLabeledScanStopsWithinOneIntervalOfCancelInYield is the deterministic
// cancellation test for the range-function traversal: the caller's own
// CancelFunc is fired from inside the yield of the third match, while the block
// that match belongs to is still being examined. The traversal may finish the
// block it has started and must then read the cached Done channel and stop, so
// at most one interval of records is examined after the cancellation - and that
// interval counts the records whose label does not match, because an interval
// that counted only matches would let the scan look bounded when it is not.
//
// The yields after the cancellation are what a caller can observe from outside.
// Because the population alternates between matching and non-matching records,
// receiving every remaining match of the block proves the traversal walked the
// non-matching records in between, and the examined positions follow from the
// block the cancellation landed in.
func TestLabeledScanStopsWithinOneIntervalOfCancelInYield(t *testing.T) {
	const population = 4 * scanInterval
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scan := installProbe(t, &probeStorage{nodes: intervalNodes(population)}).newResultScan(ctx, "Person")

	const cancelAtYield = 3
	yielded := 0
	canceledAt := -1
	matchedAfterCancel := 0
	for node := range scan.each() {
		if canceledAt >= 0 {
			matchedAfterCancel++
			continue
		}
		yielded++
		scan.add(node)
		if yielded == cancelAtYield {
			canceledAt = int(node.ID) - 1
			cancel()
		}
	}

	require.Equal(t, cancelAtYield, yielded,
		"the traversal yields matches in scan order until the caller is canceled")
	require.GreaterOrEqual(t, canceledAt, 0)
	require.ErrorIs(t, ctx.Err(), context.Canceled)

	blockEnd := min((canceledAt/scanInterval+1)*scanInterval, population)
	examinedAfterCancel := blockEnd - canceledAt - 1
	require.Equal(t, matchedBetween(canceledAt+1, blockEnd), matchedAfterCancel,
		"the traversal finishes the block it started, so every remaining match of that block is examined")
	require.NotZero(t, matchedAfterCancel, "the interrupted block has matches left for the bound to cross")
	require.LessOrEqual(t, examinedAfterCancel, scanInterval,
		"no more than one interval of records is examined after the cancellation")
	require.Len(t, scan.matches, cancelAtYield,
		"the scan had collected matches before the caller stopped")
	require.NotNil(t, scan.result())
	require.Empty(t, scan.result(),
		"a scan whose caller stopped inside a yield publishes nothing")
}

// TestLabeledScanPublishesEveryMatchForALiveCaller is the healthy control for
// the cancellation bound: the same alternating population, the same traversal
// and the same publication, with a caller that never stops.
func TestLabeledScanPublishesEveryMatchForALiveCaller(t *testing.T) {
	const population = 4 * scanInterval
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scan := installProbe(t, &probeStorage{nodes: intervalNodes(population)}).newResultScan(ctx, "Person")

	for node := range scan.each() {
		scan.add(node)
	}

	published := scan.result()
	require.Len(t, published, population/2)
	for index, node := range published {
		require.Equal(t, int64(2*index+1), node.ID, "matches are published in scan order")
	}
	require.NoError(t, ctx.Err())
}

// TestLabeledScanDoesNotPublishAfterCancelInFinalBlock is the regression for the
// publication guard a cancellation inside a block can reach: a ten-record scan
// has one block, which is also its final partial block, so a cancellation fired
// from inside a yield lands after the only interval check of the scan, with
// records still to be examined. The scan examines no more than one interval of
// them, keeps what it had collected, and publishes nothing.
func TestLabeledScanDoesNotPublishAfterCancelInFinalBlock(t *testing.T) {
	const population = 10
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scan := installProbe(t, &probeStorage{nodes: labeledScanNodes(population)}).newResultScan(ctx, "Person")

	const cancelAtYield = 5
	yielded := 0
	canceledAt := -1
	examinedAfterCancel := 0
	for node := range scan.each() {
		if canceledAt >= 0 {
			examinedAfterCancel++
			continue
		}
		yielded++
		scan.add(node)
		if yielded == cancelAtYield {
			canceledAt = int(node.ID) - 1
			cancel()
		}
	}

	require.Equal(t, cancelAtYield, yielded)
	require.GreaterOrEqual(t, canceledAt, 0)
	require.Equal(t, population-canceledAt-1, examinedAfterCancel,
		"the final partial block is examined to its end and no further")
	require.LessOrEqual(t, examinedAfterCancel, scanInterval)
	require.Equal(t, cancelAtYield, len(scan.matches),
		"the scan had collected matches before the caller stopped in the final block")
	require.NotNil(t, scan.result())
	require.Empty(t, scan.result(),
		"a scan whose caller stopped in the final partial block must not publish its matches")
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

// TestLabeledScanPublishesTheFinalPartialBlockForALiveCaller is the healthy
// control for the guard above: the same ten-record scan publishes every match
// when its caller never stops.
func TestLabeledScanPublishesTheFinalPartialBlockForALiveCaller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scan := installProbe(t, &probeStorage{nodes: labeledScanNodes(10)}).newResultScan(ctx, "Person")

	for node := range scan.each() {
		scan.add(node)
	}

	require.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, scanNodeIDs(scan.result()))
}

// TestLabeledScanHonorsCallerDeadline stops the scan on a deadline that expires
// while the scan runs, using context.WithTimeout rather than a fabricated error.
func TestLabeledScanHonorsCallerDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	functions := installProbe(t, &probeStorage{
		nodes:     labeledScanNodes(1024),
		afterRead: func() { <-ctx.Done() },
	})

	results := functions.Missing(ctx, "Person", "unset")

	require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	require.NotNil(t, results)
	require.Empty(t, results)
}

// TestSelectLabelsStopsWhenTheCallerStops pins the multi-label owner: the labels
// are scanned in the order they were given, a caller that stops inside the first
// label's scan never reaches the second label, and nothing it had collected is
// published. Every label is one public all-criteria search of its own.
func TestSelectLabelsStopsWhenTheCallerStops(t *testing.T) {
	t.Run("healthy caller receives every label in order", func(t *testing.T) {
		probe := &probeStorage{nodes: []*storage.Node{
			{ID: 1, Labels: []string{"Person", "Company"}, Properties: map[string]interface{}{"status": "active"}},
			{ID: 2, Labels: []string{"Company"}, Properties: map[string]interface{}{"status": "active"}},
			{ID: 3, Labels: []string{"Person"}, Properties: map[string]interface{}{"status": "active"}},
		}}
		functions := installProbe(t, probe)
		results := functions.MultiSearchAll(context.Background(), []string{"Person", "Company"},
			map[string]interface{}{"status": "active"})
		require.Equal(t, []int64{1, 3, 1, 2}, scanNodeIDs(results),
			"each label is scanned in the order given, so a node carrying two labels appears once per label")
		require.Equal(t, 2, probe.reads, "one whole-graph read per label")
	})

	t.Run("caller that stops inside the first label never reaches the second", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		probe := &probeStorage{
			nodes: []*storage.Node{
				{ID: 1, Labels: []string{"Person", "Company"}, Properties: map[string]interface{}{"status": "active"}},
				{ID: 2, Labels: []string{"Company"}, Properties: map[string]interface{}{"status": "active"}},
			},
			afterRead: cancel,
		}
		functions := installProbe(t, probe)
		results := functions.MultiSearchAll(ctx, []string{"Person", "Company"},
			map[string]interface{}{"status": "active"})
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		require.Equal(t, 1, probe.reads, "the second label is not read after the caller stopped")
		require.NotNil(t, results)
		require.Empty(t, results, "a stopped multi-label search must not publish the labels it had collected")
	})

	t.Run("caller that stopped before the search reads no label", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		probe := &probeStorage{nodes: labeledScanNodes(4)}
		functions := installProbe(t, probe)
		results := functions.MultiSearchAll(ctx, []string{"Person", "Company"},
			map[string]interface{}{"status": "active"})
		require.Zero(t, probe.reads, "a stopped caller must not read storage")
		require.NotNil(t, results)
		require.Empty(t, results)
	})
}

// TestEqualValuesIsTheOneEqualityOwner checks the equality owner against
// coll.Contains for every value shape a property or an expected value can have,
// so the single-value predicates, the criteria predicates and the membership
// predicates cannot answer differently from the collection equality this package
// documents. A list or a map is compared by contents, two scalars compare with
// Go's own ==, and no comparison panics.
func TestEqualValuesIsTheOneEqualityOwner(t *testing.T) {
	pointer := 7
	sameValuePointer := 7
	listed := []interface{}{
		"active", "", true, false, nil,
		int64(7), int(7), int32(7), uint8(7),
		float64(1.5), float32(1.5), math.NaN(), math.Inf(1),
		[]interface{}{"a", "b"}, map[string]interface{}{"k": "v"},
		&pointer,
	}
	probes := append([]interface{}{
		"active", "pending", "", int64(7), int64(8), int(7), int32(7), uint8(7),
		float64(1.5), float32(1.5), float64(7), math.NaN(), math.Inf(1), 0,
		[]interface{}{"a", "b"}, []interface{}{"b", "a"}, []interface{}{"a"},
		map[string]interface{}{"k": "v"}, map[string]interface{}{"k": "w"},
		&sameValuePointer, (*int)(nil),
	}, listed...)

	for _, probe := range probes {
		require.Equal(t, coll.Contains(listed, probe),
			anyListed(listed, probe),
			"probe %#v must compare the way the collection equality owner does", probe)
	}
}

// anyListed reports whether any listed value equals the probe, using the
// package's equality owner.
func anyListed(values []interface{}, probe interface{}) bool {
	for _, value := range values {
		if equalValues(probe, value) {
			return true
		}
	}
	return false
}

// TestMembershipAgreesWithItsEqualityOwner checks both membership paths - one by
// one and through the table of listed scalars - against coll.Contains, so a
// membership helper can never answer differently from the equality the package
// owns.
func TestMembershipAgreesWithItsEqualityOwner(t *testing.T) {
	pointer := 7
	sameValuePointer := 7
	listed := []interface{}{
		"active", "", true, false, nil,
		int64(7), int(7), int32(7), uint8(7),
		float64(1.5), float32(1.5), math.NaN(), math.Inf(1),
		[]interface{}{"a", "b"}, map[string]interface{}{"k": "v"},
		&pointer,
	}
	probes := append([]interface{}{
		"active", "pending", "", int64(7), int64(8), int(7), int32(7), uint8(7),
		float64(1.5), float32(1.5), float64(7), math.NaN(), math.Inf(1), 0,
		[]interface{}{"a", "b"}, []interface{}{"b", "a"}, []interface{}{"a"},
		map[string]interface{}{"k": "v"}, map[string]interface{}{"k": "w"},
		&sameValuePointer, (*int)(nil),
	}, listed...)

	index := membershipIndex(listed)
	require.NotNil(t, index, "a list longer than the threshold carries the table of listed scalars")
	require.Nil(t, membershipIndex(listed[:memberIndexThreshold]), "a short list compares one by one")

	for _, path := range []struct {
		name  string
		index map[interface{}]struct{}
	}{
		{name: "one by one"},
		{name: "table of listed scalars", index: index},
	} {
		t.Run(path.name, func(t *testing.T) {
			for _, probe := range probes {
				require.Equal(t, coll.Contains(listed, probe), listedValue(path.index, listed, probe),
					"probe %#v must compare the way the equality owner does", probe)
			}
		})
	}
}

// TestSearchHelpersCompareEveryValueShape drives the registered-shaped helpers
// over a population whose properties are lists, maps, NaN, typed nil and
// differently typed numbers, so a list or map property can be searched instead of
// aborting the statement, and scalar equality stays strict by dynamic type.
func TestSearchHelpersCompareEveryValueShape(t *testing.T) {
	typedNil := (*int)(nil)
	functions := installProbe(t, &probeStorage{nodes: []*storage.Node{
		{ID: 1, Labels: []string{"Person"}, Properties: map[string]interface{}{
			"tags": []interface{}{"a", "b"},
		}},
		{ID: 2, Labels: []string{"Person"}, Properties: map[string]interface{}{
			"meta": map[string]interface{}{"k": "v"},
		}},
		{ID: 3, Labels: []string{"Person"}, Properties: map[string]interface{}{
			"tags": []interface{}{"c"},
		}},
		{ID: 4, Labels: []string{"Person"}, Properties: map[string]interface{}{
			"score": math.NaN(),
		}},
		{ID: 5, Labels: []string{"Person"}, Properties: map[string]interface{}{
			"ref": typedNil,
		}},
		{ID: 6, Labels: []string{"Person"}, Properties: map[string]interface{}{
			"n": int64(7),
		}},
		{ID: 7, Labels: []string{"Person"}, Properties: map[string]interface{}{
			"n": int(7),
		}},
		{ID: 8, Labels: []string{"Other"}, Properties: map[string]interface{}{
			"tags": []interface{}{"a", "b"},
		}},
	}})
	ctx := context.Background()

	for _, testCase := range []struct {
		name string
		call func() []*NodeType
		want []int64
	}{
		{name: "Node: list property against an equal list", call: func() []*NodeType {
			return functions.Node(ctx, "Person", "tags", []interface{}{"a", "b"})
		}, want: []int64{1}},
		{name: "Node: list property against another list", call: func() []*NodeType {
			return functions.Node(ctx, "Person", "tags", []interface{}{"b", "a"})
		}, want: []int64{}},
		{name: "Node: map property against an equal map", call: func() []*NodeType {
			return functions.Node(ctx, "Person", "meta", map[string]interface{}{"k": "v"})
		}, want: []int64{2}},
		{name: "Node: NaN equals nothing, itself included", call: func() []*NodeType {
			return functions.Node(ctx, "Person", "score", math.NaN())
		}, want: []int64{}},
		{name: "Node: typed nil property against typed nil", call: func() []*NodeType {
			return functions.Node(ctx, "Person", "ref", typedNil)
		}, want: []int64{5}},
		{name: "Node: typed nil property against nil", call: func() []*NodeType {
			return functions.Node(ctx, "Person", "ref", nil)
		}, want: []int64{}},
		{name: "Node: scalar equality stays strict by dynamic type", call: func() []*NodeType {
			return functions.Node(ctx, "Person", "n", int64(7))
		}, want: []int64{6}},
		{name: "Node: the other scalar type does not match", call: func() []*NodeType {
			return functions.Node(ctx, "Person", "n", int(7))
		}, want: []int64{7}},
		{name: "Node: a scalar property against a list value", call: func() []*NodeType {
			return functions.Node(ctx, "Person", "n", []interface{}{int64(7)})
		}, want: []int64{}},
		{name: "NodeAll: list and map criteria", call: func() []*NodeType {
			return functions.NodeAll(ctx, "Person", map[string]interface{}{"tags": []interface{}{"a", "b"}})
		}, want: []int64{1}},
		{name: "NodeAny: list or scalar criteria", call: func() []*NodeType {
			return functions.NodeAny(ctx, "Person", map[string]interface{}{
				"tags": []interface{}{"c"}, "n": int64(7),
			})
		}, want: []int64{3, 6}},
		{name: "In: list property against an equal list value", call: func() []*NodeType {
			return functions.In(ctx, "Person", "tags", []interface{}{[]interface{}{"a", "b"}})
		}, want: []int64{1}},
		{name: "In: list property against scalar values", call: func() []*NodeType {
			return functions.In(ctx, "Person", "tags", []interface{}{"a"})
		}, want: []int64{}},
		{name: "In: map property against an equal map value", call: func() []*NodeType {
			return functions.In(ctx, "Person", "meta", []interface{}{map[string]interface{}{"k": "v"}})
		}, want: []int64{2}},
		{name: "NotIn: list and map values", call: func() []*NodeType {
			return functions.NotIn(ctx, "Person", "tags", []interface{}{"a", []interface{}{"c"}})
		}, want: []int64{1}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				require.Equal(t, testCase.want, scanNodeIDs(testCase.call()))
			})
		})
	}
}

// TestMultiSearchAnyMatchesEitherCriterion is the regression for the parent
// report: multiSearchAny answered with nodeAll's semantics, so criteria
// {name: Alice, missing: x} selected nothing because the missing property
// matched nothing. Any-criteria matching selects the node that satisfies one of
// the criteria.
func TestMultiSearchAnyMatchesEitherCriterion(t *testing.T) {
	functions := installProbe(t, &probeStorage{nodes: []*storage.Node{
		{ID: 1, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}},
		{ID: 2, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob", "missing": "x"}},
		{ID: 3, Labels: []string{"Company"}, Properties: map[string]interface{}{"name": "Alice"}},
		{ID: 4, Labels: []string{"Company"}, Properties: map[string]interface{}{"name": "Alice", "missing": "x"}},
	}})
	ctx := context.Background()
	mixed := map[string]interface{}{"name": "Alice", "missing": "x"}

	require.Equal(t, []int64{1, 2}, scanNodeIDs(functions.NodeAny(ctx, "Person", mixed)))
	require.Equal(t, []int64{1, 2, 3, 4}, scanNodeIDs(functions.MultiSearchAny(ctx, []string{"Person", "Company"}, mixed)))
	require.Equal(t, []int64{4}, scanNodeIDs(functions.MultiSearchAll(ctx, []string{"Person", "Company"}, mixed)),
		"multiSearchAll keeps all-criteria semantics: only the node satisfying every criterion matches")
	require.Equal(t, []int64{1, 2}, scanNodeIDs(functions.MultiSearchAny(ctx, []string{"Person"}, mixed)))
	require.Equal(t, []int64{}, scanNodeIDs(functions.MultiSearchAny(ctx, []string{"Missing"}, mixed)))
	require.Equal(t, []int64{}, scanNodeIDs(functions.MultiSearchAny(ctx, []string{"Person", "Company"}, nil)),
		"no criteria matches nothing")
}

// TestSearchHelpersPreserveTheirPredicates pins the match and order of every
// helper that reads the labeled scan, so the typed request cannot change what a
// helper selects.
func TestSearchHelpersPreserveTheirPredicates(t *testing.T) {
	functions := installProbe(t, &probeStorage{nodes: []*storage.Node{
		{ID: 1, Labels: []string{"Person"}, Properties: map[string]interface{}{
			"name": "Alice", "status": "active", "age": int64(30),
			"email": "alice@example.com", "note": nil,
		}},
		{ID: 2, Labels: []string{"Person"}, Properties: map[string]interface{}{
			"name": "Alison", "status": "pending", "age": int64(45),
			"email": "alison@example.com",
		}},
		{ID: 3, Labels: []string{"Person"}, Properties: map[string]interface{}{
			"name": "Bob", "status": "deleted", "age": int64(70),
			"email": "bob@corp.example.org",
		}},
		{ID: 4, Labels: []string{"Person"}, Properties: map[string]interface{}{
			"name": "Charlie", "status": "active", "age": int64(17),
		}},
		{ID: 5, Labels: []string{"Company"}, Properties: map[string]interface{}{
			"name": "Alice Corp", "status": "active", "age": int64(10),
			"email": "hr@example.com",
		}},
		{ID: 6, Labels: []string{"Person"}, Properties: map[string]interface{}{
			"name": int64(42),
		}},
	}})
	ctx := context.Background()

	for _, testCase := range []struct {
		name string
		call func() []*NodeType
		want []int64
	}{
		{name: "Node", call: func() []*NodeType { return functions.Node(ctx, "Person", "status", "active") }, want: []int64{1, 4}},
		{name: "Node/property type differs", call: func() []*NodeType {
			return functions.Node(ctx, "Person", "age", int64(30))
		}, want: []int64{1}},
		{name: "NodeAll", call: func() []*NodeType {
			return functions.NodeAll(ctx, "Person", map[string]interface{}{"status": "active", "age": int64(30)})
		}, want: []int64{1}},
		{name: "NodeAll/empty criteria matches every labeled node", call: func() []*NodeType {
			return functions.NodeAll(ctx, "Person", map[string]interface{}{})
		}, want: []int64{1, 2, 3, 4, 6}},
		{name: "NodeAny", call: func() []*NodeType {
			return functions.NodeAny(ctx, "Person", map[string]interface{}{"name": "Bob", "status": "active"})
		}, want: []int64{1, 3, 4}},
		{name: "NodeAny/empty criteria matches nothing", call: func() []*NodeType {
			return functions.NodeAny(ctx, "Person", map[string]interface{}{})
		}, want: []int64{}},
		{name: "FullText", call: func() []*NodeType { return functions.FullText(ctx, "Person", "name", "Alice Bob") }, want: []int64{1, 3}},
		{name: "Fuzzy", call: func() []*NodeType { return functions.Fuzzy(ctx, "Person", "name", "Alise", 2) }, want: []int64{1, 2}},
		{name: "Regex", call: func() []*NodeType {
			return functions.Regex(ctx, "Person", "email", `.*@example\.com$`)
		}, want: []int64{1, 2}},
		{name: "Regex/invalid pattern", call: func() []*NodeType { return functions.Regex(ctx, "Person", "email", "[") }, want: []int64{}},
		{name: "Prefix", call: func() []*NodeType { return functions.Prefix(ctx, "Person", "name", "Al") }, want: []int64{1, 2}},
		{name: "Suffix", call: func() []*NodeType { return functions.Suffix(ctx, "Person", "email", ".com") }, want: []int64{1, 2}},
		{name: "Contains", call: func() []*NodeType { return functions.Contains(ctx, "Person", "name", "li") }, want: []int64{1, 2, 4}},
		{name: "Range/integer", call: func() []*NodeType {
			return functions.Range(ctx, "Person", "age", int64(18), int64(50))
		}, want: []int64{1, 2}},
		{name: "Range/string", call: func() []*NodeType {
			return functions.Range(ctx, "Person", "name", "Alice", "Alison")
		}, want: []int64{1, 2}},
		{name: "In", call: func() []*NodeType {
			return functions.In(ctx, "Person", "status", []interface{}{"active"})
		}, want: []int64{1, 4}},
		{name: "NotIn", call: func() []*NodeType {
			return functions.NotIn(ctx, "Person", "status", []interface{}{"active"})
		}, want: []int64{2, 3}},
		{name: "Exists", call: func() []*NodeType { return functions.Exists(ctx, "Person", "email") }, want: []int64{1, 2, 3}},
		{name: "Missing", call: func() []*NodeType { return functions.Missing(ctx, "Person", "email") }, want: []int64{4, 6}},
		{name: "Null", call: func() []*NodeType { return functions.Null(ctx, "Person", "note") }, want: []int64{1}},
		{name: "NotNull", call: func() []*NodeType { return functions.NotNull(ctx, "Person", "note") }, want: []int64{}},
		{name: "Match", call: func() []*NodeType { return functions.Match(ctx, "Person", "name", "Al*") }, want: []int64{1, 2}},
		{name: "Parallel", call: func() []*NodeType {
			return functions.Parallel(ctx, []string{"Person", "Company"}, "name", "Alice")
		}, want: []int64{1}},
		{name: "MultiSearchAll", call: func() []*NodeType {
			return functions.MultiSearchAll(ctx, []string{"Person", "Company"}, map[string]interface{}{"status": "active"})
		}, want: []int64{1, 4, 5}},
		{name: "MultiSearchAny", call: func() []*NodeType {
			return functions.MultiSearchAny(ctx, []string{"Person", "Company"}, map[string]interface{}{"status": "active"})
		}, want: []int64{1, 4, 5}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.want, scanNodeIDs(testCase.call()))
		})
	}
}

// TestSearchSuggestionsPreserveTheirPredicates pins the string helpers, which
// reach the scan through Prefix and Fuzzy, and the optional limit that makes
// Suggest the one owner of both suggestion names.
func TestSearchSuggestionsPreserveTheirPredicates(t *testing.T) {
	functions := installProbe(t, &probeStorage{nodes: []*storage.Node{
		{ID: 1, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}},
		{ID: 2, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alison"}},
		{ID: 3, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}},
	}})
	ctx := context.Background()

	require.Equal(t, []string{"Alice", "Alison"}, functions.Suggest(ctx, "Person", "name", "Al", 5))
	require.Equal(t, []string{"Alice"}, functions.Suggest(ctx, "Person", "name", "Al", 1))
	require.Equal(t, []string{}, functions.Suggest(ctx, "Person", "name", "Al", 0))
	require.Equal(t, []string{"Alice", "Alison"}, functions.Suggest(ctx, "Person", "name", "Al"),
		"the default limit selects every match of this population")
	require.Equal(t, []string{"Alice", "Alison"}, functions.DidYouMean(ctx, "Person", "name", "Alise"))
}

// TestSearchSuggestionsObserveTheirCaller pins the collection guard: a caller
// that has stopped receives the documented empty list, never the strings the
// collection had already gathered.
func TestSearchSuggestionsObserveTheirCaller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	functions := installProbe(t, &probeStorage{
		nodes: []*storage.Node{
			{ID: 1, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}},
			{ID: 2, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alison"}},
		},
	})
	live := functions.Suggest(ctx, "Person", "name", "Al", 5)
	require.Equal(t, []string{"Alice", "Alison"}, live)

	canceled, stop := context.WithCancel(context.Background())
	stop()
	require.Equal(t, []string{}, functions.Suggest(canceled, "Person", "name", "Al", 5))
	require.Equal(t, []string{}, functions.DidYouMean(canceled, "Person", "name", "Alise"))

	stopping := installProbe(t, &probeStorage{
		nodes: []*storage.Node{
			{ID: 1, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}},
			{ID: 2, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alison"}},
		},
		afterRead: cancel,
	})
	require.Equal(t, []string{}, stopping.Suggest(ctx, "Person", "name", "Al", 5),
		"a caller that stops during the read publishes no suggestion")
}

// TestSearchPostProcessingObserveTheirCaller pins the score and reduced-result
// helpers: both keep their documented results for a live caller and return the
// documented empty result, never a partial collection, for a caller that stopped.
func TestSearchPostProcessingObserveTheirCaller(t *testing.T) {
	nodes := []*storage.Node{
		{ID: 1, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}},
		{ID: 2, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alicia"}},
	}
	probe := &probeStorage{nodes: nodes}
	functions := installProbe(t, probe)
	ctx := context.Background()
	matched := functions.Prefix(ctx, "Person", "name", "Ali")
	require.Len(t, matched, 2)

	scored := functions.Score(ctx, matched, "name", "Alice")
	require.Len(t, scored, 2)
	require.Equal(t, 1.0, scored[0]["score"])

	reduced := functions.NodeReduced(ctx, "Person", map[string]interface{}{"name": "Alice"}, 1)
	require.Equal(t, []int64{1}, scanNodeIDs(reduced))
	require.Equal(t, []int64{1, 2}, scanNodeIDs(functions.NodeReduced(ctx, "Person", map[string]interface{}{}, 10)))

	canceled, stop := context.WithCancel(context.Background())
	stop()
	probe.reads = 0
	require.NotNil(t, functions.Score(canceled, matched, "name", "Alice"))
	require.Empty(t, functions.Score(canceled, matched, "name", "Alice"))
	require.NotNil(t, functions.NodeReduced(canceled, "Person", map[string]interface{}{}, 10))
	require.Empty(t, functions.NodeReduced(canceled, "Person", map[string]interface{}{}, 10))
	require.Zero(t, probe.reads, "a stopped caller reads no storage")

	expired, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-expired.Done()
	require.Empty(t, functions.Score(expired, matched, "name", "Alice"))
	require.Empty(t, functions.NodeReduced(expired, "Person", map[string]interface{}{}, 10))
}
