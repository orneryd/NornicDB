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
		apocCallValue1, apocCallError1 := functions.Missing(context.Background(), "Person", "unset")
		if apocCallError1 != nil {
			t.Fatalf("APOC helper failed: %v", apocCallError1)
		}
		require.Equal(t, []int64{4, 9, 2, 1}, scanNodeIDs(apocCallValue1))
	})

	t.Run("no label match returns an empty non-nil slice", func(t *testing.T) {
		results, apocCallError2 := functions.Missing(context.Background(), "Missing", "unset")
		if apocCallError2 != nil {
			t.Fatalf("APOC helper failed: %v", apocCallError2)
		}

		require.NotNil(t, results)
		require.Empty(t, results)
	})

	t.Run("no operation match returns an empty non-nil slice", func(t *testing.T) {
		results, apocCallError3 := functions.Exists(context.Background(), "Person", "unset")
		if apocCallError3 != nil {
			t.Fatalf("APOC helper failed: %v", apocCallError3)
		}

		require.NotNil(t, results)
		require.Empty(t, results)
	})

	t.Run("failed read propagates its original error", func(t *testing.T) {
		failed := installProbe(t, &probeStorage{readErr: storage.ErrNodeNotFound})
		results, apocCallError4 := failed.Missing(context.Background(), "Person", "unset")
		require.ErrorIs(t, apocCallError4, storage.ErrNodeNotFound)

		require.Nil(t, results)
		require.Empty(t, results)
	})

	t.Run("empty store returns an empty non-nil slice", func(t *testing.T) {
		empty := installProbe(t, &probeStorage{nodes: nil})
		results, apocCallError5 := empty.Missing(context.Background(), "Person", "unset")
		if apocCallError5 != nil {
			t.Fatalf("APOC helper failed: %v", apocCallError5)
		}

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
		results, apocCallError6 := functions.Missing(observing, "Person", "unset")
		if apocCallError6 != nil {
			t.Fatalf("APOC helper failed: %v", apocCallError6)
		}

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
		results, apocCallError7 := functions.Missing(observing, "Person", "unset")
		require.ErrorIs(t, apocCallError7, context.Canceled)

		require.Equal(t, 1, observing.observed,
			"a stopped caller is rejected at admission without storage or publication")
		require.Zero(t, probe.reads, "a stopped caller must not read storage")
		require.Nil(t, results)
		require.Empty(t, results)
	})

	t.Run("caller that stops during the read publishes nothing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stopping := installProbe(t, &probeStorage{nodes: labeledScanNodes(population), afterRead: cancel})
		observing := &observingContext{Context: ctx}
		results, apocCallError8 := stopping.Missing(observing, "Person", "unset")
		require.ErrorIs(t, apocCallError8, context.Canceled)

		require.ErrorIs(t, ctx.Err(), context.Canceled)
		require.Equal(t, 2, observing.observed,
			"admission and publication both observe the caller; the cached Done channel stops the traversal")
		require.Nil(t, results)
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
	scan, apocCallError9 := installProbe(t, &probeStorage{nodes: intervalNodes(population)}).newResultScan(ctx, "Person")
	if apocCallError9 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError9)
	}

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
	apocCallValue10, apocCallError10 := scan.result()
	require.ErrorIs(t, apocCallError10, context.Canceled)
	require.Nil(t, apocCallValue10)
	apocCallValue11, apocCallError11 := scan.result()
	require.ErrorIs(t, apocCallError11, context.Canceled)
	require.Empty(t, apocCallValue11,
		"a scan whose caller stopped inside a yield publishes nothing")
}

// TestLabeledScanPublishesEveryMatchForALiveCaller is the healthy control for
// the cancellation bound: the same alternating population, the same traversal
// and the same publication, with a caller that never stops.
func TestLabeledScanPublishesEveryMatchForALiveCaller(t *testing.T) {
	const population = 4 * scanInterval
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scan, apocCallError10 := installProbe(t, &probeStorage{nodes: intervalNodes(population)}).newResultScan(ctx, "Person")
	if apocCallError10 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError10)
	}

	for node := range scan.each() {
		scan.add(node)
	}

	published, apocCallError13 := scan.result()
	if apocCallError13 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError13)
	}

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
	scan, apocCallError11 := installProbe(t, &probeStorage{nodes: labeledScanNodes(population)}).newResultScan(ctx, "Person")
	if apocCallError11 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError11)
	}

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
	apocCallValue15, apocCallError15 := scan.result()
	require.ErrorIs(t, apocCallError15, context.Canceled)
	require.Nil(t, apocCallValue15)
	apocCallValue16, apocCallError16 := scan.result()
	require.ErrorIs(t, apocCallError16, context.Canceled)
	require.Empty(t, apocCallValue16,
		"a scan whose caller stopped in the final partial block must not publish its matches")
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

// TestLabeledScanPublishesTheFinalPartialBlockForALiveCaller is the healthy
// control for the guard above: the same ten-record scan publishes every match
// when its caller never stops.
func TestLabeledScanPublishesTheFinalPartialBlockForALiveCaller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scan, apocCallError12 := installProbe(t, &probeStorage{nodes: labeledScanNodes(10)}).newResultScan(ctx, "Person")
	if apocCallError12 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError12)
	}

	for node := range scan.each() {
		scan.add(node)
	}

	apocCallValue18, apocCallError18 := scan.result()
	if apocCallError18 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError18)
	}
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, scanNodeIDs(apocCallValue18))
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

	results, apocCallError9 := functions.Missing(ctx, "Person", "unset")
	require.ErrorIs(t, apocCallError9, context.DeadlineExceeded)

	require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	require.Nil(t, results)
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
		results, apocCallError10 := functions.MultiSearchAll(context.Background(), []string{"Person", "Company"},
			map[string]interface{}{"status": "active"})
		if apocCallError10 != nil {
			t.Fatalf("APOC helper failed: %v", apocCallError10)
		}

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
		results, apocCallError11 := functions.MultiSearchAll(ctx, []string{"Person", "Company"},
			map[string]interface{}{"status": "active"})
		require.ErrorIs(t, apocCallError11, context.Canceled)

		require.ErrorIs(t, ctx.Err(), context.Canceled)
		require.Equal(t, 1, probe.reads, "the second label is not read after the caller stopped")
		require.Nil(t, results)
		require.Empty(t, results, "a stopped multi-label search must not publish the labels it had collected")
	})

	t.Run("caller that stopped before the search reads no label", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		probe := &probeStorage{nodes: labeledScanNodes(4)}
		functions := installProbe(t, probe)
		results, apocCallError12 := functions.MultiSearchAll(ctx, []string{"Person", "Company"},
			map[string]interface{}{"status": "active"})
		require.ErrorIs(t, apocCallError12, context.Canceled)

		require.Zero(t, probe.reads, "a stopped caller must not read storage")
		require.Nil(t, results)
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
			apocCallValue13, apocCallError13 := functions.Node(ctx, "Person", "tags", []interface{}{"a", "b"})
			if apocCallError13 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError13)
			}
			return apocCallValue13
		}, want: []int64{1}},
		{name: "Node: list property against another list", call: func() []*NodeType {
			apocCallValue14, apocCallError14 := functions.Node(ctx, "Person", "tags", []interface{}{"b", "a"})
			if apocCallError14 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError14)
			}
			return apocCallValue14
		}, want: []int64{}},
		{name: "Node: map property against an equal map", call: func() []*NodeType {
			apocCallValue15, apocCallError15 := functions.Node(ctx, "Person", "meta", map[string]interface{}{"k": "v"})
			if apocCallError15 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError15)
			}
			return apocCallValue15
		}, want: []int64{2}},
		{name: "Node: NaN equals nothing, itself included", call: func() []*NodeType {
			apocCallValue16, apocCallError16 := functions.Node(ctx, "Person", "score", math.NaN())
			if apocCallError16 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError16)
			}
			return apocCallValue16
		}, want: []int64{}},
		{name: "Node: typed nil property against typed nil", call: func() []*NodeType {
			apocCallValue17, apocCallError17 := functions.Node(ctx, "Person", "ref", typedNil)
			if apocCallError17 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError17)
			}
			return apocCallValue17
		}, want: []int64{5}},
		{name: "Node: typed nil property against nil", call: func() []*NodeType {
			apocCallValue18, apocCallError18 := functions.Node(ctx, "Person", "ref", nil)
			if apocCallError18 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError18)
			}
			return apocCallValue18
		}, want: []int64{}},
		{name: "Node: scalar equality stays strict by dynamic type", call: func() []*NodeType {
			apocCallValue19, apocCallError19 := functions.Node(ctx, "Person", "n", int64(7))
			if apocCallError19 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError19)
			}
			return apocCallValue19
		}, want: []int64{6}},
		{name: "Node: the other scalar type does not match", call: func() []*NodeType {
			apocCallValue20, apocCallError20 := functions.Node(ctx, "Person", "n", int(7))
			if apocCallError20 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError20)
			}
			return apocCallValue20
		}, want: []int64{7}},
		{name: "Node: a scalar property against a list value", call: func() []*NodeType {
			apocCallValue21, apocCallError21 := functions.Node(ctx, "Person", "n", []interface{}{int64(7)})
			if apocCallError21 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError21)
			}
			return apocCallValue21
		}, want: []int64{}},
		{name: "NodeAll: list and map criteria", call: func() []*NodeType {
			apocCallValue22, apocCallError22 := functions.NodeAll(ctx, "Person", map[string]interface{}{"tags": []interface{}{"a", "b"}})
			if apocCallError22 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError22)
			}
			return apocCallValue22
		}, want: []int64{1}},
		{name: "NodeAny: list or scalar criteria", call: func() []*NodeType {
			apocCallValue23, apocCallError23 := functions.NodeAny(ctx, "Person", map[string]interface{}{
				"tags": []interface{}{"c"}, "n": int64(7),
			})
			if apocCallError23 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError23)
			}
			return apocCallValue23
		}, want: []int64{3, 6}},
		{name: "In: list property against an equal list value", call: func() []*NodeType {
			apocCallValue24, apocCallError24 := functions.In(ctx, "Person", "tags", []interface{}{[]interface{}{"a", "b"}})
			if apocCallError24 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError24)
			}
			return apocCallValue24
		}, want: []int64{1}},
		{name: "In: list property against scalar values", call: func() []*NodeType {
			apocCallValue25, apocCallError25 := functions.In(ctx, "Person", "tags", []interface{}{"a"})
			if apocCallError25 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError25)
			}
			return apocCallValue25
		}, want: []int64{}},
		{name: "In: map property against an equal map value", call: func() []*NodeType {
			apocCallValue26, apocCallError26 := functions.In(ctx, "Person", "meta", []interface{}{map[string]interface{}{"k": "v"}})
			if apocCallError26 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError26)
			}
			return apocCallValue26
		}, want: []int64{2}},
		{name: "NotIn: list and map values", call: func() []*NodeType {
			apocCallValue27, apocCallError27 := functions.NotIn(ctx, "Person", "tags", []interface{}{"a", []interface{}{"c"}})
			if apocCallError27 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError27)
			}
			return apocCallValue27
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

	apocCallValue28, apocCallError28 := functions.NodeAny(ctx, "Person", mixed)
	if apocCallError28 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError28)
	}
	require.Equal(t, []int64{1, 2}, scanNodeIDs(apocCallValue28))
	apocCallValue29, apocCallError29 := functions.MultiSearchAny(ctx, []string{"Person", "Company"}, mixed)
	if apocCallError29 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError29)
	}
	require.Equal(t, []int64{1, 2, 3, 4}, scanNodeIDs(apocCallValue29))
	apocCallValue30, apocCallError30 := functions.MultiSearchAll(ctx, []string{"Person", "Company"}, mixed)
	if apocCallError30 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError30)
	}
	require.Equal(t, []int64{4}, scanNodeIDs(apocCallValue30),
		"multiSearchAll keeps all-criteria semantics: only the node satisfying every criterion matches")
	apocCallValue31, apocCallError31 := functions.MultiSearchAny(ctx, []string{"Person"}, mixed)
	if apocCallError31 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError31)
	}
	require.Equal(t, []int64{1, 2}, scanNodeIDs(apocCallValue31))
	apocCallValue32, apocCallError32 := functions.MultiSearchAny(ctx, []string{"Missing"}, mixed)
	if apocCallError32 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError32)
	}
	require.Equal(t, []int64{}, scanNodeIDs(apocCallValue32))
	apocCallValue33, apocCallError33 := functions.MultiSearchAny(ctx, []string{"Person", "Company"}, nil)
	if apocCallError33 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError33)
	}
	require.Equal(t, []int64{}, scanNodeIDs(apocCallValue33),
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
		{name: "Node", call: func() []*NodeType {
			apocCallValue34, apocCallError34 := functions.Node(ctx, "Person", "status", "active")
			if apocCallError34 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError34)
			}
			return apocCallValue34
		}, want: []int64{1, 4}},
		{name: "Node/property type differs", call: func() []*NodeType {
			apocCallValue35, apocCallError35 := functions.Node(ctx, "Person", "age", int64(30))
			if apocCallError35 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError35)
			}
			return apocCallValue35
		}, want: []int64{1}},
		{name: "NodeAll", call: func() []*NodeType {
			apocCallValue36, apocCallError36 := functions.NodeAll(ctx, "Person", map[string]interface{}{"status": "active", "age": int64(30)})
			if apocCallError36 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError36)
			}
			return apocCallValue36
		}, want: []int64{1}},
		{name: "NodeAll/empty criteria matches every labeled node", call: func() []*NodeType {
			apocCallValue37, apocCallError37 := functions.NodeAll(ctx, "Person", map[string]interface{}{})
			if apocCallError37 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError37)
			}
			return apocCallValue37
		}, want: []int64{1, 2, 3, 4, 6}},
		{name: "NodeAny", call: func() []*NodeType {
			apocCallValue38, apocCallError38 := functions.NodeAny(ctx, "Person", map[string]interface{}{"name": "Bob", "status": "active"})
			if apocCallError38 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError38)
			}
			return apocCallValue38
		}, want: []int64{1, 3, 4}},
		{name: "NodeAny/empty criteria matches nothing", call: func() []*NodeType {
			apocCallValue39, apocCallError39 := functions.NodeAny(ctx, "Person", map[string]interface{}{})
			if apocCallError39 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError39)
			}
			return apocCallValue39
		}, want: []int64{}},
		{name: "FullText", call: func() []*NodeType {
			apocCallValue40, apocCallError40 := functions.FullText(ctx, "Person", "name", "Alice Bob")
			if apocCallError40 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError40)
			}
			return apocCallValue40
		}, want: []int64{1, 3}},
		{name: "Fuzzy", call: func() []*NodeType {
			apocCallValue41, apocCallError41 := functions.Fuzzy(ctx, "Person", "name", "Alise", 2)
			if apocCallError41 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError41)
			}
			return apocCallValue41
		}, want: []int64{1, 2}},
		{name: "Regex", call: func() []*NodeType {
			apocCallValue42, apocCallError42 := functions.Regex(ctx, "Person", "email", `.*@example\.com$`)
			if apocCallError42 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError42)
			}
			return apocCallValue42
		}, want: []int64{1, 2}},
		{name: "Regex/invalid pattern", call: func() []*NodeType {
			apocCallValue43, apocCallError43 := functions.Regex(ctx, "Person", "email", "[")
			if apocCallError43 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError43)
			}
			return apocCallValue43
		}, want: []int64{}},
		{name: "Prefix", call: func() []*NodeType {
			apocCallValue44, apocCallError44 := functions.Prefix(ctx, "Person", "name", "Al")
			if apocCallError44 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError44)
			}
			return apocCallValue44
		}, want: []int64{1, 2}},
		{name: "Suffix", call: func() []*NodeType {
			apocCallValue45, apocCallError45 := functions.Suffix(ctx, "Person", "email", ".com")
			if apocCallError45 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError45)
			}
			return apocCallValue45
		}, want: []int64{1, 2}},
		{name: "Contains", call: func() []*NodeType {
			apocCallValue46, apocCallError46 := functions.Contains(ctx, "Person", "name", "li")
			if apocCallError46 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError46)
			}
			return apocCallValue46
		}, want: []int64{1, 2, 4}},
		{name: "Range/integer", call: func() []*NodeType {
			apocCallValue47, apocCallError47 := functions.Range(ctx, "Person", "age", int64(18), int64(50))
			if apocCallError47 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError47)
			}
			return apocCallValue47
		}, want: []int64{1, 2}},
		{name: "Range/string", call: func() []*NodeType {
			apocCallValue48, apocCallError48 := functions.Range(ctx, "Person", "name", "Alice", "Alison")
			if apocCallError48 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError48)
			}
			return apocCallValue48
		}, want: []int64{1, 2}},
		{name: "In", call: func() []*NodeType {
			apocCallValue49, apocCallError49 := functions.In(ctx, "Person", "status", []interface{}{"active"})
			if apocCallError49 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError49)
			}
			return apocCallValue49
		}, want: []int64{1, 4}},
		{name: "NotIn", call: func() []*NodeType {
			apocCallValue50, apocCallError50 := functions.NotIn(ctx, "Person", "status", []interface{}{"active"})
			if apocCallError50 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError50)
			}
			return apocCallValue50
		}, want: []int64{2, 3}},
		{name: "Exists", call: func() []*NodeType {
			apocCallValue51, apocCallError51 := functions.Exists(ctx, "Person", "email")
			if apocCallError51 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError51)
			}
			return apocCallValue51
		}, want: []int64{1, 2, 3}},
		{name: "Missing", call: func() []*NodeType {
			apocCallValue52, apocCallError52 := functions.Missing(ctx, "Person", "email")
			if apocCallError52 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError52)
			}
			return apocCallValue52
		}, want: []int64{4, 6}},
		{name: "Null", call: func() []*NodeType {
			apocCallValue53, apocCallError53 := functions.Null(ctx, "Person", "note")
			if apocCallError53 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError53)
			}
			return apocCallValue53
		}, want: []int64{1}},
		{name: "NotNull", call: func() []*NodeType {
			apocCallValue54, apocCallError54 := functions.NotNull(ctx, "Person", "note")
			if apocCallError54 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError54)
			}
			return apocCallValue54
		}, want: []int64{}},
		{name: "Match", call: func() []*NodeType {
			apocCallValue55, apocCallError55 := functions.Match(ctx, "Person", "name", "Al*")
			if apocCallError55 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError55)
			}
			return apocCallValue55
		}, want: []int64{1, 2}},
		{name: "Parallel", call: func() []*NodeType {
			apocCallValue56, apocCallError56 := functions.Parallel(ctx, []string{"Person", "Company"}, "name", "Alice")
			if apocCallError56 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError56)
			}
			return apocCallValue56
		}, want: []int64{1}},
		{name: "MultiSearchAll", call: func() []*NodeType {
			apocCallValue57, apocCallError57 := functions.MultiSearchAll(ctx, []string{"Person", "Company"}, map[string]interface{}{"status": "active"})
			if apocCallError57 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError57)
			}
			return apocCallValue57
		}, want: []int64{1, 4, 5}},
		{name: "MultiSearchAny", call: func() []*NodeType {
			apocCallValue58, apocCallError58 := functions.MultiSearchAny(ctx, []string{"Person", "Company"}, map[string]interface{}{"status": "active"})
			if apocCallError58 != nil {
				t.Fatalf("APOC helper failed: %v", apocCallError58)
			}
			return apocCallValue58
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

	apocCallValue59, apocCallError59 := functions.Suggest(ctx, "Person", "name", "Al", 5)
	if apocCallError59 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError59)
	}
	require.Equal(t, []string{"Alice", "Alison"}, apocCallValue59)
	apocCallValue60, apocCallError60 := functions.Suggest(ctx, "Person", "name", "Al", 1)
	if apocCallError60 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError60)
	}
	require.Equal(t, []string{"Alice"}, apocCallValue60)
	apocCallValue61, apocCallError61 := functions.Suggest(ctx, "Person", "name", "Al", 0)
	if apocCallError61 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError61)
	}
	require.Equal(t, []string{}, apocCallValue61)
	apocCallValue62, apocCallError62 := functions.Suggest(ctx, "Person", "name", "Al")
	if apocCallError62 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError62)
	}
	require.Equal(t, []string{"Alice", "Alison"}, apocCallValue62,
		"the default limit selects every match of this population")
	apocCallValue63, apocCallError63 := functions.DidYouMean(ctx, "Person", "name", "Alise")
	if apocCallError63 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError63)
	}
	require.Equal(t, []string{"Alice", "Alison"}, apocCallValue63)
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
	live, apocCallError64 := functions.Suggest(ctx, "Person", "name", "Al", 5)
	if apocCallError64 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError64)
	}

	require.Equal(t, []string{"Alice", "Alison"}, live)

	canceled, stop := context.WithCancel(context.Background())
	stop()
	apocCallValue65, apocCallError65 := functions.Suggest(canceled, "Person", "name", "Al", 5)
	require.ErrorIs(t, apocCallError65, context.Canceled)
	require.Nil(t, apocCallValue65)
	apocCallValue66, apocCallError66 := functions.DidYouMean(canceled, "Person", "name", "Alise")
	require.ErrorIs(t, apocCallError66, context.Canceled)
	require.Nil(t, apocCallValue66)

	stopping := installProbe(t, &probeStorage{
		nodes: []*storage.Node{
			{ID: 1, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}},
			{ID: 2, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alison"}},
		},
		afterRead: cancel,
	})
	apocCallValue67, apocCallError67 := stopping.Suggest(ctx, "Person", "name", "Al", 5)
	require.ErrorIs(t, apocCallError67, context.Canceled)
	require.Nil(t, apocCallValue67,
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
	matched, apocCallError68 := functions.Prefix(ctx, "Person", "name", "Ali")
	if apocCallError68 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError68)
	}

	require.Len(t, matched, 2)

	scored := functions.Score(ctx, matched, "name", "Alice")
	require.Len(t, scored, 2)
	require.Equal(t, 1.0, scored[0]["score"])

	reduced, apocCallError69 := functions.NodeReduced(ctx, "Person", map[string]interface{}{"name": "Alice"}, 1)
	if apocCallError69 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError69)
	}

	require.Equal(t, []int64{1}, scanNodeIDs(reduced))
	apocCallValue70, apocCallError70 := functions.NodeReduced(ctx, "Person", map[string]interface{}{}, 10)
	if apocCallError70 != nil {
		t.Fatalf("APOC helper failed: %v", apocCallError70)
	}
	require.Equal(t, []int64{1, 2}, scanNodeIDs(apocCallValue70))

	canceled, stop := context.WithCancel(context.Background())
	stop()
	probe.reads = 0
	require.NotNil(t, functions.Score(canceled, matched, "name", "Alice"))
	require.Empty(t, functions.Score(canceled, matched, "name", "Alice"))
	apocCallValue71, apocCallError71 := functions.NodeReduced(canceled, "Person", map[string]interface{}{}, 10)
	require.ErrorIs(t, apocCallError71, context.Canceled)
	require.Nil(t, apocCallValue71)
	apocCallValue72, apocCallError72 := functions.NodeReduced(canceled, "Person", map[string]interface{}{}, 10)
	require.ErrorIs(t, apocCallError72, context.Canceled)
	require.Empty(t, apocCallValue72)
	require.Zero(t, probe.reads, "a stopped caller reads no storage")

	expired, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-expired.Done()
	require.Empty(t, functions.Score(expired, matched, "name", "Alice"))
	apocCallValue73, apocCallError73 := functions.NodeReduced(expired, "Person", map[string]interface{}{}, 10)
	require.ErrorIs(t, apocCallError73, context.DeadlineExceeded)
	require.Empty(t, apocCallValue73)
}
