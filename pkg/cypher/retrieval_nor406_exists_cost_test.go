package cypher

// Author: NOR-406 measured public retrieval root-cause lane, 2026-11-19.
//
// The Linux public benchmark (BenchmarkSearchSourceRetrieval, 12.5K/25K/49K)
// measures the retrieval "where" pattern of retrieve_where_test.go:
//
//	EXISTS { (node)<-[:HAS_SOURCE]-(v:Version)<-[:HAS_VERSION]-(:Document)
//	         -[:MEMBER_OF]->(c:Collection)
//	         WHERE c.id IN $ids AND NOT c.archived
//	           AND v.current_source_id = node.id }
//
// At 49K it costs ~2.20 s, ~1.05 GB and ~5.18 M allocations per first page,
// against ~12 ms and ~1.4 MB without the predicate. Its CPU and allocation
// profiles attribute most of that to findPaths, traverseChainedGraph,
// newTraversalContext, loadTraversalEndpointNode and node-body copies, but a
// profile's top functions do not by themselves establish which candidate path
// runs them, how many times, or from which bound node. This test establishes
// that with counts instead of a profile.
//
// What it measures: the same public statement the benchmark runs,
//
//	CALL db.retrieve($request) YIELD page
//
// with the benchmark's own fixture (356 source groups, 1024-dimension
// embeddings, the 50-row limit, minRRFScore 0) at two deterministic
// populations: 1,000 sources and a 2,000-source control, under both statement
// parsers. A read ledger wrapped under the fixture's namespaced engine counts
// what the public path asked storage for: label scans, node streams, whole-node
// reads, point query-node reads, batch reads, and every adjacent edge read by
// requested relationship type and by the node it expanded from.
//
// What it shows, and what it deliberately does not:
//
//   - Only 178 sources are admissible (one current source for each of the 178
//     listed, non-archived groups), and a page returns 10 of them. The
//     predicate is nevertheless evaluated once for every candidate the ranked
//     continuation pulls in, and each evaluation expands that candidate's own
//     three-segment chain: the ledger counts one HAS_SOURCE, one HAS_VERSION
//     and one MEMBER_OF adjacent read, and one point query-node body read, per
//     evaluated candidate, with no label scan and no whole-node scan standing
//     in for the bound expansion. That is the excessive work: a per-candidate
//     chained traversal for a 50-row answer.
//   - The unfiltered control request issues no adjacent read at all, so every
//     counted expansion belongs to the where predicate and not to ranking.
//   - Admitting the whole matched population forces the chain from all N
//     sources, so the count is a per-candidate term rather than one fixed
//     startup read. That is what makes it the causal owner of the
//     population-proportional growth the Linux benchmark reports between
//     12.5K, 25K and 49K.
//
// Limits, disclosed: this is a unit reproduction, not the Linux qualification.
// It runs an in-memory engine on the current host, with the fixture's supplied
// query embedding and its network-free controlled reranker, so it says nothing
// about the private application's provider-included 3.9-6.9 s retrieval, and it
// is not the 16-way concurrency stress the benchmark matrix also runs. It
// changes no production code: it records the current engine's filter cost and
// its owner. A fix, if the parent accepts one, owns its own fail-first
// regression on this same public path.

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nor406AuditPopulations is the measured ladder: the benchmark's smallest
// corpus that still holds every one of its 356 source groups, and twice it as
// a control. Two sizes are enough to separate a per-candidate term from a
// fixed one, and small enough for a deterministic unit run.
var nor406AuditPopulations = []int{1000, 2000}

// nor406AuditMatchedSources is how many sources the EXISTS predicate admits at
// every measured population: 356 groups, of which the groups in collections 0
// and 1 are listed and not archived, each with exactly one current source.
const nor406AuditMatchedSources = 178

// nor406AuditParsers are the statement parsers the executor can be configured
// with; the public path must cost the same through either.
var nor406AuditParsers = []string{"nornic", "antlr"}

// nor406ReadCounts is what one public retrieval asked storage for. Adjacency
// reads are kept by the relationship type the traversal requested and by the
// node it expanded from, because that naming is what attributes an expansion to
// the EXISTS chain's segment and to the candidate it was bound to.
type nor406ReadCounts struct {
	LabelScans       int64
	FirstNodeByLabel int64
	AllNodes         int64
	NodeStreams      int64
	AdjacencyReads   int64
	AdjacencyByType  map[string]int64
	AdjacencySeeds   map[string]map[storage.NodeID]struct{}
	EdgesByType      int64
	AllEdges         int64

	// Point entity reads, by the capability each was asked through. The
	// public retrieval asks for a query-node image (metadata contract) rather
	// than a whole node, so the split says which read the owner actually made.
	NodeReads                       int64
	NodeReadsWithoutEmbeddings      int64
	QueryNodeReads                  int64
	ProjectedNodeReads              int64
	BatchNodeReads                  int64
	BatchNodeReadsWithoutEmbeddings int64
	EdgeReads                       int64
	EdgeBetween                     int64
	EndpointVisibility              int64
}

// pointReads is every point node-body read, whichever capability was asked.
func (c nor406ReadCounts) pointReads() int64 {
	return c.NodeReads + c.NodeReadsWithoutEmbeddings + c.QueryNodeReads + c.ProjectedNodeReads +
		c.BatchNodeReads + c.BatchNodeReadsWithoutEmbeddings
}

// distinctSeeds is how many different nodes the segment named by relationship
// expanded from: the evaluated candidates of that segment.
func (c nor406ReadCounts) distinctSeeds(relationship string) int {
	return len(c.AdjacencySeeds[relationship])
}

func (c nor406ReadCounts) seedCounts() map[string]int {
	counts := make(map[string]int, len(c.AdjacencySeeds))
	for relationship, seeds := range c.AdjacencySeeds {
		counts[relationship] = len(seeds)
	}
	return counts
}

func (c nor406ReadCounts) String() string {
	return fmt.Sprintf(
		"label-scans=%d first-by-label=%d all-nodes=%d node-streams=%d adjacency=%d by-type=%v seeds-by-type=%v "+
			"edges-by-type=%d all-edges=%d point-reads=%d (node=%d without-embeddings=%d query-node=%d projected=%d batch=%d batch-without-embeddings=%d) endpoint-visibility=%d edge-reads=%d edge-between=%d",
		c.LabelScans, c.FirstNodeByLabel, c.AllNodes, c.NodeStreams, c.AdjacencyReads, c.AdjacencyByType, c.seedCounts(),
		c.EdgesByType, c.AllEdges, c.pointReads(), c.NodeReads, c.NodeReadsWithoutEmbeddings,
		c.QueryNodeReads, c.ProjectedNodeReads, c.BatchNodeReads, c.BatchNodeReadsWithoutEmbeddings,
		c.EndpointVisibility, c.EdgeReads, c.EdgeBetween)
}

// nor406ReadLedger is the fixture's in-memory engine with every read the
// public path can make counted. It embeds the concrete engine, so every other
// method and every optional capability the fixture's engine already has stays
// exactly as it was: the retrieval under measurement is the real one and only
// the observation is added.
type nor406ReadLedger struct {
	*storage.MemoryEngine

	mu     sync.Mutex
	counts nor406ReadCounts
}

func newNOR406ReadLedger(t *testing.T) *nor406ReadLedger {
	t.Helper()
	return &nor406ReadLedger{
		MemoryEngine: newTestMemoryEngine(t),
		counts:       nor406EmptyReadCounts(),
	}
}

func nor406EmptyReadCounts() nor406ReadCounts {
	return nor406ReadCounts{
		AdjacencyByType: map[string]int64{},
		AdjacencySeeds:  map[string]map[storage.NodeID]struct{}{},
	}
}

// reset starts a new measurement: the fixture's own writes and index build are
// not part of what the public retrieval reads.
func (l *nor406ReadLedger) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.counts = nor406EmptyReadCounts()
}

func (l *nor406ReadLedger) record(observe func(*nor406ReadCounts)) {
	l.mu.Lock()
	observe(&l.counts)
	l.mu.Unlock()
}

// snapshot is a copy of the counts, so a later reset cannot change a
// measurement already taken.
func (l *nor406ReadLedger) snapshot() nor406ReadCounts {
	l.mu.Lock()
	defer l.mu.Unlock()
	copied := l.counts
	copied.AdjacencyByType = make(map[string]int64, len(l.counts.AdjacencyByType))
	for relationship, count := range l.counts.AdjacencyByType {
		copied.AdjacencyByType[relationship] = count
	}
	copied.AdjacencySeeds = make(map[string]map[storage.NodeID]struct{}, len(l.counts.AdjacencySeeds))
	for relationship, seeds := range l.counts.AdjacencySeeds {
		kept := make(map[storage.NodeID]struct{}, len(seeds))
		for seed := range seeds {
			kept[seed] = struct{}{}
		}
		copied.AdjacencySeeds[relationship] = kept
	}
	return copied
}

func (l *nor406ReadLedger) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	l.record(func(counts *nor406ReadCounts) { counts.NodeReads++ })
	return l.MemoryEngine.GetNode(ctx, id)
}

func (l *nor406ReadLedger) GetQueryNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	l.record(func(counts *nor406ReadCounts) { counts.QueryNodeReads++ })
	return l.MemoryEngine.GetQueryNode(ctx, id)
}

func (l *nor406ReadLedger) GetNodeWithoutEmbeddings(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	l.record(func(counts *nor406ReadCounts) { counts.NodeReadsWithoutEmbeddings++ })
	return l.MemoryEngine.GetNodeWithoutEmbeddings(ctx, id)
}

func (l *nor406ReadLedger) GetNodeProjected(ctx context.Context, id storage.NodeID, properties []string) (*storage.Node, error) {
	l.record(func(counts *nor406ReadCounts) { counts.ProjectedNodeReads++ })
	return l.MemoryEngine.GetNodeProjected(ctx, id, properties)
}

func (l *nor406ReadLedger) BatchGetNodes(ctx context.Context, ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	l.record(func(counts *nor406ReadCounts) { counts.BatchNodeReads++ })
	return l.MemoryEngine.BatchGetNodes(ctx, ids)
}

func (l *nor406ReadLedger) BatchGetNodesWithoutEmbeddings(ctx context.Context, ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	l.record(func(counts *nor406ReadCounts) { counts.BatchNodeReadsWithoutEmbeddings++ })
	return l.MemoryEngine.BatchGetNodesWithoutEmbeddings(ctx, ids)
}

func (l *nor406ReadLedger) RelationshipEndpointVisible(id storage.NodeID) (bool, bool) {
	l.record(func(counts *nor406ReadCounts) { counts.EndpointVisibility++ })
	return l.MemoryEngine.RelationshipEndpointVisible(id)
}

func (l *nor406ReadLedger) GetNodesByLabel(ctx context.Context, label string) ([]*storage.Node, error) {
	l.record(func(counts *nor406ReadCounts) { counts.LabelScans++ })
	return l.MemoryEngine.GetNodesByLabel(ctx, label)
}

func (l *nor406ReadLedger) GetFirstNodeByLabel(ctx context.Context, label string) (*storage.Node, error) {
	l.record(func(counts *nor406ReadCounts) { counts.FirstNodeByLabel++ })
	return l.MemoryEngine.GetFirstNodeByLabel(ctx, label)
}

func (l *nor406ReadLedger) AllNodes(ctx context.Context) ([]*storage.Node, error) {
	l.record(func(counts *nor406ReadCounts) { counts.AllNodes++ })
	return l.MemoryEngine.AllNodes(ctx)
}

func (l *nor406ReadLedger) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	l.record(func(counts *nor406ReadCounts) { counts.NodeStreams++ })
	return l.MemoryEngine.StreamNodesWithOptions(ctx, opts, visit)
}

// ReadAdjacentEdges counts one traversal expansion. The requested types are
// the chain segment's relationship types and the request's node is the node the
// segment expanded from, so a HAS_SOURCE entry names one candidate whose own
// chain was walked, not a ranking read.
func (l *nor406ReadLedger) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	requested := "any"
	if len(request.Types) > 0 {
		requested = request.Types[0]
	}
	l.record(func(counts *nor406ReadCounts) {
		counts.AdjacencyReads++
		counts.AdjacencyByType[requested]++
		seeds := counts.AdjacencySeeds[requested]
		if seeds == nil {
			seeds = make(map[storage.NodeID]struct{})
			counts.AdjacencySeeds[requested] = seeds
		}
		seeds[request.Node] = struct{}{}
	})
	return l.MemoryEngine.ReadAdjacentEdges(ctx, request)
}

func (l *nor406ReadLedger) GetEdgesByType(ctx context.Context, edgeType string) ([]*storage.Edge, error) {
	l.record(func(counts *nor406ReadCounts) { counts.EdgesByType++ })
	return l.MemoryEngine.GetEdgesByType(ctx, edgeType)
}

func (l *nor406ReadLedger) AllEdges(ctx context.Context) ([]*storage.Edge, error) {
	l.record(func(counts *nor406ReadCounts) { counts.AllEdges++ })
	return l.MemoryEngine.AllEdges(ctx)
}

func (l *nor406ReadLedger) GetEdge(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	l.record(func(counts *nor406ReadCounts) { counts.EdgeReads++ })
	return l.MemoryEngine.GetEdge(ctx, id)
}

func (l *nor406ReadLedger) GetEdgeBetween(ctx context.Context, startID, endID storage.NodeID, edgeType string) (*storage.Edge, error) {
	l.record(func(counts *nor406ReadCounts) { counts.EdgeBetween++ })
	return l.MemoryEngine.GetEdgeBetween(ctx, startID, endID, edgeType)
}

// nor406AuditMeasurement is one public retrieval of one population, with the
// rows it returned and the reads it made.
type nor406AuditMeasurement struct {
	Parser     string
	Population int

	PlainElapsed  time.Duration
	ExistsElapsed time.Duration
	AllElapsed    time.Duration

	Plain  nor406ReadCounts
	Exists nor406ReadCounts
	All    nor406ReadCounts

	PlainPage     *nor406Page
	ExistsPage    *nor406Page
	AdmittedCount int
}

// evaluatedCandidates is how many candidates the EXISTS predicate was
// evaluated for in the filtered first page: the chain's first segment expands
// once per evaluation, from the evaluated candidate.
func (m nor406AuditMeasurement) evaluatedCandidates() int64 {
	return m.Exists.AdjacencyByType["HAS_SOURCE"]
}

// perCandidateNanos is the filtered first page's elapsed time per evaluated
// candidate: the observation that the population multiplies.
func (m nor406AuditMeasurement) perCandidateNanos() time.Duration {
	candidates := m.evaluatedCandidates()
	if candidates == 0 {
		return 0
	}
	return time.Duration(int64(m.ExistsElapsed) / candidates)
}

// TestNOR406RetrieveExistsWhereCostOwner measures the public retrieval's
// EXISTS where at each parser and population, requires the correct matched
// rows, and requires the per-candidate chained traversal the reads show. The
// 1,000- and 2,000-source runs also require the per-candidate read count to be
// the same constant, which is what identifies the cost as a per-candidate term
// rather than a population-dependent scan.
func TestNOR406RetrieveExistsWhereCostOwner(t *testing.T) {
	measured := make(map[string]map[int]nor406AuditMeasurement, len(nor406AuditParsers))
	for _, parser := range nor406AuditParsers {
		measured[parser] = make(map[int]nor406AuditMeasurement, len(nor406AuditPopulations))
		for _, population := range nor406AuditPopulations {
			population := population
			t.Run(parser+"/"+strconv.Itoa(population), func(t *testing.T) {
				measurement := nor406MeasureExistsWhereCost(t, parser, population)
				nor406RequireMatchedRows(t, measurement)
				nor406RequireCandidateChainWork(t, measurement)
				measured[parser][population] = measurement
			})
		}
	}
	for _, parser := range nor406AuditParsers {
		t.Run(parser+"/per-candidate-cost", func(t *testing.T) {
			small := measured[parser][nor406AuditPopulations[0]]
			control := measured[parser][nor406AuditPopulations[1]]
			// Every candidate reads its first segment. Only each group's current
			// source can pass the first prefix and read the two remaining segments.
			for _, measurement := range []nor406AuditMeasurement{small, control} {
				want := int64(measurement.Population + 2*min(measurement.Population, nor406SourceGroups))
				require.Equal(t, int64(measurement.Population), measurement.All.AdjacencyByType["HAS_SOURCE"])
				require.Equal(t, want, measurement.All.AdjacencyReads)
				require.Equal(t, want, measurement.All.QueryNodeReads)
				require.Less(t, measurement.Exists.AdjacencyReads, 3*measurement.evaluatedCandidates())
			}
			t.Logf("parser=%s per-candidate: %v at %d sources, %v at %d sources",
				parser, small.perCandidateNanos(), small.Population, control.perCandidateNanos(), control.Population)
		})
	}
}

// nor406MeasureExistsWhereCost runs, on one fresh fixture:
//
//  1. the unfiltered first page, the control that must issue no chain read;
//  2. the filtered first page, the benchmark's own measured case;
//  3. one filtered one-shot retrieval whose limit admits every admissible
//     source, which is how the matched identities are established.
//
// Each run starts from reset counts, so its numbers are its own.
func nor406MeasureExistsWhereCost(t *testing.T, parser string, population int) nor406AuditMeasurement {
	t.Helper()
	previous := config.GetParserType()
	config.SetParserType(parser)
	t.Cleanup(func() { config.SetParserType(previous) })
	t.Setenv(search.EnvSearchResultCacheEnabled, "false")

	ledger := newNOR406ReadLedger(t)
	fixture := newNOR406RetrievalFixtureOnEngine(t, population, ledger)
	exec := fixture.newExecutor()

	measurement := nor406AuditMeasurement{Parser: parser, Population: population}

	measurement.PlainPage, measurement.Plain, measurement.PlainElapsed = nor406AuditFirstPage(t, exec, fixture, ledger, false)
	measurement.ExistsPage, measurement.Exists, measurement.ExistsElapsed = nor406AuditFirstPage(t, exec, fixture, ledger, true)
	measurement.AdmittedCount, measurement.All, measurement.AllElapsed = nor406AuditAllAdmitted(t, exec, fixture, ledger)

	t.Logf("parser=%s population=%d plain(%v): %v", parser, population, measurement.PlainElapsed, measurement.Plain)
	t.Logf("parser=%s population=%d exists(%v, %v/candidate, %d admitted of %d, %d evaluations): %v",
		parser, population, measurement.ExistsElapsed, measurement.perCandidateNanos(), measurement.AdmittedCount,
		population, measurement.evaluatedCandidates(), measurement.Exists)
	t.Logf("parser=%s population=%d all-admitted(%v): %v", parser, population, measurement.AllElapsed, measurement.All)
	return measurement
}

// nor406AuditFirstPage runs the benchmark's ranked first page through the
// public procedure, snapshots the reads of that statement, then releases the
// cursor. The discard executes after the snapshot, so it is not counted.
func nor406AuditFirstPage(t *testing.T, exec *StorageExecutor, fixture *nor406RetrievalFixture, ledger *nor406ReadLedger, filtered bool) (*nor406Page, nor406ReadCounts, time.Duration) {
	t.Helper()
	pageCall := "CALL db.retrieve($request) YIELD page"
	ledger.reset()
	started := time.Now()
	result, err := exec.Execute(t.Context(), pageCall, fixture.parameters(fixture.firstPageRequest(filtered)))
	elapsed := time.Since(started)
	counts := ledger.snapshot()
	require.NoError(t, err)
	page, err := nor406ReadPage(result)
	require.NoError(t, err)
	if page.qid != "" {
		require.NoError(t, nor406Discard(t.Context(), exec, pageCall, page.qid))
	}
	return page, counts, elapsed
}

// nor406AuditAllAdmitted retrieves with a limit that admits every admissible
// source in one statement, so the returned rows are the whole matched
// population rather than one page of it.
func nor406AuditAllAdmitted(t *testing.T, exec *StorageExecutor, fixture *nor406RetrievalFixture, ledger *nor406ReadLedger) (int, nor406ReadCounts, time.Duration) {
	t.Helper()
	request := fixture.oneShotRequest(true)
	request["limit"] = int64(nor406SourceGroups)
	ledger.reset()
	started := time.Now()
	result, err := exec.Execute(t.Context(), "CALL db.retrieve($request)", fixture.parameters(request))
	elapsed := time.Since(started)
	counts := ledger.snapshot()
	require.NoError(t, err)
	nodes, err := nor406ResultNodes(result)
	require.NoError(t, err)
	require.NoError(t, nor406CheckSourceIDs(nodes, true))
	require.Equal(t, nor406ExpectedMatchedSourceIDs(), nor406SortedSourceIDs(nodes),
		"the whole matched population, and only it, must be returned")
	return len(nodes), counts, elapsed
}

// nor406RequireMatchedRows requires the page and the whole matched population
// to be the identities the predicate admits: every page row in the matched
// set, and the matched population exactly the 178 expected identities.
func nor406RequireMatchedRows(t *testing.T, measurement nor406AuditMeasurement) {
	t.Helper()
	require.Len(t, measurement.PlainPage.nodes, nor406PageSize)
	require.True(t, measurement.PlainPage.hasMore, "an unfiltered page of %d has more rows", measurement.Population)
	require.NoError(t, nor406CheckSourceIDs(measurement.PlainPage.nodes, false))

	require.Len(t, measurement.ExistsPage.nodes, nor406PageSize)
	require.True(t, measurement.ExistsPage.hasMore)
	require.Equal(t, measurement.ExistsPage.returned, len(measurement.ExistsPage.nodes))
	require.NotEmpty(t, measurement.ExistsPage.qid)
	require.NoError(t, nor406CheckSourceIDs(measurement.ExistsPage.nodes, true))

	expected := nor406ExpectedMatchedSourceIDs()
	require.Equal(t, nor406AuditMatchedSources, measurement.AdmittedCount,
		"the EXISTS where admits one current source for each of the 178 listed active groups")
	require.Subset(t, expected, nor406SortedSourceIDs(measurement.ExistsPage.nodes),
		"every returned row must be one of the 178 matched identities")
}

// nor406RequireCandidateChainWork requires the reads the filter's owner makes:
// the unfiltered control expands no chain, and the filtered retrieval expands
// one three-segment chain from every candidate it decides, in that candidate's
// own direction and independently of the 178 admissible sources. The expansion
// is what the Linux benchmark's CPU and allocation profiles attribute to
// findPaths, traverseChainedGraph and the endpoint body reads.
func nor406RequireCandidateChainWork(t *testing.T, measurement nor406AuditMeasurement) {
	t.Helper()
	plain, exists := measurement.Plain, measurement.Exists

	// Ranking itself never walks the graph and reads no node bodies point by
	// point: the control proves the reads counted below belong to the where
	// predicate. Its returned rows are hydrated in batches.
	require.Zero(t, plain.AdjacencyReads, "unfiltered ranking expanded a relationship chain: %v", plain)
	require.Zero(t, plain.EdgesByType, "unfiltered ranking listed relationships by type: %v", plain)
	require.Zero(t, plain.AllEdges, "unfiltered ranking read every relationship: %v", plain)
	require.Zero(t, plain.QueryNodeReads, "unfiltered ranking read point node bodies: %v", plain)
	require.NotZero(t, plain.BatchNodeReadsWithoutEmbeddings, "unfiltered ranking hydrated no page row: %v", plain)

	// Each candidate reads HAS_SOURCE once. The existing prefix predicate
	// rejects non-current sources before HAS_VERSION and MEMBER_OF reads.
	evaluated := measurement.evaluatedCandidates()
	population := int64(measurement.Population)
	require.NotZero(t, evaluated, "the filtered retrieval evaluated no candidate: %v", exists)
	require.LessOrEqual(t, evaluated, population,
		"the filtered retrieval evaluated more candidates than the population: %v", exists)
	require.Equal(t, evaluated, int64(exists.distinctSeeds("HAS_SOURCE")),
		"a candidate must be decided once, not once per ranked phase: %v", exists)
	surviving := exists.AdjacencyByType["HAS_VERSION"]
	require.Equal(t, int64(exists.distinctSeeds("HAS_VERSION")), surviving,
		"only the unique current source of each version may read its suffix: %v", exists)
	require.LessOrEqual(t, surviving, int64(nor406SourceGroups))
	require.Equal(t, surviving, exists.AdjacencyByType["MEMBER_OF"])
	require.Equal(t, evaluated+2*surviving, exists.AdjacencyReads,
		"only prefixes still eligible after current_source_id may reach both suffix segments: %v", exists)

	// The chain is walked from the bound candidate, so no label scan of its
	// endpoints stands in for a bound expansion.
	require.Zero(t, exists.LabelScans, "the bound chain traversed by label scan: %v", exists)
	require.Zero(t, exists.FirstNodeByLabel, "the bound chain read a first node by label: %v", exists)
	require.Zero(t, exists.AllNodes, "the bound chain read every node: %v", exists)
	require.Zero(t, exists.NodeStreams, "the bound chain streamed nodes: %v", exists)

	// Each expansion reads its endpoint's body through the query-node
	// contract: one point body read per expanded segment. Candidate bodies themselves
	// arrive in batches, not as one read per candidate.
	require.NotZero(t, exists.QueryNodeReads, "the WHERE read no endpoint body: %v", exists)
	require.Equal(t, exists.AdjacencyReads, exists.QueryNodeReads,
		"each expanded segment must read its endpoint's body: %v", exists)
	require.Less(t, exists.BatchNodeReadsWithoutEmbeddings, evaluated,
		"candidate bodies must be read in batches, not once per candidate: %v", exists)
	require.Greater(t, exists.pointReads(), plain.pointReads(),
		"the filtered retrieval must read more node bodies than the control: %v vs %v", exists, plain)
}

// nor406ExpectedMatchedSourceIDs is the matched population in id order: for
// every listed, non-archived group (collections 0 and 1 of 4), its group
// number is also its current source's index.
func nor406ExpectedMatchedSourceIDs() []string {
	ids := make([]string, 0, nor406AuditMatchedSources)
	for group := 0; group < nor406SourceGroups; group++ {
		if group%nor406Collections >= nor406ActiveCollections {
			continue
		}
		ids = append(ids, nor406SourceIDPrefix+strconv.Itoa(group))
	}
	sort.Strings(ids)
	return ids
}

// nor406SortedSourceIDs is the returned nodes' id properties in the same order
// nor406ExpectedMatchedSourceIDs uses.
func nor406SortedSourceIDs(nodes []*storage.Node) []string {
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		id, ok := node.Properties["id"].(string)
		if !ok {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
