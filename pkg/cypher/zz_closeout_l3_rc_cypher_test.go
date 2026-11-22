package cypher

// Close-out lane L3: root-cause proof runs for the cypher items that failed on
// the pin e8cd436e5 (and on v1.5.1): P-N152-95, P-N15-136, P-N15-135,
// P-N152-119 and S7-NOR-119. Test-side only. Each test logs the decisive value
// with the prefix CLOSEOUT_L3_RC and asserts the mechanism (MECHANISM), a
// passing control (control) or a bypass that removes the failure (BYPASS).

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"runtime/pprof"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/pprof/profile"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func closeoutL3RCMapPtr(m map[string]interface{}) uintptr {
	if m == nil {
		return 0
	}
	return reflect.ValueOf(m).Pointer()
}

// ---------------------------------------------------------------------------
// P-N152-95: the alias header keeps the \u escape while the map key decodes.
// Diagnostic: the projection plan built from the submitted text, the item
// source at both stages and SymbolicNameValue (which only strips backticks).
// Bypass: the same statement with its \u escapes projected test-side
// (cyphertext.ProjectUnicode) gets the Neo4j header.
// ---------------------------------------------------------------------------

func TestCloseoutL3_RC_N152_95_AliasHeaderDecoding(t *testing.T) {
	closeoutL3EachParser(t, func(t *testing.T) {
		ctx := context.Background()
		exec := closeoutL3Executor(t)
		query := "RETURN {`a\\u0062`:1} AS m, 1 AS `a\\u0062`"
		returnPart := strings.TrimPrefix(query, "RETURN ")

		projected := cyphertext.ProjectUnicode(ctx, query)
		require.NoError(t, projected.Err)
		plan := returnProjectionPlanFor(ctx, "RETURN "+returnPart)
		var planAliases []string
		for _, p := range plan.projections {
			planAliases = append(planAliases, p.alias)
		}
		before := readProjectionItemSource(ctx, "1 AS `a\\u0062`", projectionBeforeRewrite)
		admitted := readProjectionItemSource(ctx, "1 AS `a\\u0062`", projectionAdmitted)
		t.Logf("CLOSEOUT_L3_RC N152-95 projectedText=%q planAliases=%q itemBeforeRewrite=%q itemAdmitted=%q SymbolicNameValue=%q",
			projected.Text, planAliases, before.alias, admitted.alias, cyphertext.SymbolicNameValue("`a\\u0062`"))

		res, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		bypass, err := exec.Execute(ctx, projected.Text, nil)
		require.NoError(t, err)
		t.Logf("CLOSEOUT_L3_RC N152-95 submitted columns=%q rows=%v | projected columns=%q rows=%v", res.Columns, res.Rows, bypass.Columns, bypass.Rows)

		m, ok := res.Rows[0][0].(map[string]interface{})
		require.True(t, ok)
		assert.Contains(t, m, "ab", "control: the map key decodes on the submitted text")
		assert.Equal(t, "a\\u0062", res.Columns[1], "MECHANISM: the header is the undecoded spelling")
		assert.Equal(t, []string{"m", "ab"}, bypass.Columns, "BYPASS: with the escape projected before execution the header is Neo4j's")
	})
}

// ---------------------------------------------------------------------------
// P-N15-136: minimal reproductions at each named step, with controls and a
// bypass that shows the catalog itself accepts an empty composite.
// ---------------------------------------------------------------------------

func TestCloseoutL3_RC_N15_136_CatalogPreparationSteps(t *testing.T) {
	ctx := context.Background()

	t.Run("empty_composite_refused_at_constituent_check", func(t *testing.T) {
		exec, manager, _ := newCreateDatabaseRegressionExecutor(t)
		_, err := exec.prepareCreateCompositeDatabase(ctx, "CREATE COMPOSITE DATABASE empty136")
		t.Logf("CLOSEOUT_L3_RC N15-136 prepare empty composite err=%v", err)
		require.Error(t, err, "MECHANISM: composite_command_preparation.go:178 refuses zero constituents")
		require.Contains(t, err.Error(), "constituent")

		require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "exists136"}))
		cmd, err := exec.prepareCreateCompositeDatabase(ctx, "CREATE COMPOSITE DATABASE exists136 IF NOT EXISTS")
		t.Logf("CLOSEOUT_L3_RC N15-136 control IF NOT EXISTS on existing err=%v noOp=%v", err, cmd != nil && cmd.noOp)
		require.NoError(t, err, "control: the same preparation admits the statement shape before :178")
		require.True(t, cmd.noOp)

		bypassErr := manager.CreateCompositeDatabase(t.Context(), "empty136", nil)
		t.Logf("CLOSEOUT_L3_RC N15-136 BYPASS manager.CreateCompositeDatabase(empty) err=%v composite=%v", bypassErr, manager.IsCompositeDatabase("empty136"))
		require.NoError(t, bypassErr, "BYPASS: the catalog owner accepts an empty composite; only the cypher preparation refuses")
		require.True(t, manager.IsCompositeDatabase("empty136"))
	})

	t.Run("alias_if_not_exists_read_as_name", func(t *testing.T) {
		exec, manager, _ := newCreateDatabaseRegressionExecutor(t)
		require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "db136"}))
		_, err := exec.executeCreateAlias(ctx, "CREATE ALIAS al136 IF NOT EXISTS FOR DATABASE db136")
		t.Logf("CLOSEOUT_L3_RC N15-136 executeCreateAlias IF NOT EXISTS err=%v", err)
		require.Error(t, err)
		require.Contains(t, err.Error(), "al136 IF NOT EXISTS", "MECHANISM: executor_show.go:1285 takes the text before FOR DATABASE as the alias name, :1296 rejects its whitespace")
		res, err := exec.executeCreateAlias(ctx, "CREATE ALIAS al136 FOR DATABASE db136")
		require.NoError(t, err, "control: the plain form succeeds")
		t.Logf("CLOSEOUT_L3_RC N15-136 control plain alias columns=%q rows=%v", res.Columns, res.Rows)
	})

	t.Run("drop_composite_if_exists_standard_only", func(t *testing.T) {
		exec, _, _ := newCreateDatabaseRegressionExecutor(t)
		_, err := exec.executeDatabaseDrop(ctx, "DROP COMPOSITE DATABASE nosuch136 IF EXISTS", dropCompositeDatabase)
		t.Logf("CLOSEOUT_L3_RC N15-136 composite drop IF EXISTS err=%v", err)
		require.Error(t, err)
		require.Contains(t, err.Error(), "nosuch136 IF EXISTS", "MECHANISM: database_drop.go:60 reads IF EXISTS only for the standard kind; :78 rejects the joined name")
		res, err := exec.executeDatabaseDrop(ctx, "DROP DATABASE nosuch136 IF EXISTS", dropStandardDatabase)
		t.Logf("CLOSEOUT_L3_RC N15-136 control standard drop IF EXISTS err=%v rows=%v", err, res)
		require.NoError(t, err, "control: the standard kind treats IF EXISTS on a missing name as a no-op")
	})
}

// ---------------------------------------------------------------------------
// P-N15-135: the returning statements themselves, called directly, and the
// public route passing them through unchanged. Control: CREATE DATABASE.
// ---------------------------------------------------------------------------

func TestCloseoutL3_RC_N15_135_AdministrationReturns(t *testing.T) {
	ctx := context.Background()
	exec, manager, _ := newCreateDatabaseRegressionExecutor(t)
	res, err := exec.Execute(ctx, "CREATE DATABASE t135", nil)
	require.NoError(t, err)
	t.Logf("CLOSEOUT_L3_RC N15-135 control CREATE DATABASE columns=%q rows=%v", res.Columns, res.Rows)
	require.Empty(t, res.Columns, "control: CREATE DATABASE already declares no columns")
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "d135"}))

	create, err := exec.executeCreateAlias(ctx, "CREATE ALIAS a135 FOR DATABASE t135")
	require.NoError(t, err)
	drop, err := exec.executeDropAlias(ctx, "DROP ALIAS a135 FOR DATABASE")
	require.NoError(t, err)
	dropDB, err := exec.executeDatabaseDrop(ctx, "DROP DATABASE d135", dropStandardDatabase)
	require.NoError(t, err)
	t.Logf("CLOSEOUT_L3_RC N15-135 executeCreateAlias=%q/%v executeDropAlias=%q/%v executeDatabaseDrop=%q/%v",
		create.Columns, create.Rows, drop.Columns, drop.Rows, dropDB.Columns, dropDB.Rows)
	assert.Equal(t, []string{"alias"}, create.Columns, "MECHANISM: executor_show.go:1338-1341")
	assert.Equal(t, []string{"alias"}, drop.Columns, "MECHANISM: executor_show.go:1434-1436")
	assert.Equal(t, []string{"name"}, dropDB.Columns, "MECHANISM: database_drop.go:98")

	public, err := exec.Execute(ctx, "CREATE ALIAS b135 FOR DATABASE t135", nil)
	require.NoError(t, err)
	t.Logf("CLOSEOUT_L3_RC N15-135 public CREATE ALIAS columns=%q rows=%v", public.Columns, public.Rows)
	assert.Equal(t, create.Columns, public.Columns, "the public route returns the executor's result unchanged")
}

// ---------------------------------------------------------------------------
// P-N152-119: with decay off the published entity is the bound storage entity
// itself (query_property_visibility.go:31-32), properties() returns that
// entity's own map (query_property_visibility.go:162-171, fn/builtins_core.go:358-363),
// and a relationship read through the adjacency cache is the engine's cached
// body (storage badger_adjacency_read.go:260-263 appends b.edgeCache[edgeID]).
// ---------------------------------------------------------------------------

func closeoutL3RCAdjacent(t *testing.T, be *storage.BadgerEngine) *storage.Edge {
	t.Helper()
	edges, err := be.ReadAdjacentEdges(context.Background(), storage.AdjacentEdgeRead{Direction: storage.AdjacentOutgoing, Node: "nor310:a", Types: []string{"LINK"}})
	require.NoError(t, err)
	require.Len(t, edges, 1)
	return edges[0]
}

func TestCloseoutL3_RC_N152_119_RelationshipIsEngineCacheBody(t *testing.T) {
	for _, decay := range []bool{false, true} {
		decay := decay
		t.Run(fmt.Sprintf("decay=%v", decay), func(t *testing.T) {
			ctx := context.Background()
			exec, be := newPublicConsumerViewExecutor(t, 1)
			be.SetDecayEnabled(decay)
			_, err := exec.Execute(ctx, "MATCH (:Doc {title:'a'})-[r:LINK]->() SET r.tags=['original']", nil)
			require.NoError(t, err)
			query := "MATCH (:Doc {title:'a'})-[r:LINK]->() RETURN r AS e, properties(r) AS props"
			first, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			second, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			e1 := closeoutL3EntityProperties(t, first.Rows[0][0])
			e2 := closeoutL3EntityProperties(t, second.Rows[0][0])
			props, ok := first.Rows[0][1].(map[string]interface{})
			require.True(t, ok)
			a1, a2 := closeoutL3RCAdjacent(t, be), closeoutL3RCAdjacent(t, be)
			p1, err := be.GetEdge(ctx, "nor310:linka")
			require.NoError(t, err)
			p2, err := be.GetEdge(ctx, "nor310:linka")
			require.NoError(t, err)
			sameAcrossStatements := closeoutL3RCMapPtr(e1) == closeoutL3RCMapPtr(e2)
			propsIsEntityMap := closeoutL3RCMapPtr(props) == closeoutL3RCMapPtr(e1)
			adjacencyShared := a1 == a2
			publishedIsCacheBody := closeoutL3RCMapPtr(a1.Properties) == closeoutL3RCMapPtr(e1)
			t.Logf("CLOSEOUT_L3_RC N152-119 rel decay=%v carrier=%T sameMapAcrossStatements=%v propertiesIsEntityMap=%v adjacencyReturnsSamePointer=%v publishedIsAdjacencyCacheBody=%v pointReadSamePointer=%v",
				decay, first.Rows[0][0], sameAcrossStatements, propsIsEntityMap, adjacencyShared, publishedIsCacheBody, p1 == p2)

			props["title"] = "leaked"
			a3 := closeoutL3RCAdjacent(t, be)
			p3, err := be.GetEdge(ctx, "nor310:linka")
			require.NoError(t, err)
			third, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			later := closeoutL3EntityProperties(t, third.Rows[0][0])["title"]
			t.Logf("CLOSEOUT_L3_RC N152-119 rel decay=%v after mutating properties(r) of statement 1: adjacencyCache.title=%v pointRead.title=%v laterStatement.title=%v",
				decay, a3.Properties["title"], p3.Properties["title"], later)
			assert.Equal(t, "link", p3.Properties["title"], "control: the stored image (point read) is never changed")
			if decay {
				assert.False(t, publishedIsCacheBody, "BYPASS: the detaching projection publishes a copy")
				assert.Equal(t, "link", later, "BYPASS: with the detaching projection no later statement sees the mutation")
				return
			}
			assert.True(t, adjacencyShared, "MECHANISM: the adjacency cache hit returns the cached *Edge itself")
			assert.True(t, publishedIsCacheBody, "MECHANISM: the published relationship's map is the engine's cached body")
			assert.True(t, propsIsEntityMap, "MECHANISM: properties(r) is the entity's own map")
			assert.Equal(t, "leaked", a3.Properties["title"], "MECHANISM: the caller's write lands in the engine's adjacency cache")
			assert.Equal(t, "leaked", later, "MECHANISM: a later statement reads the mutated cached body")
		})
	}
}

func TestCloseoutL3_RC_N152_119_UnwindRowsShareOneEntity(t *testing.T) {
	for _, decay := range []bool{false, true} {
		decay := decay
		t.Run(fmt.Sprintf("decay=%v", decay), func(t *testing.T) {
			ctx := context.Background()
			exec, be := newPublicConsumerViewExecutor(t, 1)
			be.SetDecayEnabled(decay)
			_, err := exec.Execute(ctx, "MATCH (d:Doc {title:'a'}) SET d.tags=['original']", nil)
			require.NoError(t, err)
			res, err := exec.Execute(ctx, "MATCH (d:Doc {title:'a'}) UNWIND [1,2,3] AS i RETURN d AS e, properties(d) AS props", nil)
			require.NoError(t, err)
			require.Len(t, res.Rows, 3)
			var entityPtrs, propsPtrs []uintptr
			for _, row := range res.Rows {
				entityPtrs = append(entityPtrs, closeoutL3RCMapPtr(closeoutL3EntityProperties(t, row[0])))
				props, _ := row[1].(map[string]interface{})
				propsPtrs = append(propsPtrs, closeoutL3RCMapPtr(props))
			}
			t.Logf("CLOSEOUT_L3_RC N152-119 node decay=%v carrier=%T entityMapPtrs=%v propertiesMapPtrs=%v", decay, res.Rows[0][0], entityPtrs, propsPtrs)
			if decay {
				assert.NotEqual(t, entityPtrs[0], entityPtrs[1], "BYPASS: the detaching projection gives each row its own map")
				return
			}
			assert.Equal(t, entityPtrs[0], entityPtrs[1], "MECHANISM: UNWIND rows publish the same bound entity map")
			assert.Equal(t, entityPtrs[0], propsPtrs[1], "MECHANISM: properties(d) of another row is that same map")
		})
	}
}

// ---------------------------------------------------------------------------
// S7-NOR-119: split scan cost from SET cost and name the owner of the scan by
// CPU profile. Same shape as the stand-in (MATCH ()-[r:AUTO]->() WHERE ...).
// ---------------------------------------------------------------------------

func closeoutL3RCBuildRetireGraph(t *testing.T, edges int) *StorageExecutor {
	t.Helper()
	ctx := context.Background()
	ns := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	nodeCount := edges / 5
	nodes := make([]*storage.Node, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodes = append(nodes, &storage.Node{ID: storage.NodeID(fmt.Sprintf("p%06d", i)), Labels: []string{"Endpoint"}, Properties: map[string]interface{}{"id": int64(i)}})
	}
	require.NoError(t, ns.BulkCreateNodes(ctx, nodes))
	batch := make([]*storage.Edge, 0, 5000)
	for i := 0; i < edges; i++ {
		batch = append(batch, &storage.Edge{
			ID:         storage.EdgeID(fmt.Sprintf("s%07d", i)),
			StartNode:  storage.NodeID(fmt.Sprintf("p%06d", i%nodeCount)),
			EndNode:    storage.NodeID(fmt.Sprintf("p%06d", (i*7+3)%nodeCount)),
			Type:       "AUTO",
			Properties: map[string]interface{}{"id": int64(i), "bucket": int64(i % 100)},
		})
		if len(batch) == cap(batch) {
			require.NoError(t, ns.BulkCreateEdges(ctx, batch))
			batch = make([]*storage.Edge, 0, 5000)
		}
	}
	if len(batch) > 0 {
		require.NoError(t, ns.BulkCreateEdges(ctx, batch))
	}
	return NewStorageExecutor(ns)
}

type closeoutL3RCFrame struct {
	name string
	ns   int64
}

func closeoutL3RCTop(p *profile.Profile, n int) (flat, cum []closeoutL3RCFrame, total int64) {
	idx := len(p.SampleType) - 1
	flatBy, cumBy := map[string]int64{}, map[string]int64{}
	for _, s := range p.Sample {
		v := s.Value[idx]
		total += v
		seen := map[string]bool{}
		for i, loc := range s.Location {
			for j, line := range loc.Line {
				if line.Function == nil {
					continue
				}
				name := line.Function.Name
				if i == 0 && j == 0 {
					flatBy[name] += v
				}
				if !seen[name] {
					seen[name] = true
					cumBy[name] += v
				}
			}
		}
	}
	order := func(m map[string]int64, keep func(string) bool) []closeoutL3RCFrame {
		var out []closeoutL3RCFrame
		for k, v := range m {
			if keep(k) {
				out = append(out, closeoutL3RCFrame{k, v})
			}
		}
		sort.Slice(out, func(a, b int) bool { return out[a].ns > out[b].ns })
		if len(out) > n {
			out = out[:n]
		}
		return out
	}
	flat = order(flatBy, func(string) bool { return true })
	cum = order(cumBy, func(k string) bool {
		return strings.Contains(k, "nornicdb/pkg/") && !strings.Contains(k, "testing.") && !strings.Contains(k, "zz_closeout")
	})
	return flat, cum, total
}

func closeoutL3RCProfile(t *testing.T, label string, run func()) {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, pprof.StartCPUProfile(&buf))
	run()
	pprof.StopCPUProfile()
	p, err := profile.ParseData(buf.Bytes())
	require.NoError(t, err)
	flat, cum, total := closeoutL3RCTop(p, 18)
	t.Logf("CLOSEOUT_L3_RC S7 profile %s totalCPU=%v", label, time.Duration(total))
	for _, f := range cum {
		t.Logf("CLOSEOUT_L3_RC S7 profile %s cum %5.1f%% %v %s", label, 100*float64(f.ns)/float64(total), time.Duration(f.ns), f.name)
	}
	for _, f := range flat {
		t.Logf("CLOSEOUT_L3_RC S7 profile %s flat %5.1f%% %v %s", label, 100*float64(f.ns)/float64(total), time.Duration(f.ns), f.name)
	}
	require.NotZero(t, total, "the profile recorded samples")
}

func TestCloseoutL3_RC_S7_NOR119_ScanVersusSet(t *testing.T) {
	ctx := context.Background()
	timed := func(t *testing.T, exec *StorageExecutor, q string) (time.Duration, int64) {
		start := time.Now()
		res, err := exec.Execute(ctx, q, nil)
		elapsed := time.Since(start)
		require.NoError(t, err, q)
		var n int64 = -1
		if len(res.Rows) == 1 && len(res.Rows[0]) == 1 {
			n = closeoutL3Int(res.Rows[0][0])
		}
		return elapsed, n
	}
	for _, size := range []int{25000, 50000, 100000} {
		size := size
		closeoutL3EachParser(t, func(t *testing.T) {
			exec := closeoutL3RCBuildRetireGraph(t, size)
			typeCount, n0 := timed(t, exec, "MATCH ()-[r:AUTO]->() RETURN count(r) AS c")
			scanNone, n1 := timed(t, exec, "MATCH ()-[r:AUTO]->() WHERE r.bucket = 1000 RETURN count(r) AS c")
			setNone, n2 := timed(t, exec, "MATCH ()-[r:AUTO]->() WHERE r.bucket = 1000 SET r.retired = true RETURN count(r) AS c")
			scanDense, n3 := timed(t, exec, "MATCH ()-[r:AUTO]->() WHERE r.bucket < 50 RETURN count(r) AS c")
			setDense, n4 := timed(t, exec, "MATCH ()-[r:AUTO]->() WHERE r.bucket < 50 SET r.retired = true RETURN count(r) AS c")
			t.Logf("CLOSEOUT_L3_RC S7 size=%d countByType=%v(%d) scanNoMatch=%v(%d) setNoMatch=%v(%d) scanDense=%v(%d) setDense=%v(%d) perRelScan=%v perUpdatedRelSet=%v",
				size, typeCount, n0, scanNone, n1, setNone, n2, scanDense, n3, setDense, n4,
				scanNone/time.Duration(size), (setDense-scanDense)/time.Duration(size/2))
			require.Equal(t, int64(0), n2)
			require.Equal(t, int64(size/2), n4)
			if size == 100000 {
				closeoutL3RCProfile(t, "set_no_match", func() {
					_, err := exec.Execute(ctx, "MATCH ()-[r:AUTO]->() WHERE r.bucket = 2000 SET r.retired2 = true RETURN count(r) AS c", nil)
					require.NoError(t, err)
				})
				closeoutL3RCProfile(t, "read_no_match", func() {
					_, err := exec.Execute(ctx, "MATCH ()-[r:AUTO]->() WHERE r.bucket = 2000 RETURN count(r) AS c", nil)
					require.NoError(t, err)
				})
				closeoutL3RCProfile(t, "set_dense", func() {
					_, err := exec.Execute(ctx, "MATCH ()-[r:AUTO]->() WHERE r.bucket >= 50 SET r.retired3 = true RETURN count(r) AS c", nil)
					require.NoError(t, err)
				})
			}
		})
	}
}
