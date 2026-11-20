package server

// NOR-287 remaining consumer audit: composite and Heimdall count consumers.
//
// Scope of this file (no production change):
//   - GET /db/{composite}   -> compositeConstituentStats (server_db.go:447-519)
//   - Heimdall Stats        -> heimdallDBRouter.Stats (server_heimdall.go:325-336)
//
// The composite route documents its failure marker: statsPartial is true when
// one or more constituent stats could not be collected, and statsProvenance
// carries per-constituent reachability and error
// (docs/architecture/multi-db-architecture.md:183-188,
// docs/user-guides/multi-database.md:796-804). This test asserts that documented
// marker for a real constituent count failure, so the composite consumer is
// failure-observable and is not part of the silent-zero class.
//
// The Heimdall router refuses a composite root before any count read
// (requireNonComposite), so the composite admission skip cannot reach it. Its
// native count read discards the error; this test pins that behaviour, and the
// actual caller inventory in nor287_heimdall_routerstats_inventory_test.go
// records four bundled production consumers. The separate error-contract test
// asserts that those error-aware callers must be able to observe count failures.

import (
	"net/http"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// nor287CompositeFixture creates two local constituents named after prefix and a
// composite over them in the faulted manager.
func nor287CompositeFixture(t *testing.T, manager *multidb.DatabaseManager, composite, prefix string, nodesA, nodesB int) {
	t.Helper()
	constituentA, constituentB := prefix+"_a", prefix+"_b"
	nor287CreateDatabaseWithNodes(t, manager, constituentA, nodesA)
	nor287CreateDatabaseWithNodes(t, manager, constituentB, nodesB)
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), composite, []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: constituentA, Type: "local", AccessMode: "read_write"},
		{Alias: "b", DatabaseName: constituentB, Type: "local", AccessMode: "read_write"},
	}))
}

// nor287CompositeProvenance returns the statsProvenance rows.
func nor287CompositeProvenance(t *testing.T, payload map[string]any) []map[string]any {
	t.Helper()
	raw, ok := payload["statsProvenance"].([]any)
	require.True(t, ok, "composite database info must report statsProvenance")
	rows := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		row, ok := item.(map[string]any)
		require.True(t, ok)
		rows = append(rows, row)
	}
	return rows
}

// nor287SortedCopy returns the alias-sorted copy used by the ordering contract.
func nor287SortedCopy(aliases []string) []string {
	sorted := append([]string(nil), aliases...)
	sort.Strings(sorted)
	return sorted
}

// TestNOR287CompositeStatsPartialReportsConstituentCountFailure audits the
// documented composite provenance contract when a constituent count read fails.
//
// server_db.go:484-496 already treats a constituent NodeCount/EdgeCount error as
// partial and unreachable. This test proves the observable contract over the
// real route with real stored nodes: the aggregate drops the constituent and the
// client is told which constituent could not be read.
func TestNOR287CompositeStatsPartialReportsConstituentCountFailure(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	manager, setFailure := nor287FaultedDatabaseManager(t, server)

	info := func(t *testing.T, name string) map[string]any {
		t.Helper()
		response := makeRequest(t, server, http.MethodGet, "/db/"+name, nil, token)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		return nor287DecodeJSON(t, response.Body.Bytes())
	}

	nor287CompositeFixture(t, manager, "nor287_cmp_empty", "nor287_cmp_empty", 0, 0)
	nor287CompositeFixture(t, manager, "nor287_cmp_counts", "nor287_cmp_counts", 2, 1)

	t.Run("healthy empty composite is not partial", func(t *testing.T) {
		payload := info(t, "nor287_cmp_empty")
		require.Equal(t, "composite", payload["type"])
		require.Equal(t, "constituent_sum", payload["statsAggregation"])
		require.Equal(t, false, payload["statsPartial"])
		require.Equal(t, float64(0), payload["nodeCount"])
		rows := nor287CompositeProvenance(t, payload)
		require.Len(t, rows, 2)
		for _, row := range rows {
			require.Equal(t, true, row["reachable"])
			require.NotContains(t, row, "error")
		}
	})

	t.Run("healthy non-empty composite sums each constituent", func(t *testing.T) {
		payload := info(t, "nor287_cmp_counts")
		require.Equal(t, false, payload["statsPartial"])
		require.Equal(t, float64(3), payload["nodeCount"])
		aliases := make([]string, 0, 2)
		perAlias := make(map[string]float64, 2)
		for _, row := range nor287CompositeProvenance(t, payload) {
			alias := row["alias"].(string)
			aliases = append(aliases, alias)
			perAlias[alias] = row["nodeCount"].(float64)
		}
		require.Equal(t, nor287SortedCopy(aliases), aliases, "provenance stays alias-sorted")
		require.Equal(t, float64(2), perAlias["a"])
		require.Equal(t, float64(1), perAlias["b"])
	})

	t.Run("failed constituent count is observable through statsPartial", func(t *testing.T) {
		setFailure(storagefault.NodeCountRead)
		payload := info(t, "nor287_cmp_counts")
		require.Equal(t, true, payload["statsPartial"], "the documented partial marker must report the failed constituent")
		require.Equal(t, float64(0), payload["nodeCount"], "a failed constituent contributes no count")
		rows := nor287CompositeProvenance(t, payload)
		require.Len(t, rows, 2)
		for _, row := range rows {
			require.Equal(t, false, row["reachable"])
			require.Contains(t, row["error"], errNOR287CountReadFailure.Error())
			require.Equal(t, float64(0), row["nodeCount"])
		}
	})

	setFailure("")
	t.Run("healthy answer returns when the read owner recovers", func(t *testing.T) {
		payload := info(t, "nor287_cmp_counts")
		require.Equal(t, false, payload["statsPartial"])
		require.Equal(t, float64(3), payload["nodeCount"])
	})
}

// TestNOR287HeimdallStatsRefusesCompositeAndZeroesNativeFailure audits
// heimdallDBRouter.Stats (server_heimdall.go:325-336).
//
// requireNonComposite refuses a composite root, so a denied or failing
// constituent cannot be reported as a composite-wide zero. The native count
// read discards both errors, so a failed native read is reported as a
// successful zero to this interface's caller.
func TestNOR287HeimdallStatsRefusesCompositeAndZeroesNativeFailure(t *testing.T) {
	server, _ := setupTestServer(t)
	manager, setFailure := nor287FaultedDatabaseManager(t, server)

	nor287CreateDatabaseWithNodes(t, manager, "nor287_heimdall_empty", 0)
	nor287CreateDatabaseWithNodes(t, manager, "nor287_heimdall_nodes", 3)
	nor287CompositeFixture(t, manager, "nor287_heimdall_cmp", "nor287_heimdall_cmp", 2, 1)

	router := newHeimdallDBRouter(server.db, server.dbManager, nil)
	ctx := heimdallAuthDisabled()

	t.Run("healthy empty database reports zero", func(t *testing.T) {
		stats, err := router.Stats(ctx, "nor287_heimdall_empty")
		require.NoError(t, err)
		require.Equal(t, int64(0), stats.NodeCount)
		require.Equal(t, int64(0), stats.RelationshipCount)
	})

	t.Run("healthy non-empty database reports every node", func(t *testing.T) {
		stats, err := router.Stats(ctx, "nor287_heimdall_nodes")
		require.NoError(t, err)
		require.Equal(t, int64(3), stats.NodeCount)
	})

	t.Run("composite root is refused before any count read", func(t *testing.T) {
		_, err := router.Stats(ctx, "nor287_heimdall_cmp")
		require.Error(t, err, "a composite root must not be answered with an aggregate or a zero")
		require.Contains(t, err.Error(), "composite database")
	})

	t.Run("failed native count is reported as a zero without error", func(t *testing.T) {
		setFailure(storagefault.NodeCountRead)
		stats, err := router.Stats(ctx, "nor287_heimdall_nodes")
		require.NoError(t, err)
		// Three nodes are stored; the interface answer carries no failure
		// signal. This diagnostic records the bad baseline; the separate
		// TestNOR287HeimdallStatsMustPropagateNativeCountFailure is the red
		// contract witness for the bundled production consumers.
		require.Equal(t, int64(0), stats.NodeCount)
	})

	setFailure("")
	t.Run("count recovers when the read owner recovers", func(t *testing.T) {
		stats, err := router.Stats(ctx, "nor287_heimdall_nodes")
		require.NoError(t, err)
		require.Equal(t, int64(3), stats.NodeCount)
	})
}
