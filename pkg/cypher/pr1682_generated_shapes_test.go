package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// The generated Record/Reading dataset in pr1682_reproduction_bench_test.go
// substitutes for the unavailable original NOR-214 application fixture. These
// tests assert the exact values produced by the two substituted query shapes
// (datetime label scan and CALL { UNWIND $ids ... MATCH ... } lookup) and by
// the fixture itself. They establish results for the substitute workload only;
// they cannot qualify the original application suite.

// TestPR1682GeneratedGraphFixture pins the deterministic dataset the substituted
// shapes run against: default sizes, documentation payload slices, and the
// hourly created_at sequence the datetime scan window depends on.
func TestPR1682GeneratedGraphFixture(t *testing.T) {
	graph := newPR1682GeneratedGraph(t, pr1682GeneratedReadings, pr1682GeneratedRecords)
	ctx := context.Background()

	readings, err := graph.exec.Execute(ctx, "MATCH (r:Reading) RETURN count(r) AS readings", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(pr1682GeneratedReadings)}}, readings.Rows)

	records, err := graph.exec.Execute(ctx, "MATCH (d:Record) RETURN count(d) AS records", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(pr1682GeneratedRecords)}}, records.Rows)

	content, err := graph.exec.Execute(ctx, "MATCH (r:Reading {id: 'reading-0'}) RETURN r.content AS content", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{string(graph.doc[0:1024])}}, content.Rows)

	// Reading 168 is the first instant inside the scan window; reading 167 is
	// the last outside it.
	inWindow, err := graph.exec.Execute(ctx, "MATCH (r:Reading {id: 'reading-168'}) RETURN r.created_at >= datetime('2026-01-08T00:00:00Z') AND r.created_at < datetime('2026-01-08T01:00:00Z') AS in_window", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true}}, inWindow.Rows)

	beforeWindow, err := graph.exec.Execute(ctx, "MATCH (r:Reading {id: 'reading-167'}) RETURN r.created_at >= datetime('2026-01-08T00:00:00Z') AND r.created_at < datetime('2026-01-08T01:00:00Z') AS in_window", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{false}}, beforeWindow.Rows)
}

// TestPR1682GeneratedGraphRetainedScopedCallResult pins the result the retained
// optional CALL benchmark (BenchmarkPR1682GeneratedGraphScopedCall) asserts, so
// the shared fixture keeps satisfying that benchmark after the fixture refactor.
func TestPR1682GeneratedGraphRetainedScopedCallResult(t *testing.T) {
	graph := newPR1682GeneratedGraph(t, pr1682GeneratedReadings, pr1682GeneratedRecords)

	query := "MATCH (d:Record {id: 7}) WHERE 1 = 1 "
	for n := 0; n < 20; n++ {
		query += fmt.Sprintf("CALL (d) { OPTIONAL MATCH (n:Reading {id: d.reading_id}) WITH collect(n.id) AS ids RETURN ids AS ids%d } ", n)
	}
	query += "RETURN d.reading_id AS id, ids19 AS ids"

	result, err := graph.exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 2)
	require.Equal(t, "reading-35", result.Rows[0][0])
	require.Equal(t, "[reading-35]", fmt.Sprint(result.Rows[0][1]))
}

// TestPR1682GeneratedDatetimeScanShape asserts the exact substituted datetime
// label scan result for a fresh statement text and for a repeated one. Each
// variant runs twice so the warmed case exercises statement reuse.
func TestPR1682GeneratedDatetimeScanShape(t *testing.T) {
	graph := newPR1682GeneratedGraph(t, pr1682GeneratedReadings, pr1682GeneratedRecords)
	ctx := context.Background()

	for _, test := range []struct {
		name     string
		distinct bool
	}{
		{name: "cold distinct statement", distinct: true},
		{name: "warmed repeated statement"},
	} {
		t.Run(test.name, func(t *testing.T) {
			for run := 0; run < 2; run++ {
				result, err := graph.exec.Execute(ctx, pr1682GeneratedDatetimeScanQuery(test.distinct), nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{
					int64(pr1682GeneratedScanRows),
					pr1682GeneratedScanFirst,
					pr1682GeneratedScanLast,
				}}, result.Rows)
			}
		})
	}
}

// TestPR1682GeneratedLookupShape asserts the exact substituted
// CALL { UNWIND $ids ... MATCH ... } lookup result for a fresh statement text
// and for a repeated one. Each variant runs twice so the warmed case exercises
// statement reuse.
func TestPR1682GeneratedLookupShape(t *testing.T) {
	graph := newPR1682GeneratedGraph(t, pr1682GeneratedReadings, pr1682GeneratedRecords)
	ctx := context.Background()
	ids := pr1682GeneratedLookupIDs()
	params := map[string]interface{}{"ids": ids}

	want := make(map[string]bool, len(ids))
	for _, id := range ids {
		want[id.(string)] = true
	}

	for _, test := range []struct {
		name     string
		distinct bool
	}{
		{name: "cold distinct statement", distinct: true},
		{name: "warmed repeated statement"},
	} {
		t.Run(test.name, func(t *testing.T) {
			for run := 0; run < 2; run++ {
				result, err := graph.exec.Execute(ctx, pr1682GeneratedLookupQuery(test.distinct), params)
				require.NoError(t, err)
				require.Len(t, result.Rows, len(ids))
				got := make(map[string]bool, len(result.Rows))
				for _, row := range result.Rows {
					require.Len(t, row, 2)
					id, ok := row[0].(string)
					require.True(t, ok, "lookup row id type: %#v", row[0])
					require.Equal(t, "repository documentation", row[1])
					got[id] = true
				}
				require.Equal(t, want, got)
			}
		})
	}
}
