package nornicdb

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// The startup search build's summary counts every outcome the same way
// however the background build and the database's shutdown interleave.
func TestSummarizeSearchBuilds(t *testing.T) {
	failure := errors.New("forced build failure")
	for _, tc := range []struct {
		name     string
		canceled bool
		dbCount  int
		results  []searchBuildResult
		want     searchBuildSummary
	}{
		{"one built", false, 1, []searchBuildResult{{dbName: "nornic", outcome: "built"}}, searchBuildSummary{built: 1}},
		{"several built", false, 2, []searchBuildResult{{dbName: "a", outcome: "built"}, {dbName: "b", outcome: "built"}}, searchBuildSummary{built: 2}},
		{"built, skipped and deferred", false, 3, []searchBuildResult{{dbName: "a", outcome: "built"}, {dbName: "b", outcome: "skipped"}, {dbName: "c", outcome: "deferred"}}, searchBuildSummary{built: 1, skipped: 1, deferred: 1}},
		{"all skipped", false, 2, []searchBuildResult{{dbName: "a", outcome: "skipped"}, {dbName: "b", outcome: "skipped"}}, searchBuildSummary{skipped: 2}},
		{"all deferred", false, 2, []searchBuildResult{{dbName: "a", outcome: "deferred"}, {dbName: "b", outcome: "deferred"}}, searchBuildSummary{deferred: 2}},
		{"skipped and deferred", false, 2, []searchBuildResult{{dbName: "a", outcome: "skipped"}, {dbName: "b", outcome: "deferred"}}, searchBuildSummary{skipped: 1, deferred: 1}},
		{"one failed", false, 2, []searchBuildResult{{dbName: "a", outcome: "built"}, {dbName: "b", err: failure}}, searchBuildSummary{built: 1, failed: 1}},
		{"canceled by shutdown", true, 1, []searchBuildResult{{dbName: "nornic", err: context.Canceled}}, searchBuildSummary{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			results := make(chan searchBuildResult, len(tc.results))
			for _, result := range tc.results {
				results <- result
			}
			close(results)
			require.Equal(t, tc.want, summarizeSearchBuilds(ctx, tc.dbCount, results))
		})
	}
}
