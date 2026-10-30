//go:build integration
// +build integration

package cypher

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestRealData_SimpleMatchLimitFastPathTrace(t *testing.T) {
	exec, cleanup := openRealDataIntegrationExecutor(t, "caremark_translation")
	defer cleanup()

	cases := []int{5, 10, 25}
	for _, limit := range cases {
		t.Run(fmt.Sprintf("limit_%d", limit), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			query := fmt.Sprintf("MATCH (n) RETURN n LIMIT %d /* trace_cache_bust_%d */", limit, limit)
			res, err := exec.Execute(ctx, query, nil)
			if err != nil {
				t.Fatalf("execute failed: %v", err)
			}
			if res == nil {
				t.Fatalf("expected non-nil result")
			}
			if len(res.Rows) > limit {
				t.Fatalf("returned %d rows, expected <= %d", len(res.Rows), limit)
			}

			trace := exec.LastHotPathTrace()
			if !trace.SimpleMatchLimitFastPath {
				t.Fatalf("expected SimpleMatchLimitFastPath trace=true, got %+v", trace)
			}
		})
	}
}

func openRealDataIntegrationExecutor(t *testing.T, databaseName string) (*StorageExecutor, func()) {
	t.Helper()
	if os.Getenv("INTEGRATION_TEST") == "" {
		t.Skip("Set INTEGRATION_TEST=1 to run real-data integration test")
	}

	dataDir := strings.TrimSpace(os.Getenv("NORNICDB_REAL_DATA_DIR"))
	if dataDir == "" {
		dataDir = "/usr/local/var/nornicdb/data"
	}
	if _, err := os.Stat(dataDir); err != nil {
		t.Skipf("real data dir unavailable: %s (%v)", dataDir, err)
	}

	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dataDir))
	closeBase := func() {}
	if base != nil {
		_, closeBase = fixtureowner.OwnWithClose(t, base)
	}
	if err != nil {
		t.Fatalf("open badger real data: %v", err)
	}
	store := storage.NewNamespacedEngine(base, databaseName)
	exec := NewStorageExecutor(store)

	return exec, closeBase
}
