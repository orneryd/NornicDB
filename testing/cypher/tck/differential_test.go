package tck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type differentialCase struct {
	Name    string   `json:"name"`
	Setup   []string `json:"setup"`
	Query   string   `json:"query"`
	Ordered bool     `json:"ordered"`
}

func TestFixedDifferentialCorpusMatchesPinnedNeo4j(t *testing.T) {
	referenceURI := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if referenceURI == "" {
		t.Skip("set NORNICDB_NEO4J_REFERENCE_URI to run the pinned Neo4j differential corpus")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	referenceDriver, err := neo4j.NewDriverWithContext(referenceURI, neo4j.NoAuth())
	if err != nil {
		t.Fatalf("create Neo4j reference driver: %v", err)
	}
	defer func() {
		if err := referenceDriver.Close(context.Background()); err != nil {
			t.Errorf("close Neo4j reference driver: %v", err)
		}
	}()
	if err := waitForReference(ctx, referenceDriver); err != nil {
		t.Fatalf("connect to pinned Neo4j reference: %v", err)
	}

	cases := loadDifferentialCases(t)
	for _, mode := range []TransactionMode{AutocommitMode, ExplicitTransactionMode} {
		t.Run(string(mode), func(t *testing.T) {
			nornicDriver, shutdown := startConformanceServer(t)
			defer shutdown()
			nornic := newDifferentialBackend(t, nornicDriver, "nornic", mode)
			reference := newDifferentialBackend(t, referenceDriver, "neo4j", mode)

			for _, testCase := range cases {
				t.Run(testCase.Name, func(t *testing.T) {
					resetDifferentialBackend(t, ctx, nornic, "NornicDB")
					resetDifferentialBackend(t, ctx, reference, "Neo4j")
					for _, setup := range testCase.Setup {
						executeDifferentialSetup(t, ctx, nornic, "NornicDB", setup)
						executeDifferentialSetup(t, ctx, reference, "Neo4j", setup)
					}

					nornicBefore := snapshotDifferentialBackend(t, ctx, nornic, "NornicDB")
					referenceBefore := snapshotDifferentialBackend(t, ctx, reference, "Neo4j")
					nornicResult, nornicErr := nornic.Execute(ctx, testCase.Query, nil)
					referenceResult, referenceErr := reference.Execute(ctx, testCase.Query, nil)
					compareDifferentialErrors(t, nornicErr, referenceErr)
					if referenceErr != nil {
						return
					}
					if err := CompareResults(nornicResult, referenceResult, testCase.Ordered, false); err != nil {
						t.Fatalf("result differs from Neo4j: %v", err)
					}

					nornicAfter := snapshotDifferentialBackend(t, ctx, nornic, "NornicDB")
					referenceAfter := snapshotDifferentialBackend(t, ctx, reference, "Neo4j")
					nornicEffects, err := ObserveSideEffects(nornicBefore, nornicAfter)
					if err != nil {
						t.Fatalf("observe NornicDB side effects: %v", err)
					}
					referenceEffects, err := ObserveSideEffects(referenceBefore, referenceAfter)
					if err != nil {
						t.Fatalf("observe Neo4j side effects: %v", err)
					}
					if !reflect.DeepEqual(nornicEffects, referenceEffects) {
						t.Fatalf("side effects differ: NornicDB %#v, Neo4j %#v", nornicEffects, referenceEffects)
					}
				})
			}
		})
	}
}

func loadDifferentialCases(t *testing.T) []differentialCase {
	t.Helper()
	path := filepath.Join("testdata", "differential", "cases.json")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read differential corpus: %v", err)
	}
	var cases []differentialCase
	if err := json.Unmarshal(content, &cases); err != nil {
		t.Fatalf("decode differential corpus: %v", err)
	}
	if len(cases) == 0 {
		t.Fatal("differential corpus is empty")
	}
	for index, testCase := range cases {
		if testCase.Name == "" || testCase.Query == "" {
			t.Fatalf("differential case %d requires name and query", index)
		}
	}
	return cases
}

func newDifferentialBackend(t *testing.T, driver neo4j.DriverWithContext, database string, mode TransactionMode) *BoltBackend {
	t.Helper()
	backend, err := NewBoltBackend(BoltBackendConfig{Driver: driver, DatabaseName: database, Mode: mode})
	if err != nil {
		t.Fatalf("create %s backend: %v", database, err)
	}
	return backend
}

func resetDifferentialBackend(t *testing.T, ctx context.Context, backend *BoltBackend, name string) {
	t.Helper()
	if err := backend.Reset(ctx); err != nil {
		t.Fatalf("reset %s: %v", name, err)
	}
}

func executeDifferentialSetup(t *testing.T, ctx context.Context, backend *BoltBackend, name, query string) {
	t.Helper()
	if _, err := backend.Execute(ctx, query, nil); err != nil {
		t.Fatalf("execute %s setup: %v", name, err)
	}
}

func snapshotDifferentialBackend(t *testing.T, ctx context.Context, backend *BoltBackend, name string) GraphSnapshot {
	t.Helper()
	snapshot, err := backend.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot %s: %v", name, err)
	}
	return snapshot
}

func compareDifferentialErrors(t *testing.T, nornicErr, referenceErr error) {
	t.Helper()
	if nornicErr == nil && referenceErr == nil {
		return
	}
	if nornicErr == nil || referenceErr == nil {
		t.Fatalf("error behavior differs: NornicDB %v, Neo4j %v", nornicErr, referenceErr)
	}
	var nornicQueryErr, referenceQueryErr *QueryError
	if !errors.As(nornicErr, &nornicQueryErr) || !errors.As(referenceErr, &referenceQueryErr) {
		t.Fatalf("unclassified differential errors: NornicDB %T %v, Neo4j %T %v", nornicErr, nornicErr, referenceErr, referenceErr)
	}
	if nornicQueryErr.Type != referenceQueryErr.Type || nornicQueryErr.Phase != referenceQueryErr.Phase {
		t.Fatalf("error classification differs: NornicDB %s/%s, Neo4j %s/%s", nornicQueryErr.Type, nornicQueryErr.Phase, referenceQueryErr.Type, referenceQueryErr.Phase)
	}
}

func waitForReference(ctx context.Context, driver neo4j.DriverWithContext) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		verifyCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		lastErr = driver.VerifyConnectivity(verifyCtx)
		cancel()
		if lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %v", ctx.Err(), lastErr)
		case <-ticker.C:
		}
	}
}
