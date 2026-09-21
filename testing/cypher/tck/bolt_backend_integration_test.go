package tck

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/cucumber/godog"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type conformanceDatabaseManager struct {
	store storage.Engine
}

func (m *conformanceDatabaseManager) GetStorage(name string) (storage.Engine, error) {
	if name != "nornic" {
		return nil, fmt.Errorf("database %q does not exist", name)
	}
	return m.store, nil
}

func (m *conformanceDatabaseManager) Exists(name string) bool { return name == "nornic" }

func (m *conformanceDatabaseManager) DefaultDatabaseName() string { return "nornic" }

func TestBoltBackendRunsBehaviorChecksInBothTransactionModes(t *testing.T) {
	for _, mode := range []TransactionMode{AutocommitMode, ExplicitTransactionMode} {
		t.Run(string(mode), func(t *testing.T) {
			driver, shutdown := startConformanceServer(t)
			defer shutdown()
			feature := `Feature: production Bolt adapter
  Scenario: typed results are fully consumed
    Given an empty graph
    When executing query:
      """
      RETURN 1 AS value
      """
    Then the result should be, in order:
      | value |
      | 1     |
    And no side effects

  Scenario: named graph fixtures are committed before the statement
    Given the binary-tree-1 graph
    When executing query:
      """
      MATCH (n) RETURN count(n) AS count
      """
    Then the result should be, in order:
      | count |
      | 13    |
    And no side effects

  Scenario: failed writes leave the committed graph unchanged
    Given an empty graph
    And having executed:
      """
      CREATE (:Account {id: 1})
      """
    When executing query:
      """
      MATCH (a:Account {id: 1}) SET a.transient = 1, a += {broken:} RETURN a
      """
    Then an Error should be raised at any time: *

  Scenario: successful writes are committed
    Given an empty graph
    When executing query:
      """
      CREATE (:Person {name: 'Alice'})
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |
      | +labels     | 1 |
`
			status, output := runHarnessContract(feature, func(ctx *godog.ScenarioContext) {
				RegisterSteps(ctx, func(context.Context) (Backend, error) {
					return NewBoltBackend(BoltBackendConfig{
						Driver:          driver,
						DatabaseName:    "nornic",
						Mode:            mode,
						NamedGraphsRoot: filepath.Join("testdata", "opencypher", "graphs"),
					})
				})
			})
			if status != 0 {
				t.Fatalf("Bolt adapter failed with status %d:\n%s", status, output)
			}

			verification, err := NewBoltBackend(BoltBackendConfig{
				Driver: driver, DatabaseName: "nornic", Mode: AutocommitMode,
			})
			if err != nil {
				t.Fatalf("NewBoltBackend() error = %v", err)
			}
			result, err := verification.Execute(context.Background(),
				"MATCH (p:Person {name: 'Alice'}) RETURN count(p) AS count", nil)
			if err != nil {
				t.Fatalf("verify commit: %v", err)
			}
			if len(result.Rows) != 1 || result.Rows[0][0] != int64(1) {
				t.Fatalf("committed statement is not visible: %#v", result.Rows)
			}
		})
	}
}

func startConformanceServer(t *testing.T) (neo4j.DriverWithContext, func()) {
	t.Helper()
	engine, err := storage.NewBadgerEngine(t.TempDir())
	if err != nil {
		t.Fatalf("create Badger engine: %v", err)
	}
	store := storage.NewNamespacedEngine(engine, "nornic")
	manager := &conformanceDatabaseManager{store: store}
	config := bolt.DefaultConfig()
	config.Host = "127.0.0.1"
	config.Port = 0
	config.MaxConnections = 8
	server := bolt.NewWithDatabaseManager(config, nil, manager)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = engine.Close()
		t.Fatalf("listen for Bolt server: %v", err)
	}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(listener) }()

	driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
	if err != nil {
		_ = server.Close()
		_ = engine.Close()
		t.Fatalf("create Bolt driver: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := driver.VerifyConnectivity(ctx); err != nil {
		_ = driver.Close(ctx)
		_ = server.Close()
		_ = engine.Close()
		t.Fatalf("verify Bolt connectivity: %v", err)
	}

	return driver, func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := driver.Close(closeCtx); err != nil {
			t.Errorf("close Bolt driver: %v", err)
		}
		if err := server.Close(); err != nil {
			t.Errorf("close Bolt server: %v", err)
		}
		select {
		case err := <-serverDone:
			if err != nil {
				t.Errorf("serve Bolt: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Bolt server did not stop")
		}
		if err := engine.Close(); err != nil {
			t.Errorf("close Badger engine: %v", err)
		}
	}
}
