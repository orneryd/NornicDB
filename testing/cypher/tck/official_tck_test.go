package tck

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cucumber/godog"
)

func TestOfficialOpenCypherCorpusInBothTransactionModes(t *testing.T) {
	if os.Getenv("NORNICDB_RUN_FULL_TCK") != "1" {
		t.Skip("full openCypher corpus runs through make cypher-tck or make cypher-conformance")
	}

	featuresRoot := filepath.Join("testdata", "opencypher", "features")
	inventory, err := BuildInventory(featuresRoot)
	if err != nil {
		t.Fatalf("inventory official corpus: %v", err)
	}
	if inventory.Scenarios != 3897 {
		t.Fatalf("expanded scenario count changed: got %d, want 3897", inventory.Scenarios)
	}

	for _, mode := range []TransactionMode{AutocommitMode, ExplicitTransactionMode} {
		t.Run(string(mode), func(t *testing.T) {
			driver, shutdown := startConformanceServer(t)
			defer shutdown()
			suite := godog.TestSuite{
				Name: "openCypher TCK " + string(mode),
				ScenarioInitializer: func(scenario *godog.ScenarioContext) {
					RegisterSteps(scenario, func(context.Context) (Backend, error) {
						return NewBoltBackend(BoltBackendConfig{
							Driver:          driver,
							DatabaseName:    "nornic",
							Mode:            mode,
							NamedGraphsRoot: filepath.Join("testdata", "opencypher", "graphs"),
						})
					})
				},
				Options: &godog.Options{
					Format:      "progress",
					NoColors:    true,
					Paths:       []string{featuresRoot},
					Concurrency: 1,
					TestingT:    t,
				},
			}
			if status := suite.Run(); status != 0 {
				t.Fatalf("official openCypher corpus failed in %s mode with status %d", mode, status)
			}
		})
	}
}
