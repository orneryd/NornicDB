package tck

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
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
	ratchetPath := filepath.Join("testdata", "ratchet.json")
	ratchetMode := os.Getenv("NORNICDB_TCK_RATCHET") == "1"
	updateRatchet := os.Getenv("NORNICDB_TCK_UPDATE_RATCHET") == "1"
	vettedMode := os.Getenv("NORNICDB_TCK_VETTED") == "1"
	if updateRatchet {
		ratchetMode = true
	}
	if (ratchetMode || vettedMode) && strings.TrimSpace(os.Getenv("NORNICDB_TCK_PATHS")) != "" {
		t.Fatal("ratchet and vetted runs require the checked-in corpus scope; NORNICDB_TCK_PATHS is not allowed")
	}
	if (ratchetMode || vettedMode) && strings.TrimSpace(os.Getenv("NORNICDB_TCK_MODE")) != "" {
		t.Fatal("ratchet and vetted runs require both transaction modes; NORNICDB_TCK_MODE is not allowed")
	}
	requestedPaths := []string{featuresRoot}
	if vettedMode {
		baseline, loadErr := loadRatchetBaseline(ratchetPath)
		if loadErr != nil {
			t.Fatalf("load vetted TCK feature manifest: %v", loadErr)
		}
		requestedPaths = make([]string, 0, len(baseline.VettedFeatures))
		for _, path := range baseline.VettedFeatures {
			requestedPaths = append(requestedPaths, filepath.Join(featuresRoot, filepath.FromSlash(path)))
		}
		if len(requestedPaths) == 0 {
			t.Fatal("vetted TCK feature manifest is empty")
		}
	} else if rawPaths := strings.TrimSpace(os.Getenv("NORNICDB_TCK_PATHS")); rawPaths != "" {
		requestedPaths = nil
		for _, path := range strings.Split(rawPaths, ",") {
			requestedPaths = append(requestedPaths, strings.TrimSpace(path))
		}
	} else if inventory.Scenarios != 3897 {
		t.Fatalf("expanded scenario count changed: got %d, want 3897", inventory.Scenarios)
	}

	modes := []TransactionMode{AutocommitMode, ExplicitTransactionMode}
	if requestedMode := TransactionMode(os.Getenv("NORNICDB_TCK_MODE")); requestedMode != "" {
		modes = []TransactionMode{requestedMode}
	}
	allEntries := make([]RatchetEntry, 0, inventory.Scenarios*len(modes))
	for _, mode := range modes {
		if mode != AutocommitMode && mode != ExplicitTransactionMode {
			t.Fatalf("invalid NORNICDB_TCK_MODE %q", mode)
		}
		t.Run(string(mode), func(t *testing.T) {
			driver, shutdown := startConformanceServer(t)
			defer shutdown()
			var recorder *ratchetRecorder
			if ratchetMode {
				recorder = newRatchetRecorder(mode)
			}
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
					if recorder != nil {
						scenario.After(func(runCtx context.Context, executed *godog.Scenario, scenarioErr error) (context.Context, error) {
							recorder.record(executed, scenarioErr)
							return runCtx, nil
						})
					}
				},
				Options: &godog.Options{
					Format:      "progress",
					NoColors:    true,
					Paths:       requestedPaths,
					Concurrency: 1,
				},
			}
			if ratchetMode {
				suite.Options.Output = io.Discard
			} else {
				suite.Options.TestingT = t
			}
			status := suite.Run()
			if recorder != nil {
				allEntries = append(allEntries, recorder.snapshot()...)
			}
			if !ratchetMode && status != 0 {
				t.Fatalf("official openCypher corpus failed in %s mode with status %d", mode, status)
			}
		})
	}
	if !ratchetMode {
		return
	}

	current := newRatchetBaseline(inventory.CorpusSHA256, allEntries)
	if len(current.Entries) != inventory.Scenarios*2 {
		t.Fatalf("ratchet recorded %d scenario/mode outcomes, want %d", len(current.Entries), inventory.Scenarios*2)
	}
	if updateRatchet {
		if baseline, loadErr := loadRatchetBaseline(ratchetPath); loadErr == nil {
			if err := validateRatchetUpdate(baseline, current); err != nil {
				t.Fatal(err)
			}
		} else if !errors.Is(loadErr, os.ErrNotExist) {
			t.Fatalf("load existing TCK ratchet: %v", loadErr)
		}
		if err := writeRatchetBaseline(ratchetPath, current); err != nil {
			t.Fatal(err)
		}
		t.Logf("updated TCK ratchet: %s; fully-vetted-features=%d", summarizeRatchet(current.Entries), len(current.VettedFeatures))
		return
	}

	baseline, err := loadRatchetBaseline(ratchetPath)
	if err != nil {
		t.Fatalf("load TCK ratchet: %v", err)
	}
	report, err := compareRatchet(baseline, current)
	percentage := 0.0
	if report.Total != 0 {
		percentage = float64(report.SupportedPass) * 100 / float64(report.Total)
	}
	t.Logf("TCK ratchet: %s pass=%.4f%% fully-vetted-features=%d", report, percentage, len(current.VettedFeatures))
	if err != nil {
		t.Fatal(err)
	}
}
