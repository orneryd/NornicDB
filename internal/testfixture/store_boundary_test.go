package testfixture_test

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	fixture "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestMain(m *testing.M) { os.Exit(fixture.Run(m, "testfixture")) }

func TestStoreBoundaryPreservesFailuresAndChecksActualPopulation(t *testing.T) {
	boundary := fixture.BeginStoreBoundary()
	var output bytes.Buffer
	if code := boundary.Check(7, &output, "boundary-control"); code != 7 || output.Len() != 0 {
		t.Fatalf("changed original test result: code=%d output=%s", code, &output)
	}
	t.Run("live_owned_store", func(t *testing.T) {
		_ = fixture.Own(t, fixture.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
		if code := boundary.Check(0, &output, "boundary-control"); code != 1 || !strings.Contains(output.String(), "boundary-control: 1 Badger store(s) left open after tests") {
			t.Fatalf("live store escaped boundary: code=%d output=%s", code, &output)
		}
	})
	output.Reset()
	if code := boundary.Check(0, &output, "boundary-control"); code != 0 || output.Len() != 0 {
		t.Fatalf("owned cleanup left a store open: code=%d output=%s", code, &output)
	}
}

type hiddenStoreFactory struct{}
type hiddenStoreAlias = *storage.MemoryEngine

func hiddenStoreError() error                         { _ = storage.NewMemoryEngine(); return errors.New("recovery failed") }
func hiddenStoreInterface() storage.Engine            { return storage.NewMemoryEngine() }
func hiddenStoreAliased() hiddenStoreAlias            { return storage.NewMemoryEngine() }
func hiddenStoreSecond() (int, *storage.MemoryEngine) { return 0, storage.NewMemoryEngine() }
func (hiddenStoreFactory) open() storage.Engine       { return storage.NewMemoryEngine() }

// These allocations never enter Track and some never reach a result. The real
// TestMain, rather than declaration spelling or registration, must reject them.
func TestStoreBoundaryRejectsHiddenAllocation(t *testing.T) {
	for _, shape := range []string{"error", "interface", "alias", "second", "method", "closure", "owned"} {
		t.Run(shape, func(t *testing.T) {
			command := exec.Command(os.Args[0], "-test.run=^TestStoreBoundaryHiddenAllocationChild$", "-test.count=1")
			command.Env = append(os.Environ(), "NORNIC_TEST_STORE_BOUNDARY="+shape)
			output, err := command.CombinedOutput()
			if shape == "owned" {
				if err != nil {
					t.Fatalf("owned allocation rejected: %v\n%s", err, output)
				}
				return
			}
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(string(output), "testfixture: 1 Badger store(s) left open after tests") {
				t.Fatalf("hidden %s allocation escaped TestMain: %v\n%s", shape, err, output)
			}
		})
	}
}

func TestStoreBoundaryHiddenAllocationChild(t *testing.T) {
	switch os.Getenv("NORNIC_TEST_STORE_BOUNDARY") {
	case "error":
		if err := hiddenStoreError(); err == nil {
			t.Fatal("expected recovery error")
		}
	case "interface":
		_ = hiddenStoreInterface()
	case "alias":
		_ = hiddenStoreAliased()
	case "second":
		_, _ = hiddenStoreSecond()
	case "method":
		_ = (hiddenStoreFactory{}).open()
	case "closure":
		factory := func() storage.Engine { return storage.NewMemoryEngine() }
		_ = factory()
	case "owned":
		_ = fixture.Own(t, fixture.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	case "":
		return
	default:
		t.Fatal("unknown boundary control")
	}
}
