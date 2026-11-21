// Closeout lane L3 (v1.5.2 verification campaign), 22 Nov 2026. Test-side only.
// Items: P-NGEN-22 (StoreBoundary diagnostic writer failure), P-NGEN-21
// (CurrentGoroutineID runtime header), P-NGEN-23 (releaseOutcome.failed writers).
package testfixture

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type closeoutL3FailingWriter struct {
	calls   int
	written int
	err     error
}

func (w *closeoutL3FailingWriter) Write(p []byte) (int, error) {
	w.calls++
	w.written += len(p)
	return 0, w.err
}

// P-NGEN-22: after a detected leak, a failing diagnostic writer's error is not
// reported anywhere: Check returns 1 exactly as when the write succeeds.
func TestCloseoutL3_NGEN22_LeakDiagnosticWriteErrorReported(t *testing.T) {
	leaked := StoreBoundary{start: -1} // any population differs from -1: a leak
	var good bytes.Buffer
	goodCode := leaked.Check(0, &good, "closeout-l3-control")
	t.Logf("DECISIVE control code=%d diagnostic=%q", goodCode, closeoutL3FirstLine(good.String()))
	if goodCode != 1 || !strings.Contains(good.String(), "Badger store(s) left open after tests") {
		t.Fatalf("control: leak must return 1 and write its diagnostic, got %d %q", goodCode, closeoutL3FirstLine(good.String()))
	}

	writer := &closeoutL3FailingWriter{err: errors.New("closeout-l3 diagnostic writer failed")}
	var code int
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		code = leaked.Check(0, writer, "closeout-l3-failing-writer")
	}()
	t.Logf("DECISIVE failing writer: code=%d writerCalls=%d writerErr=%v panic=%v", code, writer.calls, writer.err, recovered)
	if writer.calls == 0 {
		t.Fatalf("setup: Check did not write the diagnostic")
	}
	if recovered == nil {
		t.Errorf("NOR-363: the leak diagnostic write failed (%v) and Check reported nothing beyond code %d (same as a successful write)", writer.err, code)
	}
}

func closeoutL3FirstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// closeoutL3ParseGoroutineHeader is a test-side copy of the parsing step in
// CurrentGoroutineID (goroutine.go:12-13 at the pin), so a crafted header can
// be fed to it; the real function reads runtime.Stack directly.
func closeoutL3ParseGoroutineHeader(header []byte) (id uint64, panicked any) {
	defer func() { panicked = recover() }()
	fields := bytes.Fields(header)
	id, _ = strconv.ParseUint(string(fields[1]), 10, 64)
	return id, nil
}

// P-NGEN-21: the real runtime header is the standard "goroutine N [" format on
// the lab's Go, so CurrentGoroutineID returns the right ID for every goroutine.
// The crafted headers show what an unexpected format would do (logged only).
func TestCloseoutL3_NGEN21_CurrentGoroutineIDRuntimeHeader(t *testing.T) {
	headerID := regexp.MustCompile(`^goroutine (\d+) \[`)
	check := func() (uint64, uint64, string) {
		var buf [64]byte
		header := string(buf[:runtime.Stack(buf[:], false)])
		got := CurrentGoroutineID()
		m := headerID.FindStringSubmatch(header)
		if m == nil {
			return got, 0, header
		}
		want, _ := strconv.ParseUint(m[1], 10, 64)
		return got, want, header
	}
	var mu sync.Mutex
	var mismatches []string
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, want, header := check()
			if want == 0 || got != want {
				mu.Lock()
				mismatches = append(mismatches, fmt.Sprintf("got=%d want=%d header=%q", got, want, header))
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	got, want, header := check()
	t.Logf("DECISIVE runtime=%s test goroutine header=%q CurrentGoroutineID=%d headerID=%d goroutineMismatches=%d", runtime.Version(), header, got, want, len(mismatches))
	for _, crafted := range []string{"goroutine", "goroutine x [running]:", "thread 7 [running]:"} {
		id, p := closeoutL3ParseGoroutineHeader([]byte(crafted))
		t.Logf("CRAFTED (test-side copy) header=%q id=%d panic=%v", crafted, id, p)
	}
	if want == 0 || got != want || len(mismatches) != 0 {
		t.Errorf("NOR-362: the runtime emitted a header CurrentGoroutineID does not parse: %v", mismatches)
	}
}

// P-NGEN-23: the item's releaseOutcome.failed field with a local and an
// inherited writer (markFailedLocked) existed in scratch a211e8fbd + ec8971acb.
// This records whether that state, and so the duplicate writer, exists at the
// pin. The behaviour control is TestCloseoutL3_NGEN23_SingleFailureReport.
func TestCloseoutL3_NGEN23_ReleaseOutcomeFailedWriters(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	nonTest := 0
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		nonTest++
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.TypeSpec:
				if v.Name.Name == "releaseOutcome" {
					found = append(found, fmt.Sprintf("type %s at %s", v.Name.Name, fset.Position(v.Pos())))
				}
			case *ast.FuncDecl:
				if v.Name.Name == "markFailedLocked" || v.Name.Name == "outcomeOf" {
					found = append(found, fmt.Sprintf("func %s at %s", v.Name.Name, fset.Position(v.Pos())))
				}
			case *ast.AssignStmt:
				for _, lhs := range v.Lhs {
					if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "failed" {
						found = append(found, fmt.Sprintf("write .failed at %s", fset.Position(v.Pos())))
					}
				}
			}
			return true
		})
	}
	t.Logf("DECISIVE non-test files=%d releaseOutcome/markFailedLocked/.failed writers found=%d %v", nonTest, len(found), found)
	if nonTest == 0 {
		t.Fatalf("control: no package sources found in %s", dir)
	}
	if len(found) != 0 {
		t.Errorf("NOR-364: releaseOutcome.failed state and its writers exist at this commit: %v", found)
	}
}
