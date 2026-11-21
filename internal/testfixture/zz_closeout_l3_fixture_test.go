// Closeout lane L3 (v1.5.2 verification campaign), 22 Nov 2026. Test-side only.
// Items: P-N152-131 (ExtractZip close diagnostics), P-NGEN-24 (invalid owner
// disposal diagnostics), P-NGEN-16 (inline t.Cleanup closures discard Close
// errors), P-NGEN-23 (one failure writer, behaviour control).
package testfixture_test

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	fixture "github.com/orneryd/nornicdb/internal/testfixture"
)

const closeoutL3ChildEnv = "CLOSEOUT_L3_CHILD"

// closeoutL3RunChild re-runs this test binary for one child test so that a
// deliberately failing fixture call can be observed without failing the parent.
func closeoutL3RunChild(t *testing.T, name string) (string, error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+name+"$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), closeoutL3ChildEnv+"="+name)
	out, err := cmd.CombinedOutput()
	t.Logf("CHILD %s exit=%v\n----- child output -----\n%s\n----- end child output -----", name, err, out)
	return string(out), err
}

func closeoutL3IsChild(name string) bool { return os.Getenv(closeoutL3ChildEnv) == name }

type closeoutL3FailingCloser struct {
	err    error
	closed int
}

func (c *closeoutL3FailingCloser) Close() error {
	c.closed++
	return c.err
}

// ---------------------------------------------------------------------------
// P-N152-131: ExtractZip records copyErr first; a failing member Close (and the
// deferred archive.Close) is then never reported.

// closeoutL3CorruptDeflateZip writes an archive whose single deflate member has
// an invalid block type (0xFF: BFINAL=1, BTYPE=11 reserved). Copy fails and the
// member's Close returns the decompressor's error as a separate value.
func closeoutL3CorruptDeflateZip(t *testing.T) string {
	t.Helper()
	var buf bytes.Buffer
	writer := zip.NewWriter(&buf)
	member, err := writer.CreateHeader(&zip.FileHeader{Name: "data/member.txt", Method: zip.Deflate})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := member.Write(bytes.Repeat([]byte("closeout-l3 member payload\n"), 200)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	reader, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	offset, err := reader.File[0].DataOffset()
	if err != nil {
		t.Fatal(err)
	}
	raw[offset] = 0xFF
	path := filepath.Join(t.TempDir(), "corrupt.zip")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCloseoutL3_N152131_ChildExtractCorrupt(t *testing.T) {
	if !closeoutL3IsChild("TestCloseoutL3_N152131_ChildExtractCorrupt") {
		t.Skip("child of TestCloseoutL3_N152131_ExtractZipReportsMemberCloseError")
	}
	path := os.Getenv("CLOSEOUT_L3_ZIP")
	fixture.ExtractZip(t, path, "")
}

func TestCloseoutL3_N152131_ExtractZipReportsMemberCloseError(t *testing.T) {
	if os.Getenv(closeoutL3ChildEnv) != "" {
		t.Skip("parent only")
	}
	path := closeoutL3CorruptDeflateZip(t)

	// Decisive value: the same member, read the way ExtractZip reads it, has a
	// copy error AND a separate non-nil Close error.
	archive, err := zip.OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	input, err := archive.File[0].Open()
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(io.Discard, input)
	closeErr := input.Close()
	archiveErr := archive.Close()
	t.Logf("DECISIVE member copyErr=%v memberCloseErr=%v archiveCloseErr=%v", copyErr, closeErr, archiveErr)
	if copyErr == nil || closeErr == nil {
		t.Fatalf("setup: expected both a copy error and a member Close error, got copy=%v close=%v", copyErr, closeErr)
	}

	t.Setenv("CLOSEOUT_L3_ZIP", path)
	out, childErr := closeoutL3RunChild(t, "TestCloseoutL3_N152131_ChildExtractCorrupt")
	// Control: the primary (copy) failure is reported and fails the child.
	if childErr == nil || !strings.Contains(out, copyErr.Error()) {
		t.Fatalf("control failed: ExtractZip should fail the test with the copy error %q", copyErr)
	}
	// The copy error and the member Close error are two values with the same
	// text here; reporting both (separately or joined) prints the text twice.
	reported := strings.Count(out, closeErr.Error())
	t.Logf("DECISIVE ExtractZip child output mentions the error text %d time(s) (copy error + member Close error = 2 expected)", reported)
	if reported < 2 {
		t.Errorf("NOR-361: ExtractZip reported %d error(s); the member Close error %q (returned separately after the copy failure) was not reported", reported, closeErr)
	}
}

// Structural fact recorded alongside the run: ExtractZip defers archive.Close()
// and discards its result (no public seam can make it fail).
func TestCloseoutL3_N152131_DeferredArchiveCloseResultDiscarded(t *testing.T) {
	if os.Getenv(closeoutL3ChildEnv) != "" {
		t.Skip("parent only")
	}
	root := closeoutL3ModuleRoot(t)
	path := filepath.Join(root, "internal", "testfixture", "archive.go")
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	discarded := false
	ast.Inspect(file, func(n ast.Node) bool {
		if d, ok := n.(*ast.DeferStmt); ok {
			if sel, ok := d.Call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Close" {
				discarded = true
			}
		}
		return true
	})
	t.Logf("DECISIVE archive.go has `defer X.Close()` with discarded result: %v", discarded)
	if discarded {
		t.Errorf("NOR-361: ExtractZip defers archive.Close() and drops its error")
	}
}

// ---------------------------------------------------------------------------
// P-NGEN-24: requireTestingOwner/TrackProcess dispose the resource on an invalid
// owner and panic; the disposal error is dropped from the panic message.

func TestCloseoutL3_NGEN24_InvalidOwnerPanicIncludesDisposalError(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(resource *closeoutL3FailingCloser)
	}{
		{"Track-nil-T", func(resource *closeoutL3FailingCloser) {
			var owner *testing.T
			fixture.Track(owner, resource, nil)
		}},
		{"TrackProcess-nil-Boundary", func(resource *closeoutL3FailingCloser) {
			fixture.TrackProcess[*closeoutL3FailingCloser](nil, resource, nil)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resource := &closeoutL3FailingCloser{err: errors.New("closeout-l3 disposal failed")}
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				tc.call(resource)
			}()
			message := fmt.Sprint(recovered)
			t.Logf("DECISIVE panic=%q closeCalls=%d closeErr=%v", message, resource.closed, resource.err)
			// Control: the invalid owner is refused and the resource is disposed.
			if recovered == nil || resource.closed != 1 {
				t.Fatalf("control: expected refusal panic and one disposal, got panic=%v closes=%d", recovered, resource.closed)
			}
			if !strings.Contains(message, resource.err.Error()) {
				t.Errorf("NOR-365: invalid-owner refusal omits the failed disposal: panic %q lacks %q", message, resource.err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// P-NGEN-16: inline t.Cleanup closures that call Close and discard its error.

func closeoutL3ModuleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}

// closeoutL3DiscardedClose reports whether stmt calls X.Close() and discards
// its result (bare expression statement or assignment to blank identifiers).
func closeoutL3DiscardedClose(stmt ast.Stmt) bool {
	var call *ast.CallExpr
	switch s := stmt.(type) {
	case *ast.ExprStmt:
		call, _ = s.X.(*ast.CallExpr)
	case *ast.AssignStmt:
		for _, lhs := range s.Lhs {
			if id, ok := lhs.(*ast.Ident); !ok || id.Name != "_" {
				return false
			}
		}
		if len(s.Rhs) == 1 {
			call, _ = s.Rhs[0].(*ast.CallExpr)
		}
	}
	if call == nil || len(call.Args) != 0 {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "Close"
}

func TestCloseoutL3_NGEN16_InlineCleanupCloseCensus(t *testing.T) {
	if os.Getenv(closeoutL3ChildEnv) != "" {
		t.Skip("parent only")
	}
	root := closeoutL3ModuleRoot(t)
	skip := map[string]bool{".git": true, "node_modules": true, "third_party": true, "vendor": true, "testdata": true}
	discarded, closures, files := 0, 0, 0
	packages := map[string]int{}
	var examples []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root && (skip[d.Name()] || strings.HasPrefix(d.Name(), ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") || strings.Contains(filepath.Base(path), "zz_closeout_l3_") {
			return nil
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return nil // unparsable fixture sources are not test code
		}
		files++
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) != 1 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Cleanup" {
				return true
			}
			lit, ok := call.Args[0].(*ast.FuncLit)
			if !ok {
				return true
			}
			closures++
			ast.Inspect(lit.Body, func(m ast.Node) bool {
				if _, nested := m.(*ast.FuncLit); nested {
					return false
				}
				if stmt, ok := m.(ast.Stmt); ok && closeoutL3DiscardedClose(stmt) {
					discarded++
					rel, _ := filepath.Rel(root, filepath.Dir(path))
					packages[filepath.ToSlash(rel)]++
					if len(examples) < 25 {
						rel, _ := filepath.Rel(root, path)
						examples = append(examples, fmt.Sprintf("%s:%d", filepath.ToSlash(rel), fset.Position(m.Pos()).Line))
					}
				}
				return true
			})
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(packages))
	for name := range packages {
		names = append(names, name)
	}
	sort.Strings(names)
	t.Logf("CENSUS test_files=%d inline_cleanup_closures=%d discarded_close_calls=%d packages=%d", files, closures, discarded, len(packages))
	for _, name := range names {
		t.Logf("CENSUS_PKG %s %d", name, packages[name])
	}
	for _, example := range examples {
		t.Logf("CENSUS_EXAMPLE %s", example)
	}
	// Control: the census sees the repository's test files and their closures.
	if files < 100 || closures == 0 {
		t.Fatalf("control: census saw too little (files=%d closures=%d)", files, closures)
	}
	if discarded != 0 {
		t.Errorf("NOR-331: %d inline t.Cleanup closures discard a Close error across %d packages", discarded, len(packages))
	}
}

// Minimal demonstration: a resource whose Close fails, released by an inline
// cleanup that discards the error, leaves its test passing.
func TestCloseoutL3_NGEN16_ChildInlineCleanup(t *testing.T) {
	if !closeoutL3IsChild("TestCloseoutL3_NGEN16_ChildInlineCleanup") {
		t.Skip("child of TestCloseoutL3_NGEN16_InlineCleanupLosesCloseError")
	}
	resource := &closeoutL3FailingCloser{err: errors.New("closeout-l3 inline close failed")}
	t.Cleanup(func() { _ = resource.Close() })
	t.Cleanup(func() { t.Logf("CHILD_CLOSE_CALLS %d", resource.closed) })
}

// Control: the same failing resource through the fixture owner fails its test.
func TestCloseoutL3_NGEN16_ChildOwnCleanup(t *testing.T) {
	if !closeoutL3IsChild("TestCloseoutL3_NGEN16_ChildOwnCleanup") {
		t.Skip("child of TestCloseoutL3_NGEN16_InlineCleanupLosesCloseError")
	}
	resource := &closeoutL3FailingCloser{err: errors.New("closeout-l3 owned close failed")}
	fixture.Own(t, resource)
}

func TestCloseoutL3_NGEN16_InlineCleanupLosesCloseError(t *testing.T) {
	if os.Getenv(closeoutL3ChildEnv) != "" {
		t.Skip("parent only")
	}
	ownOut, ownErr := closeoutL3RunChild(t, "TestCloseoutL3_NGEN16_ChildOwnCleanup")
	ownReports := strings.Count(ownOut, "closing test fixture: closeout-l3 owned close failed")
	t.Logf("DECISIVE owned: exit=%v reports=%d", ownErr, ownReports)
	if ownErr == nil || ownReports != 1 {
		t.Fatalf("control: fixture owner must fail the test once with the close error (exit=%v reports=%d)", ownErr, ownReports)
	}
	inlineOut, inlineErr := closeoutL3RunChild(t, "TestCloseoutL3_NGEN16_ChildInlineCleanup")
	closed := strings.Contains(inlineOut, "CHILD_CLOSE_CALLS 1")
	t.Logf("DECISIVE inline: exit=%v closeCalled=%v errorTextReported=%v", inlineErr, closed, strings.Contains(inlineOut, "closeout-l3 inline close failed"))
	if !closed {
		t.Fatalf("setup: inline cleanup did not call Close")
	}
	if inlineErr == nil {
		t.Errorf("NOR-331: a test whose resource Close failed (inline t.Cleanup discarding the error) passed")
	}
}

// ---------------------------------------------------------------------------
// P-NGEN-23 behaviour control: one Close failure produces exactly one report
// (the item's two equivalent writers of releaseOutcome.failed are checked
// structurally in zz_closeout_l3_internal_test.go).
func TestCloseoutL3_NGEN23_ChildTrackUnclosed(t *testing.T) {
	if !closeoutL3IsChild("TestCloseoutL3_NGEN23_ChildTrackUnclosed") {
		t.Skip("child of TestCloseoutL3_NGEN23_SingleFailureReport")
	}
	resource := &closeoutL3FailingCloser{err: errors.New("closeout-l3 tracked close failed")}
	fixture.Track(t, resource, nil)
}

func TestCloseoutL3_NGEN23_SingleFailureReport(t *testing.T) {
	if os.Getenv(closeoutL3ChildEnv) != "" {
		t.Skip("parent only")
	}
	for _, child := range []struct{ name, text string }{
		{"TestCloseoutL3_NGEN16_ChildOwnCleanup", "closeout-l3 owned close failed"},
		{"TestCloseoutL3_NGEN23_ChildTrackUnclosed", "closeout-l3 tracked close failed"},
	} {
		out, err := closeoutL3RunChild(t, child.name)
		reports := strings.Count(out, child.text)
		t.Logf("DECISIVE %s exit=%v reports=%d", child.name, err, reports)
		if err == nil || reports != 1 {
			t.Errorf("NOR-364 behaviour: %s should fail once with %q (exit=%v reports=%d)", child.name, child.text, err, reports)
		}
	}
}
