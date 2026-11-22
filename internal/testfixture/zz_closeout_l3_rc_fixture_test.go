// Closeout lane L3 (v1.5.2 verification campaign), 22 Nov 2026. Test-side only.
// Root-cause proofs for P-N152-131 (NOR-361) and P-NGEN-16 (NOR-331).
package testfixture_test

import (
	"archive/zip"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	fixture "github.com/orneryd/nornicdb/internal/testfixture"
)

// closeoutL3RCExtractMember is a test-side copy of ExtractZip's per-member step
// (archive.go:28-39) that reports the three outcomes together instead of
// stopping at the first require (copyErr at :37).
func closeoutL3RCExtractMember(file *zip.File, path string) (joined, copyErr, outputErr, inputErr error) {
	input, err := file.Open()
	if err != nil {
		return err, nil, nil, nil
	}
	output, err := os.Create(path)
	if err != nil {
		_ = input.Close()
		return err, nil, nil, nil
	}
	_, copyErr = io.Copy(output, input)
	outputErr, inputErr = output.Close(), input.Close()
	return errors.Join(copyErr, outputErr, inputErr), copyErr, outputErr, inputErr
}

// P-N152-131 member half. Owner: ExtractZip, archive.go:36-37: both Close
// results are computed at :36, then require.NoError(copyErr) at :37 ends the
// test, so a non-nil inputErr (:39) is never reported.
// (a) Diagnostic: on the corrupt member, copyErr and inputErr are both non-nil
// at :36 (two returns; flate may give them the same value and text). (b) Bypass: the copy that joins them reports both;
// a valid archive reports nothing (control).
func TestCloseoutL3_RC_N152131_MemberCloseErrorDroppedAfterCopyFailure(t *testing.T) {
	corrupt := closeoutL3CorruptDeflateZip(t)
	archive, err := zip.OpenReader(corrupt)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	joined, copyErr, outputErr, inputErr := closeoutL3RCExtractMember(archive.File[0], filepath.Join(t.TempDir(), "member.txt"))
	t.Logf("CLOSEOUT_L3_RC N152131 at :36 copyErr=%v outputErr=%v inputErr=%v sameValue=%v | bypass joined=%q", copyErr, outputErr, inputErr, copyErr == inputErr, joined)

	var valid bytes.Buffer
	writer := zip.NewWriter(&valid)
	member, err := writer.Create("data/ok.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := member.Write([]byte("ok\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := zip.NewReader(bytes.NewReader(valid.Bytes()), int64(valid.Len()))
	if err != nil {
		t.Fatal(err)
	}
	controlJoined, _, _, _ := closeoutL3RCExtractMember(reader.File[0], filepath.Join(t.TempDir(), "ok.txt"))
	t.Logf("CLOSEOUT_L3_RC N152131 control joined=%v", controlJoined)
	if controlJoined != nil {
		t.Fatalf("control: valid member must extract cleanly, got %v", controlJoined)
	}
	if copyErr == nil || inputErr == nil {
		t.Fatalf("MECHANISM: expected a copy failure and a separate member Close failure at :36 (copy=%v input=%v)", copyErr, inputErr)
	}
	if !errors.Is(joined, copyErr) || !errors.Is(joined, inputErr) || strings.Count(joined.Error(), inputErr.Error()) < 2 {
		t.Fatalf("BYPASS: the joined report must carry both the copy error and the member Close error, got %q", joined)
	}
}

type closeoutL3RCFailingArchive struct{ err error }

func (c closeoutL3RCFailingArchive) Close() error { return c.err }

// P-N152-131 deferred half. Owner: archive.go:20 `defer archive.Close()`.
// No public input makes zip.ReadCloser.Close fail (it closes an *os.File the
// function opened itself), so ExtractZip cannot be driven to it; this run shows
// the step on a seam: the same defer shape drops a failing Close, the corrected
// shape (named result joined in the defer) reports it.
func TestCloseoutL3_RC_N152131_DeferredCloseShape(t *testing.T) {
	failing := closeoutL3RCFailingArchive{err: errors.New("closeout-l3 archive close failed")}
	pinShape := func() error {
		defer failing.Close()
		return nil
	}
	corrected := func() (err error) {
		defer func() { err = errors.Join(err, failing.Close()) }()
		return nil
	}
	pinErr, correctedErr := pinShape(), corrected()
	t.Logf("CLOSEOUT_L3_RC N152131 deferred: pinShape=%v corrected=%v", pinErr, correctedErr)
	if pinErr != nil || correctedErr == nil {
		t.Fatalf("MECHANISM/BYPASS: pin shape must drop and corrected shape must report the Close error")
	}
}

// P-NGEN-16. The census run counted the inline closures; this is the minimal
// reproduction of the consequence plus its control and corrected form.
// Children run in a fresh process (closeoutL3RunChild).
func TestCloseoutL3_RCChild_NGEN16_Inline(t *testing.T) {
	if !closeoutL3IsChild("TestCloseoutL3_RCChild_NGEN16_Inline") {
		t.Skip("child")
	}
	resource := &closeoutL3FailingCloser{err: errors.New("closeout-l3 rc inline close failed")}
	// Registered first, so it runs last (cleanups are LIFO).
	t.Cleanup(func() { t.Logf("RC_CHILD_CLOSE_CALLS %d", resource.closed) })
	t.Cleanup(func() { _ = resource.Close() })
}

func TestCloseoutL3_RCChild_NGEN16_Checked(t *testing.T) {
	if !closeoutL3IsChild("TestCloseoutL3_RCChild_NGEN16_Checked") {
		t.Skip("child")
	}
	resource := &closeoutL3FailingCloser{err: errors.New("closeout-l3 rc checked close failed")}
	t.Cleanup(func() { t.Logf("RC_CHILD_CLOSE_CALLS %d", resource.closed) })
	t.Cleanup(func() {
		if err := resource.Close(); err != nil {
			t.Error(err)
		}
	})
}

func TestCloseoutL3_RCChild_NGEN16_Owned(t *testing.T) {
	if !closeoutL3IsChild("TestCloseoutL3_RCChild_NGEN16_Owned") {
		t.Skip("child")
	}
	fixture.Own(t, &closeoutL3FailingCloser{err: errors.New("closeout-l3 rc owned close failed")})
}

func TestCloseoutL3_RC_NGEN16_InlineCleanupLosesCloseError(t *testing.T) {
	if os.Getenv(closeoutL3ChildEnv) != "" {
		t.Skip("parent only")
	}
	inlineOut, inlineErr := closeoutL3RunChild(t, "TestCloseoutL3_RCChild_NGEN16_Inline")
	checkedOut, checkedErr := closeoutL3RunChild(t, "TestCloseoutL3_RCChild_NGEN16_Checked")
	ownedOut, ownedErr := closeoutL3RunChild(t, "TestCloseoutL3_RCChild_NGEN16_Owned")
	t.Logf("CLOSEOUT_L3_RC NGEN16 inline: exit=%v closed=%v errReported=%v | checked(bypass): exit=%v errReported=%v | owned(control): exit=%v errReported=%v",
		inlineErr, strings.Contains(inlineOut, "RC_CHILD_CLOSE_CALLS 1"), strings.Contains(inlineOut, "inline close failed"),
		checkedErr, strings.Contains(checkedOut, "checked close failed"),
		ownedErr, strings.Contains(ownedOut, "owned close failed"))
	if ownedErr == nil || !strings.Contains(ownedOut, "owned close failed") {
		t.Fatalf("control: the fixture owner must fail the test with the close error")
	}
	if checkedErr == nil || !strings.Contains(checkedOut, "checked close failed") {
		t.Fatalf("BYPASS: a cleanup that checks Close must fail the test")
	}
	if !strings.Contains(inlineOut, "RC_CHILD_CLOSE_CALLS 1") {
		t.Fatalf("setup: inline cleanup did not call Close")
	}
	if inlineErr != nil || strings.Contains(inlineOut, "inline close failed") {
		t.Fatalf("MECHANISM: the inline `_ = resource.Close()` cleanup must pass silently (exit=%v)", inlineErr)
	}
}
