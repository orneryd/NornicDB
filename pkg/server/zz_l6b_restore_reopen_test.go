package server

// Lane L6b (closeout): M05 L-H9-002 / L-H1-002, a natively restored store does
// not reopen. Same flow as TestVerifyH9_002_RestoredStoreReopens (write=false).
// Before the reopen, every journal record of the restored target is listed;
// on the reopen error the accepted-outcome barrier (storage.AcceptedCommitError,
// commit_receipt_prototype.go) is opened with errors.As to read the native
// cause it holds in Err, and the journal record carrying that logical command
// ID is identified.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func zzL6bFindWALDirs(root string) []string {
	var dirs []string
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.IsDir() && info.Name() == "wal" {
			dirs = append(dirs, path)
		}
		return nil
	})
	return dirs
}

func TestL6bRestoreReopenNativeCause(t *testing.T) {
	base := t.TempDir()
	backup := verifyH9SourceBackup(t, base, "")
	target := filepath.Join(base, "target")
	tgt, err := verifyH9Open(t, target, true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, tgt.admin("POST", "/admin/restore", map[string]interface{}{"path": backup}))
	tgt.close()

	type rec struct {
		dir  string
		seq  uint64
		op   storage.OperationType
		db   string
		data []byte
	}
	var records []rec
	for _, dir := range zzL6bFindWALDirs(target) {
		visitErr := storage.VisitWALEntriesAfterFromDir(context.Background(), dir, 0, func(e storage.WALEntry) error {
			records = append(records, rec{dir: dir, seq: e.Sequence, op: e.Operation, db: e.Database, data: append([]byte(nil), e.Data...)})
			return nil
		})
		t.Logf("L6B_M05 wal dir=%s visit_err=%v", strings.TrimPrefix(dir, base), visitErr)
	}
	ops := map[string]int{}
	for _, r := range records {
		ops[fmt.Sprintf("%s/%s", r.op, r.db)]++
	}
	t.Logf("L6B_M05 restored target journal records=%d by op/db=%v", len(records), ops)

	_, reopenErr := verifyH9Open(t, target, false)
	t.Logf("L6B_M05 reopen err=%v", reopenErr)
	var accepted *storage.AcceptedCommitError
	if !errors.As(reopenErr, &accepted) {
		t.Logf("L6B_M05 reopen error carries no AcceptedCommitError (type %T)", reopenErr)
		return
	}
	cause := accepted.Err
	chain := []string{}
	for c := cause; c != nil; c = errors.Unwrap(c) {
		chain = append(chain, fmt.Sprintf("%T: %v", c, c))
		if len(chain) > 8 {
			break
		}
	}
	t.Logf("L6B_M05 accepted command=%s state=%v native cause=%v chain=%q", accepted.LogicalID, accepted.State, cause, chain)
	for _, r := range records {
		if accepted.LogicalID != "" && bytes.Contains(r.data, []byte(accepted.LogicalID)) {
			markers := []string{}
			for _, m := range []string{"system", "nornic", "_User", "_Role", "admin", "FR", "_Database"} {
				if bytes.Contains(r.data, []byte(m)) {
					markers = append(markers, m)
				}
			}
			t.Logf("L6B_M05 failing record seq=%d op=%s db=%q bytes=%d contains=%v", r.seq, r.op, r.db, len(r.data), markers)
		}
	}
	t.Errorf("CAUSE M05: reopen replays accepted command %s from the restored target's journal; its re-submission fails with %v", accepted.LogicalID, cause)
}
