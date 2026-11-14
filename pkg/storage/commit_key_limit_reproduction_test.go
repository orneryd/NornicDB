package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// B03/report6 reproduction. This file names nothing the change adds, so it runs
// against the source before it as well as after: on that source the commit is
// journaled before its label count key reaches the store, so the write buffer
// acknowledges the commit and the store never gains the node, and the prepared
// hand-off accepts a record the store can never apply. With key admission the
// same commit is refused, and neither a stored node nor a journal record is left
// behind, before or after the recovery at open.
//
// The label's count delta key (countDeltaKey) is the label's length plus 16 in
// this namespace, so a label of 64990 bytes has a 65006-byte key: 6 bytes more
// than the store's 65000. The label's index key, at 65000 bytes, fits.
func TestStoreKeyLimitRefusesACommitItCannotWrite(t *testing.T) {
	for _, async := range []bool{false, true} {
		for mode, modeName := range admissionModes {
			t.Run(fmt.Sprintf("async=%v/%s", async, modeName), func(t *testing.T) {
				dir := t.TempDir()
				b := admissionEngine(t, dir, async, false)
				err := admissionCommit(t, b, mode, "key-limit", func(ctx context.Context, tx *BadgerTransaction) error {
					_, err := tx.CreateNode(ctx, &Node{ID: "test:key-limit", Labels: []string{"Admission", strings.Repeat("l", 64990)}})
					return err
				})
				require.Error(t, err, "a commit whose label count key is longer than the store's keys is refused")
				require.Zero(t, admissionRecords(t, dir, "key-limit"), "the journal holds no record of the refused commit")
				require.Zero(t, admissionStored(t, b))
				b = admissionReopen(t, b, dir, async, false)
				require.Zero(t, admissionStored(t, b), "after crashClose and the recovery at open")
			})
		}
	}
}
