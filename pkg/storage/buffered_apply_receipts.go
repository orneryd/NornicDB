package storage

import (
	"context"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// unappliedBufferedCommits observes markers in the SAME conflict-tracked update
// transaction that applies the retained records and writes their markers. Its
// managed read floor remains registered until the whole apply returns. A racing
// duplicate therefore conflicts instead of publishing counts twice; callers may
// explicitly retry the existing retriable conflict.
func unappliedBufferedCommits(ctx context.Context, txn *badger.Txn, commits []bufferedCommit) ([]bufferedCommit, error) {
	var seen map[string]struct{}
	if len(commits) > 1 {
		seen = make(map[string]struct{}, len(commits))
	}
	var retained []bufferedCommit
	filtered := false
	for i, commit := range commits {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		keep := true
		if commit.txID != "" {
			key := bufferedCommitMarkerKey(commit.namespace, commit.txID, commit.logicalTxID)
			duplicate := false
			if seen != nil {
				_, duplicate = seen[string(key)]
			}
			if duplicate {
				keep = false
			} else {
				_, err := txn.Get(key)
				if err == nil {
					keep = false
				} else if !errors.Is(err, badger.ErrKeyNotFound) {
					return nil, err
				}
				if seen != nil {
					seen[string(key)] = struct{}{}
				}
			}
		}
		if !keep && !filtered {
			retained = append([]bufferedCommit(nil), commits[:i]...)
			filtered = true
		}
		if keep && filtered {
			retained = append(retained, commit)
		}
	}
	if !filtered {
		return commits, nil
	}
	return retained, nil
}
