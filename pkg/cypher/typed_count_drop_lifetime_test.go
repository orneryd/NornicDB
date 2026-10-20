package cypher

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTypedCountDropRejectsEarlierTransaction(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, recreate := range []bool{false, true} {
			t.Run(fmt.Sprintf("buffered=%v/recreate=%v", buffered, recreate), func(t *testing.T) {
				dir := t.TempDir()
				f := openTypedCountScope(t, dir, buffered)
				for _, db := range []string{"scope_old", "scope_peer"} {
					typedCountRun(t, f.admin, "CREATE DATABASE "+db)
				}
				a, b := f.executor(t, "scope_old"), f.executor(t, "scope_peer")
				typedCountSeed(t, a, 1)
				typedCountSeed(t, b, 2)
				typedCountRun(t, a, "BEGIN")
				typedCountRun(t, a, "CREATE (:ScopeStart:ScopeProbe)-[:SCOPE]->(:ScopeEnd:ScopeProbe)")
				typedCountRun(t, b, "BEGIN")
				typedCountRun(t, b, "CREATE (:PeerNew)")
				typedCountRun(t, f.admin, "DROP DATABASE scope_old")
				if recreate {
					typedCountRun(t, f.admin, "CREATE DATABASE scope_old")
				}
				_, err := a.Execute(context.Background(), "COMMIT", nil)
				if recreate {
					require.ErrorIs(t, err, storage.ErrConflict, "an old transaction must not enter the recreated namespace")
				} else {
					require.True(t, errors.Is(err, storage.ErrConflict) || errors.Is(err, multidb.ErrDatabaseNotFound),
						"an old transaction must be rejected while its database is absent: %v", err)
				}
				typedCountRun(t, b, "COMMIT")
				if !recreate {
					typedCountRun(t, f.admin, "CREATE DATABASE scope_old")
				}
				a = f.executor(t, "scope_old")
				typedCountShapes(t, a, 0)
				typedCountWant(t, a, "MATCH (n) RETURN count(n) AS n", 0)
				typedCountSeed(t, a, 1)
				typedCountShapes(t, a, 1)
				typedCountShapes(t, b, 2)
				typedCountWant(t, b, "MATCH (n:PeerNew) RETURN count(n) AS n", 1)
				f.close(t)
				f = openTypedCountScope(t, dir, buffered)
				typedCountShapes(t, f.executor(t, "scope_old"), 1)
				typedCountShapes(t, f.executor(t, "scope_peer"), 2)
			})
		}
	}
}
