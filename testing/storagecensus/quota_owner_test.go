package storagecensus

import (
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
	"strings"
	"testing"
)

func TestSharedQuotaFieldsRetainExactWriters(t *testing.T) {
	p, pkg := ssaFixture(t, `package multidb
 type DatabaseInfo struct{nodeSize,usageNodes,otherSize int64;pending map[string]reservedUsage}
 type reservedUsage struct{nodes int64}
 type managedDatabaseRuntime struct{info *DatabaseInfo}
 func(r *managedDatabaseRuntime)initializeStorageUsage(){r.info.nodeSize++;r.info.usageNodes++}
 func(r *managedDatabaseRuntime)ResetManagedEntityUsage(){r.info.nodeSize=0;r.info.usageNodes=0}
 func(i *DatabaseInfo)publishStorageUsage(){i.nodeSize++;i.usageNodes++}
 func(i *DatabaseInfo)reserveStorageUsage(r *reservedUsage){r.nodes++;i.pending["held"]=*r}
 func(i *DatabaseInfo)removeReservationLocked(r *reservedUsage){r.nodes--;delete(i.pending,"held")}
 func rogue(i *DatabaseInfo,r *reservedUsage){i.nodeSize++;i.usageNodes++;i.otherSize++;r.nodes++;alias:=i.pending;alias["rogue"]=*r}
 type DatabaseInfoLookalike struct{nodeSize int64}
 func lookalike(i *DatabaseInfoLookalike){i.nodeSize++}
 `, "pkg/multidb", ssa.SanityCheckFunctions)
	for fn := range ssautil.AllFunctions(pkg.Prog) {
		if fn.Pkg == pkg {
			p.funcs = append(p.funcs, fn)
		}
	}
	r := &Report{}
	(&analysis{p: p}).checkFields(r)
	text := strings.Join(r.Violations, "\n")
	for _, field := range []string{"pkg/multidb.DatabaseInfo.nodeSize", "pkg/multidb.DatabaseInfo.usageNodes", "pkg/multidb.reservedUsage.nodes", "pkg/multidb.DatabaseInfo.pending"} {
		require.Contains(t, text, "unowned quota change: "+field)
		require.Contains(t, text, "in pkg/multidb.rogue")
	}
	for _, owner := range []string{"managedDatabaseRuntime.initializeStorageUsage", "managedDatabaseRuntime.ResetManagedEntityUsage", "DatabaseInfo.publishStorageUsage", "DatabaseInfo.reserveStorageUsage", "DatabaseInfo.removeReservationLocked"} {
		require.NotContains(t, text, "in pkg/multidb."+owner)
	}
	_, classified := classOf("pkg/multidb.DatabaseInfoLookalike.nodeSize")
	require.False(t, classified)
}
