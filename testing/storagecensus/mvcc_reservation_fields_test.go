package storagecensus

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

func TestMVCCReservationFieldsRetainExactPublicationOwners(t *testing.T) {
	p, pkg := ssaFixture(t, `package storage
 type mvccReservations struct { count, cutsInProgress int; lastCutNanos int64 }
 func(r *mvccReservations)enterLocked(){r.count++}
 func(r *mvccReservations)retireLocked(){if r.cutsInProgress==0 {r.count--}}
 func(r *mvccReservations)endCutLocked(){r.cutsInProgress--;r.count--}
 func(r *mvccReservations)cutStarted(){r.cutsInProgress++}
 func(r *mvccReservations)cutTaken()int{r.lastCutNanos++;return r.count}
 func(r *mvccReservations)reserve()int64{return r.lastCutNanos}
 func rogueWrite(r *mvccReservations){r.count++;r.cutsInProgress++;r.lastCutNanos++}
 func rogueRead(r *mvccReservations)(int,int,int64){return r.count,r.cutsInProgress,r.lastCutNanos}
 `, "pkg/storage", ssa.SanityCheckFunctions)
	for fn := range ssautil.AllFunctions(pkg.Prog) {
		if fn.Pkg == pkg {
			p.funcs = append(p.funcs, fn)
		}
	}
	r := &Report{}
	(&analysis{p: p}).checkFields(r)
	text := strings.Join(r.Violations, "\n")
	for _, field := range []string{"count", "cutsInProgress", "lastCutNanos"} {
		name := "pkg/storage.mvccReservations." + field
		row, ok := classOf(name)
		require.True(t, ok)
		require.Equal(t, "publication-order", row.class)
		for _, misuse := range []struct{ kind, caller string }{{"change", "rogueWrite"}, {"read", "rogueRead"}} {
			found := false
			for _, violation := range r.Violations {
				found = found || strings.Contains(violation, "unowned publication-order "+misuse.kind+": "+name) && strings.Contains(violation, "in pkg/storage."+misuse.caller)
			}
			require.True(t, found, "%s must reject %s in %s", name, misuse.kind, misuse.caller)
		}
	}
	for _, owner := range []string{"enterLocked", "retireLocked", "endCutLocked", "cutStarted", "cutTaken", "reserve"} {
		require.NotContains(t, text, "in pkg/storage.mvccReservations."+owner)
	}
	_, ok := classOf("pkg/storage.mvccReservations.unreviewedCount")
	require.False(t, ok)
	_, ok = classOf("pkg/storage.mvccReservationsLookalike.count")
	require.False(t, ok)
}
