package storagecensus

import (
	"golang.org/x/tools/go/ssa"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The bounded ledger keeps four publication-order controls: at holds each
// in-flight reservation's position, cuts lists the cuts in progress,
// cutsStarted numbers them, and lastCutNanos floors later reservations. The
// fixture names each real writer and reader, and proves the census still
// rejects a rogue writer or reader, a lookalike type and an unreviewed field.
func TestMVCCReservationFieldsRetainExactPublicationOwners(t *testing.T) {
	p, _ := ssaFixture(t, `package storage
 type mvccReservation struct{namespace string; sequence uint64}
 type mvccReservations struct { at map[mvccReservation]int; cuts []uint64; cutsStarted uint64; lastCutNanos int64; unreviewedCuts int }
 type mvccReservationsLookalike struct { at map[mvccReservation]int; cuts []uint64; cutsStarted uint64; lastCutNanos int64 }
 func(r *mvccReservations)enterLocked(){r.at[mvccReservation{}]=0}
 func(r *mvccReservations)retireLocked(){delete(r.at,mvccReservation{});if len(r.cuts)>0{_ = r.cutsStarted}}
 func(r *mvccReservations)cutStarted(){r.cutsStarted++;r.cuts=append(r.cuts,r.cutsStarted)}
 func(r *mvccReservations)endCutLocked(){r.cuts[0]=r.cuts[len(r.cuts)-1];r.cuts=r.cuts[:len(r.cuts)-1]}
 func(r *mvccReservations)cutTaken(){r.lastCutNanos++}
 func(r *mvccReservations)reserve()int64{return r.lastCutNanos}
 func rogueWrite(r *mvccReservations){r.at[mvccReservation{}]=0;r.cuts=append(r.cuts,1);r.cutsStarted++;r.lastCutNanos++}
 func rogueRead(r *mvccReservations)(int,int,uint64,int64){return len(r.at),len(r.cuts),r.cutsStarted,r.lastCutNanos}
 func unreviewedWrite(r *mvccReservations){r.unreviewedCuts++}
 func lookalikeWrite(r *mvccReservationsLookalike){r.at[mvccReservation{}]=0;r.cuts=append(r.cuts,1);r.cutsStarted++;r.lastCutNanos++}
 `, "pkg/storage", ssa.SanityCheckFunctions, importableFixture)
	r := &Report{}
	(&analysis{p: p}).checkFields(r)
	text := strings.Join(r.Violations, "\n")
	for _, field := range []string{"at", "cuts", "cutsStarted", "lastCutNanos"} {
		name := "pkg/storage.mvccReservations." + field
		row, ok := classOf(name)
		require.True(t, ok)
		require.Equal(t, "publication-order", row.class)
		require.NotEmpty(t, row.owners, "%s must name its owners", name)
		for _, misuse := range []struct{ kind, caller string }{{"change", "rogueWrite"}, {"read", "rogueRead"}} {
			found := false
			for _, violation := range r.Violations {
				found = found || strings.Contains(violation, "unowned publication-order "+misuse.kind+": "+name) && strings.Contains(violation, "in pkg/storage."+misuse.caller)
			}
			require.True(t, found, "%s must reject %s in %s", name, misuse.kind, misuse.caller)
		}
	}
	for _, owner := range []string{"enterLocked", "retireLocked", "cutStarted", "endCutLocked", "cutTaken", "reserve"} {
		require.NotContains(t, text, "in pkg/storage.mvccReservations."+owner)
	}
	for _, obsolete := range []string{"count", "cutsInProgress"} {
		_, ok := classOf("pkg/storage.mvccReservations." + obsolete)
		require.False(t, ok, "%s is no ledger field: its row must be gone", obsolete)
	}
	require.Contains(t, text, "unclassified field: pkg/storage.mvccReservations.unreviewedCuts is changed")
	for _, field := range []string{"at", "cuts", "cutsStarted", "lastCutNanos"} {
		_, ok := classOf("pkg/storage.mvccReservationsLookalike." + field)
		require.False(t, ok)
		require.Contains(t, text, "unclassified field: pkg/storage.mvccReservationsLookalike."+field+" is changed")
	}
}
