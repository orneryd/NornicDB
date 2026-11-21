package search

// Batch 2b L541 (research only, untracked): during a build parked after its
// node snapshot, a node is replaced in storage and its refresh's temporal
// lookup fails once. Does the build publish the replaced (snapshot) entry, and
// for how long? Same text on both builds; per-build adapters in
// zz_ei2_l541_build_test.go.

import (
	"sync"
	"testing"
	"time"
)

func TestZZEI2L541BuildAdmissionFailure(t *testing.T) {
	for _, failLookup := range []bool{true, false} {
		name := "lookup-fails-once"
		if !failLookup {
			name = "control-lookup-ok"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
			gate := newNodeSnapshotGateEngine(t)
			engine := zzei2NewFaultEngine(gate)
			svc := buildWriteService(t, engine, "", "v2", nil)
			t.Cleanup(func() { _ = svc.Close() })
			t.Cleanup(gate.open)
			var mu sync.Mutex
			var retries []RefreshRetry
			svc.SetRefreshRetryOwner(func(work RefreshRetry, err error) {
				mu.Lock()
				retries = append(retries, work)
				mu.Unlock()
				t.Logf("ZZEI2 L541 %s: retry handed to owner, err=%v", name, err)
			})
			zzei2CreateNode(t, gate, buildWriteNode("nornic:x", "oldterm", []float32{1, 0, 0}))
			built := startGatedBuild(t, gate, svc)
			zzei2UpdateNode(t, gate, buildWriteNode("nornic:x", "newterm", []float32{0, 1, 0}))
			zzei2SetFail(engine, failLookup)
			var refreshErr error
			requireReturnsDuringBuild(t, "refresh during the build", func() error { refreshErr = zzei2Refresh(svc, "nornic:x"); return nil })
			gate.open()
			buildErr := <-built
			mu.Lock()
			handed := len(retries)
			mu.Unlock()
			t.Logf("ZZEI2 L541 %s: refresh err=%v; build err=%v; right after the build: oldterm=%v newterm=%v; retries handed=%d",
				name, refreshErr, buildErr, buildWriteSearchIDs(t, svc, "oldterm"), buildWriteSearchIDs(t, svc, "newterm"), handed)
			time.Sleep(500 * time.Millisecond)
			mu.Lock()
			handed = len(retries)
			var work RefreshRetry
			if handed > 0 {
				work = retries[handed-1]
			}
			mu.Unlock()
			t.Logf("ZZEI2 L541 %s: 500 ms after the build (no retry run yet): oldterm=%v newterm=%v; retries handed=%d",
				name, buildWriteSearchIDs(t, svc, "oldterm"), buildWriteSearchIDs(t, svc, "newterm"), handed)
			if handed > 0 {
				err := zzei2RunRetry(svc, work)
				t.Logf("ZZEI2 L541 %s: after running the handed retry (err=%v): oldterm=%v newterm=%v",
					name, err, buildWriteSearchIDs(t, svc, "oldterm"), buildWriteSearchIDs(t, svc, "newterm"))
			}
		})
	}
}
