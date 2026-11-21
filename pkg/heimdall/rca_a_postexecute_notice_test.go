package heimdall

// Root-cause diagnostic (campaign v1.5.2, lane A, group
// G-heimdall-postexecute-failure-notice). Test-side only.
// CallPostExecuteHooks (plugin.go:1896) enqueues each hook on the worker pool
// and returns; the streaming handler drains the context's notification queue
// immediately afterwards (handler.go:991-992, 1009-1010). A notice the hook
// queues after that drain stays in the queue and nothing drains it again.
// The corrected variant joins the hook before draining.

import "testing"

func TestRCAPostExecuteNoticeArrivesAfterDrain(t *testing.T) {
	t.Run("handler order: drain right after CallPostExecuteHooks", func(t *testing.T) {
		probe := newNOR302Probe(t)
		ctx := &PostExecuteContext{Context: t.Context(), RequestID: "rca-a", Action: nor302ProbeAction, Result: &ActionResult{Success: true}}
		CallPostExecuteHooks(ctx)
		returned := "returned"
		select {
		case <-probe.finished:
			returned = "returned-after-hook-finished"
		default:
		}
		probe.waitStarted(t)
		first := ctx.DrainNotifications()
		t.Logf("RCA_POSTEXEC CallPostExecuteHooks %s; drain at handler point: %d notices %+v", returned, len(first), first)
		probe.releaseHook()
		probe.waitFinished(t)
		late := ctx.DrainNotifications()
		t.Logf("RCA_POSTEXEC after hook finished: %d notices %+v (no handler code drains this queue again)", len(late), late)
	})
	t.Run("corrected variant: join the hook, then drain", func(t *testing.T) {
		probe := newNOR302Probe(t)
		ctx := &PostExecuteContext{Context: t.Context(), RequestID: "rca-a2", Action: nor302ProbeAction, Result: &ActionResult{Success: true}}
		CallPostExecuteHooks(ctx)
		probe.releaseHook()
		probe.waitFinished(t)
		got := ctx.DrainNotifications()
		t.Logf("RCA_POSTEXEC joined-then-drained: %d notices %+v", len(got), got)
	})
}
