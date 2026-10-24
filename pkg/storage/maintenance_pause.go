package storage

import "sync"

// The points where a maintenance pass that runs beside writes can be paused.
const (
	pauseIndexBackfillChunk  = "index-backfill-chunk"  // a backfill chunk read its records, before its commit
	pauseTemporalRebuildNode = "temporal-rebuild-node" // the temporal rebuild read a node, before indexing it
	pauseIDRepairRestore     = "id-repair-restore"     // the ID repair identified an entity, before restoring it
)

// maintenancePauseHook lets storage tests stop a maintenance pass at a named
// point and land a write there; production leaves it nil.
var (
	maintenancePauseHook   func(point string)
	maintenancePauseHookMu sync.RWMutex
)

func setMaintenancePauseHook(hook func(point string)) (restore func()) {
	maintenancePauseHookMu.Lock()
	previous := maintenancePauseHook
	maintenancePauseHook = hook
	maintenancePauseHookMu.Unlock()
	return func() {
		maintenancePauseHookMu.Lock()
		maintenancePauseHook = previous
		maintenancePauseHookMu.Unlock()
	}
}

func maintenancePause(point string) {
	maintenancePauseHookMu.RLock()
	hook := maintenancePauseHook
	maintenancePauseHookMu.RUnlock()
	if hook != nil {
		hook(point)
	}
}
