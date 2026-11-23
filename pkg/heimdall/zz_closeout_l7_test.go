package heimdall

// Close-out lane L7 (scratch branch only; test-side, not product code).
// L-H7-012: TestHeimdallCoverage_HooksDispatcherAndActionInvoker calls
// setupHeimdallCoverageGlobals, which replaces the package globals
// globalManager, globalPostExecutePool and globalEventDispatcher and never
// restores them; the test then registers the PrePrompt plugins "alpha" (warns)
// and "beta" (cancels with "blocked") in that manager. A later streaming chat
// request is cancelled by beta and answered as a non-streaming completion.
//
// Run alone (-run '^TestL7012'):
//   A victim alone                          expected PASS (control)
//   B polluter then victim                   expected FAIL at handler_test.go:1329/1330
//   C polluter, restore globalManager, victim expected PASS (bypass of the leaked manager only)

import (
	"sort"
	"testing"
)

func l7012Plugins() []string {
	globalManagerMu.Lock()
	defer globalManagerMu.Unlock()
	if globalManager == nil {
		return nil
	}
	globalManager.mu.RLock()
	defer globalManager.mu.RUnlock()
	names := make([]string, 0, len(globalManager.plugins))
	for name := range globalManager.plugins {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

type l7012Globals struct {
	manager *SubsystemManager
	post    *postExecuteWorkerPool
	events  *dbEventDispatcher
}

func l7012Save() l7012Globals {
	globalManagerMu.Lock()
	defer globalManagerMu.Unlock()
	return l7012Globals{manager: globalManager, post: globalPostExecutePool, events: globalEventDispatcher}
}

func l7012RestoreAll(t *testing.T, saved l7012Globals) {
	t.Helper()
	globalManagerMu.Lock()
	globalManager = saved.manager
	globalManagerMu.Unlock()
	globalPostExecutePool = saved.post
	globalEventDispatcher = saved.events
}

func TestL7012AVictimAlone(t *testing.T) {
	t.Logf("L7_012 plugins before victim: %v", l7012Plugins())
	t.Run("TestHandler_OpenAI_V1Streaming_SuppressesHeimdallNotificationChunks", TestHandler_OpenAI_V1Streaming_SuppressesHeimdallNotificationChunks)
}

func TestL7012BPolluterThenVictim(t *testing.T) {
	saved := l7012Save()
	t.Cleanup(func() { l7012RestoreAll(t, saved) })
	t.Run("TestHeimdallCoverage_HooksDispatcherAndActionInvoker", TestHeimdallCoverage_HooksDispatcherAndActionInvoker)
	t.Logf("L7_012 plugins after polluter: %v (manager replaced: %v)", l7012Plugins(), l7012Save().manager != saved.manager)
	t.Run("TestHandler_OpenAI_V1Streaming_SuppressesHeimdallNotificationChunks", TestHandler_OpenAI_V1Streaming_SuppressesHeimdallNotificationChunks)
}

func TestL7012CPolluterRestoreManagerVictim(t *testing.T) {
	saved := l7012Save()
	t.Cleanup(func() { l7012RestoreAll(t, saved) })
	t.Run("TestHeimdallCoverage_HooksDispatcherAndActionInvoker", TestHeimdallCoverage_HooksDispatcherAndActionInvoker)
	t.Logf("L7_012 plugins after polluter: %v", l7012Plugins())
	// Bypass: restore only the subsystem manager the polluter replaced.
	globalManagerMu.Lock()
	globalManager = saved.manager
	globalManagerMu.Unlock()
	t.Logf("L7_012 plugins after restoring globalManager only: %v", l7012Plugins())
	t.Run("TestHandler_OpenAI_V1Streaming_SuppressesHeimdallNotificationChunks", TestHandler_OpenAI_V1Streaming_SuppressesHeimdallNotificationChunks)
}
