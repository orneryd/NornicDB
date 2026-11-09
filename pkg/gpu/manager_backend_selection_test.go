package gpu

import "testing"

func TestManagerEnablesOnlyUsableComputeBackends(t *testing.T) {
	check := func(t *testing.T, manager *Manager) {
		t.Helper()
		if manager.IsEnabled() {
			device := manager.Device()
			if device == nil || !device.Available || device.Backend == BackendNone {
				t.Fatalf("enabled manager selected hardware without a compute backend: %+v", device)
			}
		}
	}
	t.Run("constructor", func(t *testing.T) {
		config := DefaultConfig()
		config.Enabled, config.FallbackOnError = true, true
		manager, err := NewManager(config)
		if err != nil {
			t.Fatal(err)
		}
		check(t, manager)
	})
	t.Run("enable", func(t *testing.T) {
		manager, err := NewManager(DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.Enable(); err != nil {
			if manager.IsEnabled() {
				t.Fatal("failed enable left compute enabled")
			}
			return
		}
		check(t, manager)
	})
}
