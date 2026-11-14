package config

import "testing"

func TestExperimentalPropertyStorageToggle(t *testing.T) {
	previous := IsExperimentalPropertyStorageEnabled()
	t.Cleanup(func() { SetExperimentalPropertyStorageEnabled(previous) })
	for _, enabled := range []bool{false, true, false} {
		SetExperimentalPropertyStorageEnabled(enabled)
		if IsExperimentalPropertyStorageEnabled() != enabled || GetFeatureStatus().ExperimentalPropertyStorageEnabled != enabled {
			t.Fatal("feature status does not match flag")
		}
	}
}
