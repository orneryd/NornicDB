package main

import (
	"testing"
	"time"

	appconfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

func TestServeConfigPropagatesDecaySettings(t *testing.T) {
	cfg := &appconfig.Config{}
	cfg.Memory.DecayEnabled = true
	cfg.Memory.DecayInterval = 17 * time.Second
	cfg.Memory.AccessFlushBufferSize = 321
	cfg.Memory.VisibilityThreshold = 0.17

	dbConfig := nornicdb.DefaultConfig()
	dbConfig.Memory.DecayEnabled = cfg.Memory.DecayEnabled
	dbConfig.Memory.DecayInterval = cfg.Memory.DecayInterval
	dbConfig.Memory.AccessFlushBufferSize = cfg.Memory.AccessFlushBufferSize
	dbConfig.Memory.VisibilityThreshold = cfg.Memory.VisibilityThreshold

	if !dbConfig.Memory.DecayEnabled {
		t.Fatal("expected decay enabled to propagate into runtime DB config")
	}
	if dbConfig.Memory.DecayInterval != 17*time.Second {
		t.Fatalf("expected decay interval 17s, got %v", dbConfig.Memory.DecayInterval)
	}
	if dbConfig.Memory.AccessFlushBufferSize != 321 {
		t.Fatalf("expected access flush buffer size 321, got %d", dbConfig.Memory.AccessFlushBufferSize)
	}
	if dbConfig.Memory.VisibilityThreshold != 0.17 {
		t.Fatalf("expected visibility threshold 0.17, got %v", dbConfig.Memory.VisibilityThreshold)
	}
}
