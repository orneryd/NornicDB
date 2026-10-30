package temporal

import (
	"testing"
	"time"
)

func TestPatternDetectorConfig_Default(t *testing.T) {
	cfg := DefaultPatternDetectorConfig()
	if cfg.MinSamplesForPattern <= 0 {
		t.Error("MinSamplesForPattern should be positive")
	}
}

func TestPatternDetector_New(t *testing.T) {
	pd := NewPatternDetector(DefaultPatternDetectorConfig())
	if pd == nil {
		t.Fatal("NewPatternDetector returned nil")
	}
}

func TestPatternDetector_DailyPattern(t *testing.T) {
	cfg := DefaultPatternDetectorConfig()
	cfg.MinSamplesForPattern = 5
	cfg.DailyConfidenceThreshold = 0.2
	pd := NewPatternDetector(cfg)

	baseTime := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)
	for day := 0; day < 14; day++ {
		accessTime := baseTime.Add(time.Duration(day*24) * time.Hour)
		pd.RecordAccess("daily-node", accessTime)
	}

	patterns := pd.DetectPatterns("daily-node", 0)
	found := false
	for _, p := range patterns {
		if p.Type == PatternDaily {
			found = true
			if p.PeakHour != 9 {
				t.Errorf("PeakHour = %v, want 9", p.PeakHour)
			}
		}
	}
	if !found {
		t.Error("Should detect daily pattern")
	}
}

func TestPatternDetector_GrowingPattern(t *testing.T) {
	pd := NewPatternDetector(DefaultPatternDetectorConfig())
	now := time.Now()
	for i := 0; i < 15; i++ {
		pd.RecordAccess("growing-node", now.Add(time.Duration(i)*time.Second))
	}

	patterns := pd.DetectPatterns("growing-node", 0.1)
	found := false
	for _, p := range patterns {
		if p.Type == PatternGrowing {
			found = true
		}
	}
	if !found {
		t.Error("Should detect growing pattern")
	}
}

func TestPatternDetector_Reset(t *testing.T) {
	pd := NewPatternDetector(DefaultPatternDetectorConfig())
	pd.RecordAccess("node-1", time.Now())
	pd.Reset()
	patterns := pd.DetectPatterns("node-1", 0)
	if len(patterns) > 0 {
		t.Error("Should have no patterns after reset")
	}
}
