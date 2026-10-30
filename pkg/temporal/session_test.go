package temporal

import (
	"testing"
	"time"
)

func TestSessionDetectorConfig_Default(t *testing.T) {
	cfg := DefaultSessionDetectorConfig()
	if cfg.TimeGapThresholdSeconds <= 0 {
		t.Error("TimeGapThresholdSeconds should be positive")
	}
}

func TestSessionDetector_New(t *testing.T) {
	sd := NewSessionDetector(DefaultSessionDetectorConfig())
	if sd == nil {
		t.Fatal("NewSessionDetector returned nil")
	}
}

func TestSessionDetector_FirstAccess(t *testing.T) {
	sd := NewSessionDetector(DefaultSessionDetectorConfig())

	event := sd.RecordAccess("node-1", time.Now())

	if event.Type != SessionStart {
		t.Errorf("Type = %v, want SessionStart", event.Type)
	}
	if event.Reason != "first_access" {
		t.Errorf("Reason = %v, want first_access", event.Reason)
	}
}

func TestSessionDetector_SameSession(t *testing.T) {
	sd := NewSessionDetector(DefaultSessionDetectorConfig())

	now := time.Now()
	sd.RecordAccess("node-1", now)
	event := sd.RecordAccess("node-1", now.Add(10*time.Second))

	if event.Type != SessionContinue {
		t.Errorf("Type = %v, want SessionContinue", event.Type)
	}
}

func TestSessionDetector_TimeGapSession(t *testing.T) {
	cfg := DefaultSessionDetectorConfig()
	cfg.TimeGapThresholdSeconds = 60
	sd := NewSessionDetector(cfg)

	now := time.Now()
	sd.RecordAccess("node-1", now)
	event := sd.RecordAccess("node-1", now.Add(120*time.Second))

	if event.Type != SessionStart {
		t.Errorf("Type = %v, want SessionStart", event.Type)
	}
	if event.Reason != "time_gap" {
		t.Errorf("Reason = %v, want time_gap", event.Reason)
	}
}

func TestSessionDetector_GetCurrentSession(t *testing.T) {
	sd := NewSessionDetector(DefaultSessionDetectorConfig())

	sd.RecordAccess("node-1", time.Now())

	session := sd.GetCurrentSession("node-1")
	if session == nil {
		t.Fatal("Should have current session")
	}
	if !session.IsCurrent {
		t.Error("Session should be current")
	}
	if len(session.NodeIDs) == 0 {
		t.Error("Session should have nodes")
	}
}

func TestSessionDetector_GetSessionHistory(t *testing.T) {
	cfg := DefaultSessionDetectorConfig()
	cfg.TimeGapThresholdSeconds = 60
	sd := NewSessionDetector(cfg)

	now := time.Now()
	sd.RecordAccess("node-1", now)
	sd.RecordAccess("node-1", now.Add(120*time.Second))
	sd.RecordAccess("node-1", now.Add(240*time.Second))

	history := sd.GetSessionHistory("node-1", 10)
	t.Logf("Session history: %d sessions", len(history))
}

func TestSessionDetector_GetActiveSessions(t *testing.T) {
	sd := NewSessionDetector(DefaultSessionDetectorConfig())

	sd.RecordAccess("node-1", time.Now())
	sd.RecordAccess("node-2", time.Now())

	sessions := sd.GetActiveSessions()
	if len(sessions) < 1 {
		t.Error("Should have active sessions")
	}
}

func TestSessionDetector_GetCoAccessedNodes(t *testing.T) {
	sd := NewSessionDetector(DefaultSessionDetectorConfig())

	now := time.Now()
	sd.RecordAccess("node-1", now)
	sd.RecordAccess("node-1", now.Add(10*time.Second))

	coAccessed := sd.GetCoAccessedNodes("node-1")
	t.Logf("Co-accessed nodes: %v", coAccessed)
}

func TestSessionDetector_GetVelocity(t *testing.T) {
	sd := NewSessionDetector(DefaultSessionDetectorConfig())

	now := time.Now()
	for i := 0; i < 10; i++ {
		sd.RecordAccess("node-1", now.Add(time.Duration(i*10)*time.Second))
	}

	vel := sd.GetVelocity("node-1")
	t.Logf("Velocity: %.4f", vel)
}

func TestSessionDetector_AddListener(t *testing.T) {
	sd := NewSessionDetector(DefaultSessionDetectorConfig())

	var receivedEvent *SessionEvent
	sd.AddListener(func(e SessionEvent) {
		receivedEvent = &e
	})

	sd.RecordAccess("node-1", time.Now())

	if receivedEvent == nil {
		t.Error("Listener should have been called")
	}
}

func TestSessionDetector_Reset(t *testing.T) {
	sd := NewSessionDetector(DefaultSessionDetectorConfig())

	sd.RecordAccess("node-1", time.Now())
	sd.Reset()

	session := sd.GetCurrentSession("node-1")
	if session != nil {
		t.Error("Should have no sessions after reset")
	}
}
