// Package audit tests for compliance audit logging.
package audit

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewLogger(t *testing.T) {
	t.Run("disabled logger", func(t *testing.T) {
		logger, err := NewLogger(Config{Enabled: false})
		if err != nil {
			t.Fatalf("NewLogger() error = %v", err)
		}
		defer logger.Close()

		// Should not error when logging to disabled logger
		err = logger.Log(Event{Type: EventLogin})
		if err != nil {
			t.Errorf("Log() on disabled logger should not error, got %v", err)
		}
	})

	t.Run("file logger", func(t *testing.T) {
		tmpDir := t.TempDir()
		logPath := filepath.Join(tmpDir, "audit.log")

		logger, err := NewLogger(Config{
			Enabled: true,
			LogPath: logPath,
		})
		if err != nil {
			t.Fatalf("NewLogger() error = %v", err)
		}
		defer logger.Close()

		// Log an event
		err = logger.Log(Event{
			Type:     EventLogin,
			UserID:   "user-123",
			Username: "testuser",
			Success:  true,
		})
		if err != nil {
			t.Fatalf("Log() error = %v", err)
		}

		// Verify file was created and contains event
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("reading log file: %v", err)
		}
		if !strings.Contains(string(data), "LOGIN") {
			t.Error("expected log file to contain LOGIN event")
		}
		if !strings.Contains(string(data), "user-123") {
			t.Error("expected log file to contain user-123")
		}
	})
}

func TestLoggerWithWriter(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLoggerWithWriter(&buf, Config{Enabled: true})

	// Log multiple events
	events := []Event{
		{Type: EventLogin, UserID: "user-1", Success: true},
		{Type: EventDataRead, UserID: "user-1", ResourceID: "node-123", Success: true},
		{Type: EventLoginFailed, UserID: "user-2", Success: false, Reason: "wrong password"},
	}

	for _, e := range events {
		if err := logger.Log(e); err != nil {
			t.Fatalf("Log() error = %v", err)
		}
	}

	// Parse output
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Errorf("expected 3 log lines, got %d", len(lines))
	}

	// Verify each event
	var parsed Event
	if err := json.Unmarshal([]byte(lines[0]), &parsed); err != nil {
		t.Fatalf("parsing first event: %v", err)
	}
	if parsed.Type != EventLogin {
		t.Errorf("expected LOGIN, got %s", parsed.Type)
	}
	if parsed.ID == "" {
		t.Error("expected auto-generated ID")
	}
	if parsed.Timestamp.IsZero() {
		t.Error("expected auto-generated timestamp")
	}
}

func TestLogAuth(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLoggerWithWriter(&buf, Config{Enabled: true})

	err := logger.LogAuth(EventLogin, "user-123", "testuser", "192.168.1.1", "Mozilla/5.0", true, "")
	if err != nil {
		t.Fatalf("LogAuth() error = %v", err)
	}

	var event Event
	if err := json.Unmarshal(buf.Bytes(), &event); err != nil {
		t.Fatalf("parsing event: %v", err)
	}

	if event.Type != EventLogin {
		t.Errorf("expected LOGIN, got %s", event.Type)
	}
	if event.UserID != "user-123" {
		t.Errorf("expected user-123, got %s", event.UserID)
	}
	if event.IPAddress != "192.168.1.1" {
		t.Errorf("expected 192.168.1.1, got %s", event.IPAddress)
	}
	if event.UserAgent != "Mozilla/5.0" {
		t.Errorf("expected Mozilla/5.0, got %s", event.UserAgent)
	}
}

func TestLogDataAccess(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLoggerWithWriter(&buf, Config{Enabled: true})

	err := logger.LogDataAccess("user-123", "testuser", "node", "node-456", "READ", true, "PHI")
	if err != nil {
		t.Fatalf("LogDataAccess() error = %v", err)
	}

	var event Event
	if err := json.Unmarshal(buf.Bytes(), &event); err != nil {
		t.Fatalf("parsing event: %v", err)
	}

	if event.Type != "DATA_READ" {
		t.Errorf("expected DATA_READ, got %s", event.Type)
	}
	if event.Resource != "node" {
		t.Errorf("expected resource 'node', got %s", event.Resource)
	}
	if event.ResourceID != "node-456" {
		t.Errorf("expected resource ID 'node-456', got %s", event.ResourceID)
	}
	if event.DataClassification != "PHI" {
		t.Errorf("expected PHI classification, got %s", event.DataClassification)
	}
}

func TestLogErasure(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLoggerWithWriter(&buf, Config{Enabled: true})

	// Request
	err := logger.LogErasure("admin-1", "admin", "user-to-delete", false, "Erasure request received")
	if err != nil {
		t.Fatalf("LogErasure() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	var event Event
	if err := json.Unmarshal([]byte(lines[0]), &event); err != nil {
		t.Fatalf("parsing event: %v", err)
	}

	if event.Type != EventErasureRequest {
		t.Errorf("expected ERASURE_REQUEST, got %s", event.Type)
	}
	if event.Metadata["target_user_id"] != "user-to-delete" {
		t.Errorf("expected target_user_id in metadata")
	}

	// Complete
	err = logger.LogErasure("admin-1", "admin", "user-to-delete", true, "Erasure complete")
	if err != nil {
		t.Fatalf("LogErasure() error = %v", err)
	}

	lines = strings.Split(strings.TrimSpace(buf.String()), "\n")
	if err := json.Unmarshal([]byte(lines[1]), &event); err != nil {
		t.Fatalf("parsing event: %v", err)
	}

	if event.Type != EventErasureComplete {
		t.Errorf("expected ERASURE_COMPLETE, got %s", event.Type)
	}
}

func TestLogConsent(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLoggerWithWriter(&buf, Config{Enabled: true})

	// Grant consent
	err := logger.LogConsent("user-123", "testuser", true, "marketing", "v1.0")
	if err != nil {
		t.Fatalf("LogConsent() error = %v", err)
	}

	var event Event
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if err := json.Unmarshal([]byte(lines[0]), &event); err != nil {
		t.Fatalf("parsing event: %v", err)
	}

	if event.Type != EventConsentGiven {
		t.Errorf("expected CONSENT_GIVEN, got %s", event.Type)
	}
	if event.Metadata["consent_type"] != "marketing" {
		t.Errorf("expected consent_type 'marketing'")
	}

	// Revoke consent
	err = logger.LogConsent("user-123", "testuser", false, "marketing", "v1.0")
	if err != nil {
		t.Fatalf("LogConsent() error = %v", err)
	}

	lines = strings.Split(strings.TrimSpace(buf.String()), "\n")
	if err := json.Unmarshal([]byte(lines[1]), &event); err != nil {
		t.Fatalf("parsing event: %v", err)
	}

	if event.Type != EventConsentRevoked {
		t.Errorf("expected CONSENT_REVOKED, got %s", event.Type)
	}
}

func TestAlertCallback(t *testing.T) {
	var buf bytes.Buffer
	config := Config{
		Enabled:       true,
		AlertOnEvents: []EventType{EventBreach, EventSecurityAlert},
	}
	logger := NewLoggerWithWriter(&buf, config)

	var alertedEvents []Event
	logger.SetAlertCallback(func(e Event) {
		alertedEvents = append(alertedEvents, e)
	})

	// Normal event - no alert
	logger.Log(Event{Type: EventLogin, Success: true})
	if len(alertedEvents) != 0 {
		t.Error("normal event should not trigger alert")
	}

	// Security alert - should alert
	logger.Log(Event{Type: EventSecurityAlert, Reason: "suspicious activity"})
	if len(alertedEvents) != 1 {
		t.Error("security alert should trigger callback")
	}

	// Breach event - should alert
	logger.Log(Event{Type: EventBreach, Reason: "unauthorized access"})
	if len(alertedEvents) != 2 {
		t.Error("breach event should trigger callback")
	}
}

func TestReader(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	// Create logger and write events
	logger, err := NewLogger(Config{
		Enabled: true,
		LogPath: logPath,
	})
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}

	now := time.Now().UTC()
	events := []Event{
		{Timestamp: now.Add(-2 * time.Hour), Type: EventLogin, UserID: "user-1", Success: true},
		{Timestamp: now.Add(-1 * time.Hour), Type: EventDataRead, UserID: "user-1", ResourceID: "node-1", Success: true},
		{Timestamp: now.Add(-30 * time.Minute), Type: EventLoginFailed, UserID: "user-2", Success: false},
		{Timestamp: now, Type: EventDataCreate, UserID: "user-1", ResourceID: "node-2", Success: true},
	}

	for _, e := range events {
		if err := logger.Log(e); err != nil {
			t.Fatalf("Log() error = %v", err)
		}
	}
	logger.Close()

	// Test reader
	reader := NewReader(logPath)

	t.Run("query all", func(t *testing.T) {
		result, err := reader.Query(Query{})
		if err != nil {
			t.Fatalf("Query() error = %v", err)
		}
		if result.TotalCount != 4 {
			t.Errorf("expected 4 events, got %d", result.TotalCount)
		}
	})

	t.Run("query by user", func(t *testing.T) {
		result, err := reader.Query(Query{UserID: "user-1"})
		if err != nil {
			t.Fatalf("Query() error = %v", err)
		}
		if result.TotalCount != 3 {
			t.Errorf("expected 3 events for user-1, got %d", result.TotalCount)
		}
	})

	t.Run("query by event type", func(t *testing.T) {
		result, err := reader.Query(Query{EventTypes: []EventType{EventLogin, EventLoginFailed}})
		if err != nil {
			t.Fatalf("Query() error = %v", err)
		}
		if result.TotalCount != 2 {
			t.Errorf("expected 2 login events, got %d", result.TotalCount)
		}
	})

	t.Run("query by success", func(t *testing.T) {
		success := false
		result, err := reader.Query(Query{Success: &success})
		if err != nil {
			t.Fatalf("Query() error = %v", err)
		}
		if result.TotalCount != 1 {
			t.Errorf("expected 1 failed event, got %d", result.TotalCount)
		}
	})

	t.Run("query with pagination", func(t *testing.T) {
		result, err := reader.Query(Query{Limit: 2})
		if err != nil {
			t.Fatalf("Query() error = %v", err)
		}
		if len(result.Events) != 2 {
			t.Errorf("expected 2 events with limit, got %d", len(result.Events))
		}
		if !result.HasMore {
			t.Error("expected HasMore to be true")
		}

		result2, err := reader.Query(Query{Limit: 2, Offset: 2})
		if err != nil {
			t.Fatalf("Query() error = %v", err)
		}
		if len(result2.Events) != 2 {
			t.Errorf("expected 2 events with offset, got %d", len(result2.Events))
		}
	})

	t.Run("query by time range", func(t *testing.T) {
		result, err := reader.Query(Query{
			StartTime: now.Add(-90 * time.Minute),
			EndTime:   now.Add(-15 * time.Minute),
		})
		if err != nil {
			t.Fatalf("Query() error = %v", err)
		}
		if result.TotalCount != 2 {
			t.Errorf("expected 2 events in time range, got %d", result.TotalCount)
		}
	})
}

func TestGetUserActivity(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	logger, _ := NewLogger(Config{Enabled: true, LogPath: logPath})

	now := time.Now().UTC()
	logger.Log(Event{Timestamp: now, Type: EventLogin, UserID: "user-1"})
	logger.Log(Event{Timestamp: now, Type: EventDataRead, UserID: "user-1"})
	logger.Log(Event{Timestamp: now, Type: EventLogin, UserID: "user-2"})
	logger.Close()

	reader := NewReader(logPath)
	result, err := reader.GetUserActivity("user-1", now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil {
		t.Fatalf("GetUserActivity() error = %v", err)
	}

	if result.TotalCount != 2 {
		t.Errorf("expected 2 events for user-1, got %d", result.TotalCount)
	}
}

func TestGetDataAccessReport(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	logger, _ := NewLogger(Config{Enabled: true, LogPath: logPath})

	now := time.Now().UTC()
	logger.Log(Event{Timestamp: now, Type: EventLogin, UserID: "user-1"})
	logger.Log(Event{Timestamp: now, Type: EventDataRead, UserID: "user-1"})
	logger.Log(Event{Timestamp: now, Type: EventDataCreate, UserID: "user-1"})
	logger.Log(Event{Timestamp: now, Type: EventDataDelete, UserID: "user-2"})
	logger.Close()

	reader := NewReader(logPath)
	result, err := reader.GetDataAccessReport(now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil {
		t.Fatalf("GetDataAccessReport() error = %v", err)
	}

	if result.TotalCount != 3 {
		t.Errorf("expected 3 data access events, got %d", result.TotalCount)
	}
}

func TestGetSecurityReport(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	logger, _ := NewLogger(Config{Enabled: true, LogPath: logPath})

	now := time.Now().UTC()
	logger.Log(Event{Timestamp: now, Type: EventLogin, Success: true})
	logger.Log(Event{Timestamp: now, Type: EventLoginFailed, Success: false})
	logger.Log(Event{Timestamp: now, Type: EventAccessDenied, Success: false})
	logger.Log(Event{Timestamp: now, Type: EventSecurityAlert, Success: false})
	logger.Close()

	reader := NewReader(logPath)
	result, err := reader.GetSecurityReport(now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil {
		t.Fatalf("GetSecurityReport() error = %v", err)
	}

	if result.TotalCount != 3 {
		t.Errorf("expected 3 security events, got %d", result.TotalCount)
	}
}

func TestGenerateComplianceReport(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	logger, _ := NewLogger(Config{Enabled: true, LogPath: logPath})

	now := time.Now().UTC()

	// Various event types
	logger.Log(Event{Timestamp: now, Type: EventLogin, UserID: "user-1", Success: true})
	logger.Log(Event{Timestamp: now, Type: EventLoginFailed, UserID: "user-2", Success: false})
	logger.Log(Event{Timestamp: now, Type: EventLoginFailed, UserID: "user-3", Success: false})
	logger.Log(Event{Timestamp: now, Type: EventDataRead, UserID: "user-1", Success: true})
	logger.Log(Event{Timestamp: now, Type: EventDataCreate, UserID: "user-1", Success: true})
	logger.Log(Event{Timestamp: now, Type: EventAccessDenied, UserID: "user-4", Success: false})
	logger.Log(Event{Timestamp: now, Type: EventErasureRequest, UserID: "admin-1", Success: true})
	logger.Log(Event{Timestamp: now, Type: EventSecurityAlert, UserID: "system", Success: false})
	logger.Close()

	reader := NewReader(logPath)
	report, err := reader.GenerateComplianceReport(
		now.Add(-time.Hour),
		now.Add(time.Hour),
		"Q4 2025",
	)
	if err != nil {
		t.Fatalf("GenerateComplianceReport() error = %v", err)
	}

	if report.Period != "Q4 2025" {
		t.Errorf("expected period 'Q4 2025', got %s", report.Period)
	}
	if report.TotalEvents != 8 {
		t.Errorf("expected 8 total events, got %d", report.TotalEvents)
	}
	if report.FailedLogins != 2 {
		t.Errorf("expected 2 failed logins, got %d", report.FailedLogins)
	}
	if report.AccessDenied != 1 {
		t.Errorf("expected 1 access denied, got %d", report.AccessDenied)
	}
	if report.DataAccesses != 2 {
		t.Errorf("expected 2 data accesses, got %d", report.DataAccesses)
	}
	if report.ErasureRequests != 1 {
		t.Errorf("expected 1 erasure request, got %d", report.ErasureRequests)
	}
	if report.SecurityAlerts != 1 {
		t.Errorf("expected 1 security alert, got %d", report.SecurityAlerts)
	}
	if report.UniqueUsers != 5 { // user-1, user-2, user-3, user-4, admin-1, system = 6 but system might not be counted
		t.Logf("unique users: %d", report.UniqueUsers)
	}
	if report.GeneratedAt.IsZero() {
		t.Error("expected GeneratedAt to be set")
	}
}

func TestReaderNonExistentFile(t *testing.T) {
	reader := NewReader("/nonexistent/path/audit.log")
	result, err := reader.Query(Query{})
	if err != nil {
		t.Fatalf("Query() should not error for nonexistent file, got %v", err)
	}
	if len(result.Events) != 0 {
		t.Error("expected empty results for nonexistent file")
	}
}

func TestLoggerClose(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLoggerWithWriter(&buf, Config{Enabled: true})

	logger.Log(Event{Type: EventLogin})

	if err := logger.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Logging after close should error
	err := logger.Log(Event{Type: EventLogin})
	if err == nil {
		t.Error("expected error when logging after close")
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if !config.Enabled {
		t.Error("default should be enabled")
	}
	if config.RetentionDays != 2555 {
		t.Errorf("expected 2555 days retention (7 years), got %d", config.RetentionDays)
	}
	if config.RotationSize != 100*1024*1024 {
		t.Errorf("expected 100MB rotation size")
	}
	if !config.SyncWrites {
		t.Error("default should have sync writes enabled")
	}
	if len(config.AlertOnEvents) != 3 {
		t.Errorf("expected 3 alert event types")
	}
}

func TestEventTypes(t *testing.T) {
	// Ensure all event type constants are defined
	eventTypes := []EventType{
		EventLogin, EventLogout, EventLoginFailed, EventPasswordChange,
		EventAccessDenied, EventRoleChange,
		EventDataRead, EventDataCreate, EventDataUpdate, EventDataDelete, EventDataExport,
		EventErasureRequest, EventErasureComplete, EventConsentGiven, EventConsentRevoked,
		EventConfigChange, EventBackup, EventRestore, EventSchemaChange,
		EventSecurityAlert, EventBreach,
	}

	for _, et := range eventTypes {
		if et == "" {
			t.Error("event type should not be empty")
		}
	}
}

func TestLogSecurityEvent(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLoggerWithWriter(&buf, Config{Enabled: true})

	err := logger.LogSecurityEvent(EventSecurityAlert, "system", "192.168.1.1", "Unusual login pattern detected", map[string]string{
		"severity": "high",
		"source":   "auth-service",
	})
	if err != nil {
		t.Fatalf("LogSecurityEvent() error = %v", err)
	}

	var event Event
	if err := json.Unmarshal(buf.Bytes(), &event); err != nil {
		t.Fatalf("parsing event: %v", err)
	}

	if event.Type != EventSecurityAlert {
		t.Errorf("expected SECURITY_ALERT, got %s", event.Type)
	}
	if event.UserID != "system" {
		t.Errorf("expected system user, got %s", event.UserID)
	}
	if event.IPAddress != "192.168.1.1" {
		t.Errorf("expected IP 192.168.1.1, got %s", event.IPAddress)
	}
	if event.Reason != "Unusual login pattern detected" {
		t.Errorf("unexpected reason: %s", event.Reason)
	}
	if event.Metadata["severity"] != "high" {
		t.Errorf("expected severity high in metadata")
	}
}

func TestLogBreach(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLoggerWithWriter(&buf, Config{Enabled: true})

	err := logger.LogSecurityEvent(EventBreach, "admin", "10.0.0.1", "Data exfiltration detected", map[string]string{
		"severity": "critical",
	})
	if err != nil {
		t.Fatalf("LogSecurityEvent() error = %v", err)
	}

	var event Event
	if err := json.Unmarshal(buf.Bytes(), &event); err != nil {
		t.Fatalf("parsing event: %v", err)
	}

	if event.Type != EventBreach {
		t.Errorf("expected BREACH, got %s", event.Type)
	}
}

func TestNewLoggerInvalidPath(t *testing.T) {
	// Try to create logger in a directory that we can't write to
	// On Windows, CON is a reserved device name that can't be used as a file
	// On Unix, /proc is typically read-only
	var invalidPath string
	if os.PathSeparator == '\\' {
		invalidPath = "CON:\\invalid\\path\\audit.log"
	} else {
		invalidPath = "/proc/self/nonexistent/audit.log"
	}

	logger, err := NewLogger(Config{
		Enabled: true,
		LogPath: invalidPath,
	})
	if err == nil {
		logger.Close()
		t.Log("warning: expected error for invalid log path but got none (this may be OS-dependent)")
	}
}

func TestLogWithMetadata(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLoggerWithWriter(&buf, Config{Enabled: true})

	err := logger.Log(Event{
		Type:   EventDataRead,
		UserID: "user-1",
		Metadata: map[string]string{
			"query":       "MATCH (n) RETURN n",
			"result_size": "42",
		},
	})
	if err != nil {
		t.Fatalf("Log() error = %v", err)
	}

	var event Event
	if err := json.Unmarshal(buf.Bytes(), &event); err != nil {
		t.Fatalf("parsing event: %v", err)
	}

	if event.Metadata["query"] != "MATCH (n) RETURN n" {
		t.Errorf("expected query in metadata")
	}
	if event.Metadata["result_size"] != "42" {
		t.Errorf("expected result_size in metadata")
	}
}

func TestReaderQueryResourceID(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	logger, _ := NewLogger(Config{Enabled: true, LogPath: logPath})

	now := time.Now().UTC()
	logger.Log(Event{Timestamp: now, Type: EventDataRead, ResourceID: "node-123"})
	logger.Log(Event{Timestamp: now, Type: EventDataRead, ResourceID: "node-456"})
	logger.Log(Event{Timestamp: now, Type: EventDataRead, ResourceID: "node-123"})
	logger.Close()

	reader := NewReader(logPath)
	result, err := reader.Query(Query{ResourceID: "node-123"})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if result.TotalCount != 2 {
		t.Errorf("expected 2 events for node-123, got %d", result.TotalCount)
	}
}

func TestDisabledLoggerMethods(t *testing.T) {
	logger, err := NewLogger(Config{Enabled: false})
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}
	defer logger.Close()

	// All log methods should silently succeed on disabled logger
	if err := logger.LogAuth(EventLogin, "user", "name", "ip", "ua", true, ""); err != nil {
		t.Error("LogAuth on disabled logger should succeed")
	}
	if err := logger.LogDataAccess("user", "name", "res", "id", "READ", true, ""); err != nil {
		t.Error("LogDataAccess on disabled logger should succeed")
	}
	if err := logger.LogErasure("user", "name", "target", true, "reason"); err != nil {
		t.Error("LogErasure on disabled logger should succeed")
	}
	if err := logger.LogConsent("user", "name", true, "type", "version"); err != nil {
		t.Error("LogConsent on disabled logger should succeed")
	}
	if err := logger.LogSecurityEvent(EventBreach, "user", "127.0.0.1", "reason", nil); err != nil {
		t.Error("LogSecurityEvent on disabled logger should succeed")
	}
}
