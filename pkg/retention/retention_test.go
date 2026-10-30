// Package retention tests for data lifecycle management.
package retention

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPolicyValidate(t *testing.T) {
	t.Run("valid policy", func(t *testing.T) {
		p := &Policy{
			ID:       "test-1",
			Category: CategoryUser,
			RetentionPeriod: RetentionPeriod{
				Duration: 24 * time.Hour,
			},
			Active: true,
		}
		if err := p.Validate(); err != nil {
			t.Errorf("valid policy should not error: %v", err)
		}
	})

	t.Run("indefinite policy", func(t *testing.T) {
		p := &Policy{
			ID:       "test-2",
			Category: CategorySystem,
			RetentionPeriod: RetentionPeriod{
				Indefinite: true,
			},
			Active: true,
		}
		if err := p.Validate(); err != nil {
			t.Errorf("indefinite policy should be valid: %v", err)
		}
	})

	t.Run("missing ID", func(t *testing.T) {
		p := &Policy{
			Category: CategoryUser,
			RetentionPeriod: RetentionPeriod{
				Duration: time.Hour,
			},
		}
		if err := p.Validate(); err == nil {
			t.Error("expected error for missing ID")
		}
	})

	t.Run("missing category", func(t *testing.T) {
		p := &Policy{
			ID: "test",
			RetentionPeriod: RetentionPeriod{
				Duration: time.Hour,
			},
		}
		if err := p.Validate(); err == nil {
			t.Error("expected error for missing category")
		}
	})

	t.Run("missing retention period", func(t *testing.T) {
		p := &Policy{
			ID:       "test",
			Category: CategoryUser,
		}
		if err := p.Validate(); err == nil {
			t.Error("expected error for missing retention period")
		}
	})

	t.Run("archive without path", func(t *testing.T) {
		p := &Policy{
			ID:       "test",
			Category: CategoryUser,
			RetentionPeriod: RetentionPeriod{
				Duration: time.Hour,
			},
			ArchiveBeforeDelete: true,
		}
		if err := p.Validate(); err == nil {
			t.Error("expected error for archive without path")
		}
	})
}

func TestPolicyIsExpired(t *testing.T) {
	t.Run("not expired", func(t *testing.T) {
		p := &Policy{
			RetentionPeriod: RetentionPeriod{
				Duration: 24 * time.Hour,
			},
		}
		if p.IsExpired(time.Now()) {
			t.Error("recently created should not be expired")
		}
	})

	t.Run("expired", func(t *testing.T) {
		p := &Policy{
			RetentionPeriod: RetentionPeriod{
				Duration: time.Hour,
			},
		}
		if !p.IsExpired(time.Now().Add(-2 * time.Hour)) {
			t.Error("old data should be expired")
		}
	})

	t.Run("indefinite never expires", func(t *testing.T) {
		p := &Policy{
			RetentionPeriod: RetentionPeriod{
				Indefinite: true,
			},
		}
		if p.IsExpired(time.Now().Add(-100 * 365 * 24 * time.Hour)) {
			t.Error("indefinite retention should never expire")
		}
	})
}

func TestLegalHold(t *testing.T) {
	t.Run("active hold", func(t *testing.T) {
		h := &LegalHold{
			ID:     "hold-1",
			Active: true,
		}
		if !h.IsActive() {
			t.Error("should be active")
		}
	})

	t.Run("inactive hold", func(t *testing.T) {
		h := &LegalHold{
			ID:     "hold-1",
			Active: false,
		}
		if h.IsActive() {
			t.Error("should be inactive")
		}
	})

	t.Run("expired hold", func(t *testing.T) {
		h := &LegalHold{
			ID:        "hold-1",
			Active:    true,
			ExpiresAt: time.Now().Add(-time.Hour),
		}
		if h.IsActive() {
			t.Error("expired hold should not be active")
		}
	})

	t.Run("future expiry hold", func(t *testing.T) {
		h := &LegalHold{
			ID:        "hold-1",
			Active:    true,
			ExpiresAt: time.Now().Add(time.Hour),
		}
		if !h.IsActive() {
			t.Error("future expiry should be active")
		}
	})
}

func TestLegalHoldCoversData(t *testing.T) {
	t.Run("covers all when empty", func(t *testing.T) {
		h := &LegalHold{
			ID:         "hold-1",
			Active:     true,
			SubjectIDs: []string{}, // Empty = all subjects
			Categories: []DataCategory{},
		}
		if !h.CoversData("any-user", CategoryUser) {
			t.Error("empty filters should cover all data")
		}
	})

	t.Run("covers specific subject", func(t *testing.T) {
		h := &LegalHold{
			ID:         "hold-1",
			Active:     true,
			SubjectIDs: []string{"user-1", "user-2"},
		}
		if !h.CoversData("user-1", CategoryUser) {
			t.Error("should cover user-1")
		}
		if h.CoversData("user-3", CategoryUser) {
			t.Error("should not cover user-3")
		}
	})

	t.Run("covers specific category", func(t *testing.T) {
		h := &LegalHold{
			ID:         "hold-1",
			Active:     true,
			Categories: []DataCategory{CategoryPHI, CategoryPII},
		}
		if !h.CoversData("any-user", CategoryPHI) {
			t.Error("should cover PHI")
		}
		if h.CoversData("any-user", CategoryUser) {
			t.Error("should not cover USER category")
		}
	})

	t.Run("inactive hold covers nothing", func(t *testing.T) {
		h := &LegalHold{
			ID:     "hold-1",
			Active: false,
		}
		if h.CoversData("user-1", CategoryUser) {
			t.Error("inactive hold should cover nothing")
		}
	})
}

func TestManager(t *testing.T) {
	m := NewManager()

	t.Run("add policy", func(t *testing.T) {
		p := &Policy{
			ID:       "policy-1",
			Name:     "Test Policy",
			Category: CategoryUser,
			RetentionPeriod: RetentionPeriod{
				Duration: 24 * time.Hour,
			},
			Active: true,
		}
		if err := m.AddPolicy(p); err != nil {
			t.Fatalf("AddPolicy() error = %v", err)
		}
	})

	t.Run("add duplicate policy", func(t *testing.T) {
		p := &Policy{
			ID:       "policy-1",
			Category: CategoryUser,
			RetentionPeriod: RetentionPeriod{
				Duration: 48 * time.Hour,
			},
		}
		if err := m.AddPolicy(p); err != ErrAlreadyExists {
			t.Errorf("expected ErrAlreadyExists, got %v", err)
		}
	})

	t.Run("get policy", func(t *testing.T) {
		p, err := m.GetPolicy("policy-1")
		if err != nil {
			t.Fatalf("GetPolicy() error = %v", err)
		}
		if p.Name != "Test Policy" {
			t.Errorf("expected 'Test Policy', got %s", p.Name)
		}
	})

	t.Run("get nonexistent policy", func(t *testing.T) {
		_, err := m.GetPolicy("nonexistent")
		if err != ErrPolicyNotFound {
			t.Errorf("expected ErrPolicyNotFound, got %v", err)
		}
	})

	t.Run("update policy", func(t *testing.T) {
		p, _ := m.GetPolicy("policy-1")
		p.Name = "Updated Policy"
		if err := m.UpdatePolicy(p); err != nil {
			t.Fatalf("UpdatePolicy() error = %v", err)
		}

		updated, _ := m.GetPolicy("policy-1")
		if updated.Name != "Updated Policy" {
			t.Error("policy not updated")
		}
	})

	t.Run("update nonexistent policy", func(t *testing.T) {
		p := &Policy{
			ID:       "nonexistent",
			Category: CategoryUser,
			RetentionPeriod: RetentionPeriod{
				Duration: time.Hour,
			},
		}
		if err := m.UpdatePolicy(p); err != ErrPolicyNotFound {
			t.Errorf("expected ErrPolicyNotFound, got %v", err)
		}
	})

	t.Run("list policies", func(t *testing.T) {
		policies := m.ListPolicies()
		if len(policies) != 1 {
			t.Errorf("expected 1 policy, got %d", len(policies))
		}
	})

	t.Run("delete policy", func(t *testing.T) {
		if err := m.DeletePolicy("policy-1"); err != nil {
			t.Fatalf("DeletePolicy() error = %v", err)
		}
		if len(m.ListPolicies()) != 0 {
			t.Error("policy not deleted")
		}
	})

	t.Run("delete nonexistent policy", func(t *testing.T) {
		if err := m.DeletePolicy("nonexistent"); err != ErrPolicyNotFound {
			t.Errorf("expected ErrPolicyNotFound, got %v", err)
		}
	})
}

func TestManagerDefaultPolicy(t *testing.T) {
	m := NewManager()

	defaultPolicy := &Policy{
		ID:       "default",
		Name:     "Default",
		Category: CategoryUser,
		RetentionPeriod: RetentionPeriod{
			Duration: 30 * 24 * time.Hour,
		},
		Active: true,
	}
	if err := m.SetDefaultPolicy(defaultPolicy); err != nil {
		t.Fatalf("SetDefaultPolicy() error = %v", err)
	}

	// Query for category without specific policy
	p, err := m.GetPolicyForCategory(CategoryAnalytics)
	if err != nil {
		t.Fatalf("GetPolicyForCategory() error = %v", err)
	}
	if p.ID != "default" {
		t.Error("should return default policy")
	}
}

func TestManagerLegalHolds(t *testing.T) {
	m := NewManager()

	t.Run("place hold", func(t *testing.T) {
		h := &LegalHold{
			ID:          "hold-1",
			Description: "Test Hold",
			PlacedBy:    "admin",
			SubjectIDs:  []string{"user-1"},
		}
		if err := m.PlaceLegalHold(h); err != nil {
			t.Fatalf("PlaceLegalHold() error = %v", err)
		}

		retrieved, _ := m.GetLegalHold("hold-1")
		if !retrieved.Active {
			t.Error("hold should be active")
		}
	})

	t.Run("place hold without ID", func(t *testing.T) {
		h := &LegalHold{}
		if err := m.PlaceLegalHold(h); err == nil {
			t.Error("expected error for missing ID")
		}
	})

	t.Run("is under legal hold", func(t *testing.T) {
		if !m.IsUnderLegalHold("user-1", CategoryUser) {
			t.Error("user-1 should be under hold")
		}
		if m.IsUnderLegalHold("user-2", CategoryUser) {
			t.Error("user-2 should not be under hold")
		}
	})

	t.Run("list holds", func(t *testing.T) {
		holds := m.ListLegalHolds()
		if len(holds) != 1 {
			t.Errorf("expected 1 hold, got %d", len(holds))
		}
	})

	t.Run("release hold", func(t *testing.T) {
		if err := m.ReleaseLegalHold("hold-1"); err != nil {
			t.Fatalf("ReleaseLegalHold() error = %v", err)
		}
		if m.IsUnderLegalHold("user-1", CategoryUser) {
			t.Error("user-1 should no longer be under hold")
		}
	})

	t.Run("release nonexistent hold", func(t *testing.T) {
		if err := m.ReleaseLegalHold("nonexistent"); err != ErrPolicyNotFound {
			t.Errorf("expected ErrPolicyNotFound, got %v", err)
		}
	})

	t.Run("get nonexistent hold", func(t *testing.T) {
		_, err := m.GetLegalHold("nonexistent")
		if err != ErrPolicyNotFound {
			t.Errorf("expected ErrPolicyNotFound, got %v", err)
		}
	})
}

func TestManagerShouldDelete(t *testing.T) {
	m := NewManager()

	// Add policy
	p := &Policy{
		ID:       "user-policy",
		Category: CategoryUser,
		RetentionPeriod: RetentionPeriod{
			Duration: time.Hour,
		},
		Active: true,
	}
	m.AddPolicy(p)

	t.Run("should delete expired", func(t *testing.T) {
		record := &DataRecord{
			ID:        "rec-1",
			Category:  CategoryUser,
			CreatedAt: time.Now().Add(-2 * time.Hour),
		}
		shouldDelete, reason := m.ShouldDelete(record)
		if !shouldDelete {
			t.Errorf("should delete expired record, reason: %s", reason)
		}
	})

	t.Run("should not delete within period", func(t *testing.T) {
		record := &DataRecord{
			ID:        "rec-2",
			Category:  CategoryUser,
			CreatedAt: time.Now(),
		}
		shouldDelete, reason := m.ShouldDelete(record)
		if shouldDelete {
			t.Errorf("should not delete recent record, reason: %s", reason)
		}
	})

	t.Run("should not delete under legal hold", func(t *testing.T) {
		m.PlaceLegalHold(&LegalHold{
			ID:         "hold-2",
			Active:     true,
			SubjectIDs: []string{"user-held"},
		})

		record := &DataRecord{
			ID:        "rec-3",
			SubjectID: "user-held",
			Category:  CategoryUser,
			CreatedAt: time.Now().Add(-2 * time.Hour),
		}
		shouldDelete, reason := m.ShouldDelete(record)
		if shouldDelete {
			t.Errorf("should not delete record under legal hold, reason: %s", reason)
		}
		if reason != "under legal hold" {
			t.Errorf("expected 'under legal hold' reason, got %s", reason)
		}
	})

	t.Run("no policy found", func(t *testing.T) {
		record := &DataRecord{
			ID:       "rec-4",
			Category: CategoryFinancial, // No policy for this
		}
		shouldDelete, reason := m.ShouldDelete(record)
		if shouldDelete {
			t.Error("should not delete without policy")
		}
		if reason != "no policy found" {
			t.Errorf("expected 'no policy found', got %s", reason)
		}
	})
}

func TestManagerProcessRecord(t *testing.T) {
	m := NewManager()

	var deletedRecords []string
	var archivedRecords []string

	m.SetDeleteCallback(func(record *DataRecord) error {
		deletedRecords = append(deletedRecords, record.ID)
		return nil
	})
	m.SetArchiveCallback(func(record *DataRecord, path string) error {
		archivedRecords = append(archivedRecords, record.ID)
		return nil
	})

	// Add policy with archiving
	p := &Policy{
		ID:       "user-archive",
		Category: CategoryUser,
		RetentionPeriod: RetentionPeriod{
			Duration: time.Hour,
		},
		ArchiveBeforeDelete: true,
		ArchivePath:         "/archive",
		Active:              true,
	}
	m.AddPolicy(p)

	t.Run("process expired record", func(t *testing.T) {
		record := &DataRecord{
			ID:        "rec-archive",
			Category:  CategoryUser,
			CreatedAt: time.Now().Add(-2 * time.Hour),
		}
		if err := m.ProcessRecord(context.Background(), record); err != nil {
			t.Fatalf("ProcessRecord() error = %v", err)
		}

		if len(archivedRecords) != 1 || archivedRecords[0] != "rec-archive" {
			t.Error("record should have been archived")
		}
		if len(deletedRecords) != 1 || deletedRecords[0] != "rec-archive" {
			t.Error("record should have been deleted")
		}
	})

	t.Run("process non-expired record", func(t *testing.T) {
		deletedRecords = nil
		archivedRecords = nil

		record := &DataRecord{
			ID:        "rec-keep",
			Category:  CategoryUser,
			CreatedAt: time.Now(),
		}
		if err := m.ProcessRecord(context.Background(), record); err != nil {
			t.Fatalf("ProcessRecord() error = %v", err)
		}

		if len(deletedRecords) != 0 {
			t.Error("recent record should not be deleted")
		}
	})
}

func TestErasureRequest(t *testing.T) {
	m := NewManager()

	t.Run("create erasure request", func(t *testing.T) {
		req, err := m.CreateErasureRequest("user-123", "user@example.com")
		if err != nil {
			t.Fatalf("CreateErasureRequest() error = %v", err)
		}
		if req.SubjectID != "user-123" {
			t.Error("wrong subject ID")
		}
		if req.Status != ErasureStatusPending {
			t.Error("should be pending")
		}
		if req.Deadline.IsZero() {
			t.Error("should have deadline")
		}
	})

	t.Run("get erasure request", func(t *testing.T) {
		reqs := m.ListErasureRequests()
		if len(reqs) != 1 {
			t.Fatalf("expected 1 request, got %d", len(reqs))
		}

		req, err := m.GetErasureRequest(reqs[0].ID)
		if err != nil {
			t.Fatalf("GetErasureRequest() error = %v", err)
		}
		if req.SubjectEmail != "user@example.com" {
			t.Error("wrong email")
		}
	})

	t.Run("get nonexistent request", func(t *testing.T) {
		_, err := m.GetErasureRequest("nonexistent")
		if err != ErrPolicyNotFound {
			t.Errorf("expected ErrPolicyNotFound, got %v", err)
		}
	})
}

func TestProcessErasure(t *testing.T) {
	m := NewManager()

	var deletedRecords []string
	m.SetDeleteCallback(func(record *DataRecord) error {
		deletedRecords = append(deletedRecords, record.ID)
		return nil
	})

	t.Run("process erasure request", func(t *testing.T) {
		req, _ := m.CreateErasureRequest("user-erase", "user@example.com")

		records := []*DataRecord{
			{ID: "rec-1", SubjectID: "user-erase", Category: CategoryUser},
			{ID: "rec-2", SubjectID: "user-erase", Category: CategoryUser},
		}

		err := m.ProcessErasure(context.Background(), req.ID, records)
		if err != nil {
			t.Fatalf("ProcessErasure() error = %v", err)
		}

		updated, _ := m.GetErasureRequest(req.ID)
		if updated.Status != ErasureStatusCompleted {
			t.Errorf("expected COMPLETED, got %s", updated.Status)
		}
		if updated.ItemsErased != 2 {
			t.Errorf("expected 2 erased, got %d", updated.ItemsErased)
		}
		if len(deletedRecords) != 2 {
			t.Error("records should be deleted")
		}
	})

	t.Run("process erasure with legal hold", func(t *testing.T) {
		m.PlaceLegalHold(&LegalHold{
			ID:         "hold-erasure",
			Active:     true,
			SubjectIDs: []string{"user-held-erasure"},
		})

		req, _ := m.CreateErasureRequest("user-held-erasure", "held@example.com")
		deletedRecords = nil

		records := []*DataRecord{
			{ID: "rec-held-1", SubjectID: "user-held-erasure", Category: CategoryUser},
			{ID: "rec-held-2", SubjectID: "user-held-erasure", Category: CategoryUser},
		}

		err := m.ProcessErasure(context.Background(), req.ID, records)
		if err != nil {
			t.Fatalf("ProcessErasure() error = %v", err)
		}

		updated, _ := m.GetErasureRequest(req.ID)
		if updated.Status != ErasureStatusPartial {
			t.Errorf("expected PARTIAL, got %s", updated.Status)
		}
		if updated.ItemsRetained != 2 {
			t.Errorf("expected 2 retained, got %d", updated.ItemsRetained)
		}
		if len(deletedRecords) != 0 {
			t.Error("records should NOT be deleted (legal hold)")
		}
	})

	t.Run("process erasure cancelled", func(t *testing.T) {
		req, _ := m.CreateErasureRequest("user-cancel", "cancel@example.com")

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		records := []*DataRecord{
			{ID: "rec-cancel", SubjectID: "user-cancel", Category: CategoryUser},
		}

		err := m.ProcessErasure(ctx, req.ID, records)
		if err == nil {
			t.Error("expected context cancelled error")
		}
	})

	t.Run("process nonexistent request", func(t *testing.T) {
		err := m.ProcessErasure(context.Background(), "nonexistent", nil)
		if err != ErrPolicyNotFound {
			t.Errorf("expected ErrPolicyNotFound, got %v", err)
		}
	})
}

func TestSaveLoadPolicies(t *testing.T) {
	tmpDir := t.TempDir()
	policyFile := filepath.Join(tmpDir, "policies.json")

	m := NewManager()
	m.AddPolicy(&Policy{
		ID:       "save-test",
		Name:     "Save Test",
		Category: CategoryUser,
		RetentionPeriod: RetentionPeriod{
			Duration: 24 * time.Hour,
		},
		Active: true,
	})

	t.Run("save policies", func(t *testing.T) {
		if err := m.SavePolicies(policyFile); err != nil {
			t.Fatalf("SavePolicies() error = %v", err)
		}

		// Verify file exists
		if _, err := os.Stat(policyFile); os.IsNotExist(err) {
			t.Error("policy file not created")
		}

		// Verify content
		data, _ := os.ReadFile(policyFile)
		var policies []*Policy
		json.Unmarshal(data, &policies)
		if len(policies) != 1 {
			t.Errorf("expected 1 policy in file, got %d", len(policies))
		}
	})

	t.Run("load policies", func(t *testing.T) {
		m2 := NewManager()
		if err := m2.LoadPolicies(policyFile); err != nil {
			t.Fatalf("LoadPolicies() error = %v", err)
		}

		policies := m2.ListPolicies()
		if len(policies) != 1 {
			t.Errorf("expected 1 loaded policy, got %d", len(policies))
		}
		if policies[0].ID != "save-test" {
			t.Error("wrong policy loaded")
		}
	})

	t.Run("load nonexistent file", func(t *testing.T) {
		m2 := NewManager()
		err := m2.LoadPolicies("/nonexistent/path/policies.json")
		if err == nil {
			t.Error("expected error for nonexistent file")
		}
	})
}

func TestDefaultPolicies(t *testing.T) {
	policies := DefaultPolicies()

	if len(policies) == 0 {
		t.Error("should have default policies")
	}

	// Check specific expected policies
	categories := make(map[DataCategory]bool)
	for _, p := range policies {
		categories[p.Category] = true
		if err := p.Validate(); err != nil {
			t.Errorf("default policy %s invalid: %v", p.ID, err)
		}
	}

	expected := []DataCategory{CategoryAudit, CategoryPHI, CategoryPII, CategoryFinancial, CategoryUser}
	for _, cat := range expected {
		if !categories[cat] {
			t.Errorf("missing default policy for %s", cat)
		}
	}
}

func TestDataCategories(t *testing.T) {
	categories := []DataCategory{
		CategorySystem, CategoryAudit, CategoryUser, CategoryAnalytics,
		CategoryBackup, CategoryArchive, CategoryPHI, CategoryPII,
		CategoryFinancial, CategoryLegal,
	}

	for _, cat := range categories {
		if cat == "" {
			t.Error("category should not be empty")
		}
	}
}

func TestErasureStatus(t *testing.T) {
	statuses := []ErasureStatus{
		ErasureStatusPending, ErasureStatusInProgress,
		ErasureStatusCompleted, ErasureStatusFailed, ErasureStatusPartial,
	}

	for _, s := range statuses {
		if s == "" {
			t.Error("status should not be empty")
		}
	}
}

func TestAddInvalidPolicy(t *testing.T) {
	m := NewManager()

	// Missing required fields
	p := &Policy{}
	if err := m.AddPolicy(p); err == nil {
		t.Error("expected error for invalid policy")
	}
}

func TestSetInvalidDefaultPolicy(t *testing.T) {
	m := NewManager()

	p := &Policy{} // Invalid
	if err := m.SetDefaultPolicy(p); err == nil {
		t.Error("expected error for invalid default policy")
	}
}

func TestGetPolicyForCategoryNoPolicy(t *testing.T) {
	m := NewManager()

	_, err := m.GetPolicyForCategory(CategoryUser)
	if err != ErrPolicyNotFound {
		t.Errorf("expected ErrPolicyNotFound, got %v", err)
	}
}

func TestDuplicateErasureRequest(t *testing.T) {
	m := NewManager()

	// Create first request
	req, _ := m.CreateErasureRequest("user-dup", "user@example.com")

	// Start processing (set to in progress)
	m.mu.Lock()
	req.Status = ErasureStatusInProgress
	m.mu.Unlock()

	// Try to create duplicate
	_, err := m.CreateErasureRequest("user-dup", "user@example.com")
	if err != ErrErasureInProgress {
		t.Errorf("expected ErrErasureInProgress, got %v", err)
	}
}

func TestPolicyInactive(t *testing.T) {
	m := NewManager()

	// Add active policy
	activePolicy := &Policy{
		ID:       "active-policy",
		Category: CategoryUser,
		RetentionPeriod: RetentionPeriod{
			Duration: time.Hour,
		},
		Active: true,
	}
	m.AddPolicy(activePolicy)

	// Add inactive policy for same category - should NOT be used
	inactivePolicy := &Policy{
		ID:       "inactive-policy",
		Category: CategoryUser,
		RetentionPeriod: RetentionPeriod{
			Duration: 24 * time.Hour, // Longer retention
		},
		Active: false,
	}
	m.AddPolicy(inactivePolicy)

	record := &DataRecord{
		ID:        "rec-inactive",
		Category:  CategoryUser,
		CreatedAt: time.Now().Add(-2 * time.Hour), // Older than active policy
	}

	// Should use active policy, which has 1 hour retention
	shouldDelete, reason := m.ShouldDelete(record)
	if !shouldDelete {
		t.Errorf("should delete using active policy, reason: %s", reason)
	}

	// Now test with only inactive policy (remove active one)
	m.DeletePolicy("active-policy")

	shouldDelete, reason = m.ShouldDelete(record)
	if shouldDelete {
		t.Error("should not delete with no active policy")
	}
	if reason != "no policy found" {
		t.Errorf("expected 'no policy found', got %s", reason)
	}
}
