package nornicdb

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/retention"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	defaultRetentionSweepInterval   = time.Hour * 24
	defaultRetentionMaxSweepRecords = 50000
)

func (db *DB) startRetentionSweep(ctx context.Context) {
	if db.retentionManager == nil {
		return
	}
	ctx = db.retentionContext(ctx)

	interval := defaultRetentionSweepInterval
	if db.config != nil {
		if seconds := db.config.Retention.SweepIntervalSeconds; seconds > 0 {
			interval = time.Duration(seconds) * time.Second
		}
	}

	started := db.startBackgroundTask(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		log.Printf("🔄 Retention sweep started (interval: %s)", interval)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				db.runRetentionSweep(ctx)
			}
		}
	})
	if !started {
		log.Printf("⚠️  Retention sweep did not start because database is closed")
	}
}

func (db *DB) runRetentionSweep(ctx context.Context) {
	ctx = db.retentionContext(ctx)
	rm := db.retentionManager
	if rm == nil {
		return
	}

	excludedLabels := db.retentionExcludedLabels()
	budget := db.retentionMaxSweepRecords()
	processed := 0
	deleted := 0
	skippedExcluded := 0
	sweepStart := time.Now()
	dbName := db.defaultDatabaseName()

	err := storage.StreamNodesWithFallback(ctx, db.storage, 1000, func(node *storage.Node) error {
		if processed >= budget {
			return localizedError(localization.NornicDBRetentionSweepBudgetExhausted(budget), nil)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if hasExcludedLabel(node, excludedLabels) {
			skippedExcluded++
			processed++
			return nil
		}

		record := nodeToDataRecord(node, db.subjectIdentifierProperties())
		shouldDelete, _ := rm.ShouldDelete(record)
		if shouldDelete {
			if err := rm.ProcessRecord(ctx, record); err != nil {
				log.Printf("⚠️  Retention: failed to process %s: %v", node.ID, err)
			} else {
				deleted++
				if idxErr := db.removeNodeFromSearchIndexes(ctx, dbName, db.storage, node.ID); idxErr != nil && !db.shouldIgnoreSearchIndexingError(idxErr) {
					log.Printf("⚠️  Retention: failed to remove %s from search indexes: %v", node.ID, idxErr)
				}
			}
		}
		processed++
		return nil
	})

	sweepDuration := time.Since(sweepStart)
	if err != nil && ctx.Err() == nil {
		log.Printf("📋 Retention sweep paused: %v (will resume next interval)", err)
	}
	if deleted > 0 || skippedExcluded > 0 {
		log.Printf("📋 Retention sweep: processed=%d, deleted=%d, excluded=%d, duration=%s",
			processed, deleted, skippedExcluded, sweepDuration.Round(time.Millisecond))
	}
}

// RunRetentionSweep triggers a single retention sweep immediately.
func (db *DB) RunRetentionSweep(ctx context.Context) {
	db.runRetentionSweep(ctx)
}

func (db *DB) retentionContext(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	if db != nil && db.buildCtx != nil {
		return db.buildCtx
	}
	log.Printf("⚠️  Retention: no lifecycle context available, using background context")
	return context.Background()
}

// CollectSubjectRetentionRecords returns retention records for all nodes owned by a subject.
func (db *DB) CollectSubjectRetentionRecords(ctx context.Context, subjectID string) ([]*retention.DataRecord, error) {
	var records []*retention.DataRecord
	err := storage.StreamNodesWithFallback(ctx, db.storage, 1000, func(node *storage.Node) error {
		if db.nodeMatchesSubject(node, subjectID) {
			records = append(records, nodeToDataRecord(node, db.subjectIdentifierProperties()))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (db *DB) retentionExcludedLabels() map[string]struct{} {
	if db == nil || db.config == nil || len(db.config.Retention.ExcludedLabels) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(db.config.Retention.ExcludedLabels))
	for _, label := range db.config.Retention.ExcludedLabels {
		trimmed := strings.TrimSpace(label)
		if trimmed != "" {
			set[trimmed] = struct{}{}
		}
	}
	return set
}

func (db *DB) retentionMaxSweepRecords() int {
	if db != nil && db.config != nil && db.config.Retention.MaxSweepRecords > 0 {
		return db.config.Retention.MaxSweepRecords
	}
	return defaultRetentionMaxSweepRecords
}

func hasExcludedLabel(node *storage.Node, excluded map[string]struct{}) bool {
	if node == nil || len(excluded) == 0 {
		return false
	}
	for _, label := range node.Labels {
		if _, ok := excluded[label]; ok {
			return true
		}
	}
	return false
}

func nodeToDataRecord(node *storage.Node, subjectKeys []string) *retention.DataRecord {
	record := &retention.DataRecord{
		ID:        string(node.ID),
		CreatedAt: node.CreatedAt,
		Category:  inferCategory(node),
		Metadata:  make(map[string]string),
	}

	for _, key := range subjectKeys {
		if value, ok := node.Properties[key]; ok {
			record.SubjectID = fmt.Sprintf("%v", value)
			break
		}
	}

	if node.UpdatedAt.After(node.CreatedAt) {
		record.LastAccessedAt = node.UpdatedAt
	}

	return record
}

func inferCategory(node *storage.Node) retention.DataCategory {
	for _, label := range node.Labels {
		switch label {
		case "AuditLog", "Audit":
			return retention.CategoryAudit
		case "PHI", "HealthRecord":
			return retention.CategoryPHI
		case "PII", "PersonalData":
			return retention.CategoryPII
		case "Financial", "Transaction":
			return retention.CategoryFinancial
		case "Analytics", "Metric", "Telemetry":
			return retention.CategoryAnalytics
		case "System", "Config", "Schema":
			return retention.CategorySystem
		case "Archive":
			return retention.CategoryArchive
		case "Backup":
			return retention.CategoryBackup
		case "Legal", "LegalDocument":
			return retention.CategoryLegal
		}
	}

	if cat, ok := node.Properties["data_category"]; ok {
		return retention.DataCategory(fmt.Sprintf("%v", cat))
	}

	return retention.CategoryUser
}
