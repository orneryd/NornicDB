package storage

import "time"

// initializeCreationTimes mirrors direct engine creation while preserving
// explicit times supplied by imports and recovery.
func initializeCreationTimes(createdAt, updatedAt *time.Time) {
	if createdAt.IsZero() {
		*createdAt = time.Now()
	}
	if updatedAt.IsZero() {
		*updatedAt = *createdAt
	}
}
