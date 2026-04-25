package storage

import "fmt"

// RunOnStartMigrations performs all necessary schema migrations.
// Called once during engine initialization. Idempotent — checks the schema
// version marker before each migration and skips if already applied.
func (b *BadgerEngine) RunOnStartMigrations() error {
	currentVersion := b.readSchemaVersion()

	if currentVersion < 1 {
		if err := b.migrateV0ToV1(); err != nil {
			return fmt.Errorf("migration v0→v1 failed: %w", err)
		}
	}

	return nil
}
