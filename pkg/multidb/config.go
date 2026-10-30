// Package multidb provides configuration helpers for multi-database support.
package multidb

// NewConfigFromDefaultDatabase creates a DatabaseManager Config from a default database name.
// This allows the DatabaseManager to use the same default database name as configured
// in the main NornicDB configuration.
//
// Example:
//
//	// Use default database name from main config
//	mainConfig := config.LoadDefaults()
//	dbConfig := multidb.NewConfigFromDefaultDatabase(mainConfig.Database.DefaultDatabase)
//	// ctx belongs to the application startup operation.
//	manager, err := multidb.NewDatabaseManager(ctx, inner, dbConfig)
func NewConfigFromDefaultDatabase(defaultDatabase string) *Config {
	if defaultDatabase == "" {
		// Fall back to "nornic" if empty
		defaultDatabase = "nornic"
	}
	return &Config{
		DefaultDatabase:  defaultDatabase,
		SystemDatabase:   "system",
		MaxDatabases:     0, // Unlimited
		AllowDropDefault: false,
	}
}
