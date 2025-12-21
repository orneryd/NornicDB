// Package cypher provides composite database command execution.
package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

// executeCreateCompositeDatabase handles CREATE COMPOSITE DATABASE command.
//
// Syntax:
//
//	CREATE COMPOSITE DATABASE name
//	  ALIAS alias1 FOR DATABASE db1
//	  ALIAS alias2 FOR DATABASE db2
//	  ...
//
// Example:
//
//	CREATE COMPOSITE DATABASE analytics
//	  ALIAS tenant_a FOR DATABASE tenant_a
//	  ALIAS tenant_b FOR DATABASE tenant_b
func (e *StorageExecutor) executeCreateCompositeDatabase(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database manager not available - CREATE COMPOSITE DATABASE requires multi-database support")
	}

	// Find "CREATE COMPOSITE DATABASE" keyword position
	createIdx := findMultiWordKeywordIndex(cypher, "CREATE", "COMPOSITE DATABASE")
	if createIdx == -1 {
		return nil, fmt.Errorf("invalid CREATE COMPOSITE DATABASE syntax")
	}

	// Skip "CREATE COMPOSITE DATABASE" and whitespace
	startPos := createIdx + len("CREATE")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "COMPOSITE DATABASE"
	if startPos+len("COMPOSITE DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE DATABASE")], "COMPOSITE DATABASE") {
		startPos += len("COMPOSITE DATABASE")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	} else {
		// Try with flexible whitespace
		if startPos+len("COMPOSITE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE")], "COMPOSITE") {
			startPos += len("COMPOSITE")
			for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
				startPos++
			}
			if startPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("DATABASE")], "DATABASE") {
				startPos += len("DATABASE")
				for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
					startPos++
				}
			}
		}
	}

	if startPos >= len(cypher) {
		return nil, fmt.Errorf("invalid CREATE COMPOSITE DATABASE syntax: database name expected")
	}

	// Extract composite database name (until newline or end)
	dbNameEnd := startPos
	for dbNameEnd < len(cypher) && !isWhitespace(cypher[dbNameEnd]) && cypher[dbNameEnd] != '\n' {
		dbNameEnd++
	}

	dbName := strings.TrimSpace(cypher[startPos:dbNameEnd])
	if dbName == "" {
		return nil, fmt.Errorf("invalid CREATE COMPOSITE DATABASE syntax: database name cannot be empty")
	}

	// Parse constituents (ALIAS ... FOR DATABASE ...)
	constituents := []interface{}{}
	remaining := strings.TrimSpace(cypher[dbNameEnd:])

	for remaining != "" {
		// Look for "ALIAS" keyword
		aliasIdx := findKeywordIndex(remaining, "ALIAS")
		if aliasIdx == -1 {
			break
		}

		// Skip "ALIAS" and whitespace
		aliasStart := aliasIdx + len("ALIAS")
		for aliasStart < len(remaining) && isWhitespace(remaining[aliasStart]) {
			aliasStart++
		}

		// Find "FOR DATABASE"
		forIdx := findMultiWordKeywordIndex(remaining[aliasStart:], "FOR", "DATABASE")
		if forIdx == -1 {
			return nil, fmt.Errorf("invalid constituent syntax: FOR DATABASE expected")
		}

		// Extract alias name
		aliasName := strings.TrimSpace(remaining[aliasStart : aliasStart+forIdx])
		aliasName = strings.ReplaceAll(aliasName, " ", "")
		aliasName = strings.ReplaceAll(aliasName, "\t", "")
		aliasName = strings.ReplaceAll(aliasName, "\n", "")
		aliasName = strings.ReplaceAll(aliasName, "\r", "")

		if aliasName == "" {
			return nil, fmt.Errorf("invalid constituent syntax: alias name cannot be empty")
		}

		// Skip "FOR DATABASE" and whitespace
		dbStart := aliasStart + forIdx + len("FOR")
		for dbStart < len(remaining) && isWhitespace(remaining[dbStart]) {
			dbStart++
		}
		if dbStart+len("DATABASE") <= len(remaining) && strings.EqualFold(remaining[dbStart:dbStart+len("DATABASE")], "DATABASE") {
			dbStart += len("DATABASE")
			for dbStart < len(remaining) && isWhitespace(remaining[dbStart]) {
				dbStart++
			}
		}

		// Extract database name (until newline or end)
		dbNameEnd2 := dbStart
		for dbNameEnd2 < len(remaining) && !isWhitespace(remaining[dbNameEnd2]) && remaining[dbNameEnd2] != '\n' {
			dbNameEnd2++
		}

		constituentDbName := strings.TrimSpace(remaining[dbStart:dbNameEnd2])
		constituentDbName = strings.ReplaceAll(constituentDbName, " ", "")
		constituentDbName = strings.ReplaceAll(constituentDbName, "\t", "")
		constituentDbName = strings.ReplaceAll(constituentDbName, "\n", "")
		constituentDbName = strings.ReplaceAll(constituentDbName, "\r", "")

		if constituentDbName == "" {
			return nil, fmt.Errorf("invalid constituent syntax: database name cannot be empty")
		}

		// Add constituent (default to local, read_write)
		constituents = append(constituents, map[string]interface{}{
			"alias":         aliasName,
			"database_name": constituentDbName,
			"type":          "local",
			"access_mode":   "read_write",
		})

		// Move to next constituent
		remaining = strings.TrimSpace(remaining[dbNameEnd2:])
	}

	if len(constituents) == 0 {
		return nil, fmt.Errorf("invalid CREATE COMPOSITE DATABASE syntax: at least one constituent required")
	}

	// Create composite database
	err := e.dbManager.CreateCompositeDatabase(dbName, constituents)
	if err != nil {
		return nil, fmt.Errorf("failed to create composite database '%s': %w", dbName, err)
	}

	return &ExecuteResult{
		Columns: []string{"name"},
		Rows:    [][]interface{}{{dbName}},
	}, nil
}

// executeDropCompositeDatabase handles DROP COMPOSITE DATABASE command.
func (e *StorageExecutor) executeDropCompositeDatabase(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database manager not available - DROP COMPOSITE DATABASE requires multi-database support")
	}

	// Find "DROP COMPOSITE DATABASE" keyword position
	dropIdx := findMultiWordKeywordIndex(cypher, "DROP", "COMPOSITE DATABASE")
	if dropIdx == -1 {
		return nil, fmt.Errorf("invalid DROP COMPOSITE DATABASE syntax")
	}

	// Skip "DROP COMPOSITE DATABASE" and whitespace
	startPos := dropIdx + len("DROP")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "COMPOSITE DATABASE"
	if startPos+len("COMPOSITE DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE DATABASE")], "COMPOSITE DATABASE") {
		startPos += len("COMPOSITE DATABASE")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	} else {
		// Try with flexible whitespace
		if startPos+len("COMPOSITE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE")], "COMPOSITE") {
			startPos += len("COMPOSITE")
			for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
				startPos++
			}
			if startPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("DATABASE")], "DATABASE") {
				startPos += len("DATABASE")
				for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
					startPos++
				}
			}
		}
	}

	if startPos >= len(cypher) {
		return nil, fmt.Errorf("invalid DROP COMPOSITE DATABASE syntax: database name expected")
	}

	// Extract database name
	dbName := strings.TrimSpace(cypher[startPos:])
	dbName = strings.ReplaceAll(dbName, " ", "")
	dbName = strings.ReplaceAll(dbName, "\t", "")
	dbName = strings.ReplaceAll(dbName, "\n", "")
	dbName = strings.ReplaceAll(dbName, "\r", "")

	if dbName == "" {
		return nil, fmt.Errorf("invalid DROP COMPOSITE DATABASE syntax: database name cannot be empty")
	}

	// Drop composite database
	err := e.dbManager.DropCompositeDatabase(dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to drop composite database '%s': %w", dbName, err)
	}

	return &ExecuteResult{
		Columns: []string{"name"},
		Rows:    [][]interface{}{{dbName}},
	}, nil
}

// executeShowCompositeDatabases handles SHOW COMPOSITE DATABASES command.
func (e *StorageExecutor) executeShowCompositeDatabases(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database manager not available - SHOW COMPOSITE DATABASES requires multi-database support")
	}

	compositeDbs := e.dbManager.ListCompositeDatabases()

	rows := make([][]interface{}, len(compositeDbs))
	for i, db := range compositeDbs {
		rows[i] = []interface{}{db.Name(), db.Type(), db.Status()}
	}

	return &ExecuteResult{
		Columns: []string{"name", "type", "status"},
		Rows:    rows,
	}, nil
}

// executeShowConstituents handles SHOW CONSTITUENTS FOR COMPOSITE DATABASE command.
func (e *StorageExecutor) executeShowConstituents(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database manager not available - SHOW CONSTITUENTS requires multi-database support")
	}

	// Find "SHOW CONSTITUENTS" keyword position
	showIdx := findMultiWordKeywordIndex(cypher, "SHOW", "CONSTITUENTS")
	if showIdx == -1 {
		return nil, fmt.Errorf("invalid SHOW CONSTITUENTS syntax")
	}

	// Check for "FOR COMPOSITE DATABASE"
	forIdx := findMultiWordKeywordIndex(cypher, "FOR", "COMPOSITE DATABASE")
	var compositeName string

	if forIdx >= 0 {
		// Extract composite database name
		startPos := forIdx + len("FOR")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
		// Skip "COMPOSITE DATABASE"
		if startPos+len("COMPOSITE DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE DATABASE")], "COMPOSITE DATABASE") {
			startPos += len("COMPOSITE DATABASE")
			for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
				startPos++
			}
		} else {
			// Try with flexible whitespace
			if startPos+len("COMPOSITE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE")], "COMPOSITE") {
				startPos += len("COMPOSITE")
				for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
					startPos++
				}
				if startPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("DATABASE")], "DATABASE") {
					startPos += len("DATABASE")
					for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
						startPos++
					}
				}
			}
		}

		compositeName = strings.TrimSpace(cypher[startPos:])
		compositeName = strings.ReplaceAll(compositeName, " ", "")
		compositeName = strings.ReplaceAll(compositeName, "\t", "")
		compositeName = strings.ReplaceAll(compositeName, "\n", "")
		compositeName = strings.ReplaceAll(compositeName, "\r", "")
	}

	if compositeName == "" {
		return nil, fmt.Errorf("invalid SHOW CONSTITUENTS syntax: FOR COMPOSITE DATABASE name expected")
	}

	// Get constituents
	constituents, err := e.dbManager.GetCompositeConstituents(compositeName)
	if err != nil {
		return nil, fmt.Errorf("failed to get constituents: %w", err)
	}

	rows := make([][]interface{}, len(constituents))
	for i, c := range constituents {
		// Handle ConstituentRef type
		if ref, ok := c.(multidb.ConstituentRef); ok {
			rows[i] = []interface{}{
				ref.Alias,
				ref.DatabaseName,
				ref.Type,
				ref.AccessMode,
			}
		} else if m, ok := c.(map[string]interface{}); ok {
			// Fallback for map format (if returned as map)
			rows[i] = []interface{}{
				m["alias"],
				m["database_name"],
				m["type"],
				m["access_mode"],
			}
		} else {
			// Unknown type - return empty row
			rows[i] = []interface{}{"", "", "", ""}
		}
	}

	return &ExecuteResult{
		Columns: []string{"alias", "database", "type", "access_mode"},
		Rows:    rows,
	}, nil
}

// executeAlterCompositeDatabase handles ALTER COMPOSITE DATABASE command.
//
// Syntax:
//
//	ALTER COMPOSITE DATABASE name
//	  ADD ALIAS alias FOR DATABASE db
//	ALTER COMPOSITE DATABASE name
//	  DROP ALIAS alias
//
// Example:
//
//	ALTER COMPOSITE DATABASE analytics
//	  ADD ALIAS tenant_d FOR DATABASE tenant_d
//
//	ALTER COMPOSITE DATABASE analytics
//	  DROP ALIAS tenant_c
func (e *StorageExecutor) executeAlterCompositeDatabase(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database manager not available - ALTER COMPOSITE DATABASE requires multi-database support")
	}

	// Find "ALTER COMPOSITE DATABASE" keyword position
	// First find "ALTER COMPOSITE", then check for "DATABASE"
	alterIdx := findMultiWordKeywordIndex(cypher, "ALTER", "COMPOSITE")
	if alterIdx == -1 {
		return nil, fmt.Errorf("invalid ALTER COMPOSITE DATABASE syntax")
	}

	// Check that "DATABASE" follows "COMPOSITE"
	afterComposite := alterIdx + len("ALTER")
	for afterComposite < len(cypher) && isWhitespace(cypher[afterComposite]) {
		afterComposite++
	}
	if afterComposite+len("COMPOSITE") <= len(cypher) && strings.EqualFold(cypher[afterComposite:afterComposite+len("COMPOSITE")], "COMPOSITE") {
		afterComposite += len("COMPOSITE")
		for afterComposite < len(cypher) && isWhitespace(cypher[afterComposite]) {
			afterComposite++
		}
		if afterComposite+len("DATABASE") > len(cypher) || !strings.EqualFold(cypher[afterComposite:afterComposite+len("DATABASE")], "DATABASE") {
			return nil, fmt.Errorf("invalid ALTER COMPOSITE DATABASE syntax: DATABASE expected after COMPOSITE")
		}
	} else {
		return nil, fmt.Errorf("invalid ALTER COMPOSITE DATABASE syntax")
	}

	// Skip "ALTER COMPOSITE DATABASE" and whitespace
	startPos := alterIdx + len("ALTER")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "COMPOSITE DATABASE"
	if startPos+len("COMPOSITE DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE DATABASE")], "COMPOSITE DATABASE") {
		startPos += len("COMPOSITE DATABASE")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	} else {
		// Try with flexible whitespace
		if startPos+len("COMPOSITE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE")], "COMPOSITE") {
			startPos += len("COMPOSITE")
			for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
				startPos++
			}
			if startPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("DATABASE")], "DATABASE") {
				startPos += len("DATABASE")
				for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
					startPos++
				}
			}
		}
	}

	if startPos >= len(cypher) {
		return nil, fmt.Errorf("invalid ALTER COMPOSITE DATABASE syntax: database name expected")
	}

	// Extract composite database name (until newline or whitespace)
	dbNameEnd := startPos
	for dbNameEnd < len(cypher) && !isWhitespace(cypher[dbNameEnd]) && cypher[dbNameEnd] != '\n' {
		dbNameEnd++
	}

	dbName := strings.TrimSpace(cypher[startPos:dbNameEnd])
	if dbName == "" {
		return nil, fmt.Errorf("invalid ALTER COMPOSITE DATABASE syntax: database name cannot be empty")
	}

	// Check for ADD or DROP
	remaining := strings.TrimSpace(cypher[dbNameEnd:])
	upperRemaining := strings.ToUpper(remaining)

	if strings.HasPrefix(upperRemaining, "ADD ALIAS") {
		// ADD ALIAS alias FOR DATABASE db
		addIdx := findMultiWordKeywordIndex(remaining, "ADD", "ALIAS")
		if addIdx == -1 {
			return nil, fmt.Errorf("invalid ALTER COMPOSITE DATABASE syntax: ADD ALIAS expected")
		}

		// Skip "ADD ALIAS" and whitespace
		aliasStart := addIdx + len("ADD")
		for aliasStart < len(remaining) && isWhitespace(remaining[aliasStart]) {
			aliasStart++
		}
		if aliasStart+len("ALIAS") <= len(remaining) && strings.EqualFold(remaining[aliasStart:aliasStart+len("ALIAS")], "ALIAS") {
			aliasStart += len("ALIAS")
			for aliasStart < len(remaining) && isWhitespace(remaining[aliasStart]) {
				aliasStart++
			}
		}

		// Find "FOR DATABASE"
		forIdx := findMultiWordKeywordIndex(remaining[aliasStart:], "FOR", "DATABASE")
		if forIdx == -1 {
			return nil, fmt.Errorf("invalid ADD ALIAS syntax: FOR DATABASE expected")
		}

		// Extract alias name
		aliasName := strings.TrimSpace(remaining[aliasStart : aliasStart+forIdx])
		aliasName = strings.ReplaceAll(aliasName, " ", "")
		aliasName = strings.ReplaceAll(aliasName, "\t", "")
		aliasName = strings.ReplaceAll(aliasName, "\n", "")
		aliasName = strings.ReplaceAll(aliasName, "\r", "")

		if aliasName == "" {
			return nil, fmt.Errorf("invalid ADD ALIAS syntax: alias name cannot be empty")
		}

		// Skip "FOR DATABASE" and whitespace
		dbStart := aliasStart + forIdx + len("FOR")
		for dbStart < len(remaining) && isWhitespace(remaining[dbStart]) {
			dbStart++
		}
		if dbStart+len("DATABASE") <= len(remaining) && strings.EqualFold(remaining[dbStart:dbStart+len("DATABASE")], "DATABASE") {
			dbStart += len("DATABASE")
			for dbStart < len(remaining) && isWhitespace(remaining[dbStart]) {
				dbStart++
			}
		}

		// Extract database name
		constituentDbName := strings.TrimSpace(remaining[dbStart:])
		constituentDbName = strings.ReplaceAll(constituentDbName, " ", "")
		constituentDbName = strings.ReplaceAll(constituentDbName, "\t", "")
		constituentDbName = strings.ReplaceAll(constituentDbName, "\n", "")
		constituentDbName = strings.ReplaceAll(constituentDbName, "\r", "")

		if constituentDbName == "" {
			return nil, fmt.Errorf("invalid ADD ALIAS syntax: database name cannot be empty")
		}

		// Convert to ConstituentRef format (interface{} for DatabaseManagerInterface)
		constituent := map[string]interface{}{
			"alias":         aliasName,
			"database_name": constituentDbName,
			"type":          "local",
			"access_mode":   "read_write",
		}

		// Add constituent using the interface
		err := e.dbManager.AddConstituent(dbName, constituent)
		if err != nil {
			return nil, fmt.Errorf("failed to add constituent to composite database '%s': %w", dbName, err)
		}

		return &ExecuteResult{
			Columns: []string{"composite_database", "action", "alias", "database"},
			Rows:    [][]interface{}{{dbName, "ADD", aliasName, constituentDbName}},
		}, nil

	} else if strings.HasPrefix(upperRemaining, "DROP ALIAS") {
		// DROP ALIAS alias
		dropIdx := findMultiWordKeywordIndex(remaining, "DROP", "ALIAS")
		if dropIdx == -1 {
			return nil, fmt.Errorf("invalid ALTER COMPOSITE DATABASE syntax: DROP ALIAS expected")
		}

		// Skip "DROP ALIAS" and whitespace
		aliasStart := dropIdx + len("DROP")
		for aliasStart < len(remaining) && isWhitespace(remaining[aliasStart]) {
			aliasStart++
		}
		if aliasStart+len("ALIAS") <= len(remaining) && strings.EqualFold(remaining[aliasStart:aliasStart+len("ALIAS")], "ALIAS") {
			aliasStart += len("ALIAS")
			for aliasStart < len(remaining) && isWhitespace(remaining[aliasStart]) {
				aliasStart++
			}
		}

		// Extract alias name (until newline or end)
		aliasNameEnd := aliasStart
		for aliasNameEnd < len(remaining) && !isWhitespace(remaining[aliasNameEnd]) && remaining[aliasNameEnd] != '\n' {
			aliasNameEnd++
		}

		aliasName := strings.TrimSpace(remaining[aliasStart:aliasNameEnd])
		aliasName = strings.ReplaceAll(aliasName, " ", "")
		aliasName = strings.ReplaceAll(aliasName, "\t", "")
		aliasName = strings.ReplaceAll(aliasName, "\n", "")
		aliasName = strings.ReplaceAll(aliasName, "\r", "")

		if aliasName == "" {
			return nil, fmt.Errorf("invalid DROP ALIAS syntax: alias name cannot be empty")
		}

		// Remove constituent
		err := e.dbManager.RemoveConstituent(dbName, aliasName)
		if err != nil {
			return nil, fmt.Errorf("failed to remove constituent from composite database '%s': %w", dbName, err)
		}

		return &ExecuteResult{
			Columns: []string{"composite_database", "action", "alias"},
			Rows:    [][]interface{}{{dbName, "DROP", aliasName}},
		}, nil

	} else {
		return nil, fmt.Errorf("invalid ALTER COMPOSITE DATABASE syntax: ADD ALIAS or DROP ALIAS expected")
	}
}
