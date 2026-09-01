// Package cypher provides composite database command execution.
package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
)

func parseCypherValueToken(tok string) (string, error) {
	tok = strings.TrimSpace(tok)
	if tok == "" {
		return "", nil
	}
	if len(tok) >= 2 {
		if (tok[0] == '\'' && tok[len(tok)-1] == '\'') || (tok[0] == '"' && tok[len(tok)-1] == '"') {
			return tok[1 : len(tok)-1], nil
		}
	}
	if strings.HasPrefix(tok, "`") {
		return unquoteBacktickIdentifier(tok)
	}
	return tok, nil
}

func parseConstituentFromTokens(tokens []string, idx *int) (map[string]interface{}, error) {
	if *idx >= len(tokens) || !strings.EqualFold(tokens[*idx], "ALIAS") {
		return nil, localizedError(localization.CypherCompositeConstituentAliasExpected(), nil)
	}
	*idx = *idx + 1

	if *idx >= len(tokens) {
		return nil, localizedError(localization.CypherCompositeConstituentAliasNameEmpty(), nil)
	}
	aliasName, err := parseCypherValueToken(tokens[*idx])
	if err != nil {
		return nil, localizedError(localization.CypherCompositeConstituentAliasNameInvalid(err), err)
	}
	*idx = *idx + 1
	if strings.TrimSpace(aliasName) == "" {
		return nil, localizedError(localization.CypherCompositeConstituentAliasNameEmpty(), nil)
	}

	if *idx+1 >= len(tokens) || !strings.EqualFold(tokens[*idx], "FOR") || !strings.EqualFold(tokens[*idx+1], "DATABASE") {
		return nil, localizedError(localization.CypherCompositeConstituentForDatabaseExpected(), nil)
	}
	*idx += 2

	if *idx >= len(tokens) {
		return nil, localizedError(localization.CypherCompositeConstituentDatabaseNameEmpty(), nil)
	}
	constituentDbName, err := parseCypherValueToken(tokens[*idx])
	if err != nil {
		return nil, localizedError(localization.CypherCompositeConstituentDatabaseNameInvalid(err), err)
	}
	*idx = *idx + 1
	if strings.TrimSpace(constituentDbName) == "" {
		return nil, localizedError(localization.CypherCompositeConstituentDatabaseNameEmpty(), nil)
	}

	ref := map[string]interface{}{
		"alias":         aliasName,
		"database_name": constituentDbName,
		"type":          "local",
		"access_mode":   "read_write",
	}
	hasUserPassword := false
	hasOIDCForwarding := false

	for *idx < len(tokens) {
		if strings.EqualFold(tokens[*idx], "ALIAS") {
			break
		}

		switch {
		case strings.EqualFold(tokens[*idx], "AT"):
			*idx = *idx + 1
			if *idx >= len(tokens) {
				return nil, localizedError(localization.CypherCompositeConstituentRemoteURIExpected(), nil)
			}
			uri, err := parseCypherValueToken(tokens[*idx])
			if err != nil {
				return nil, localizedError(localization.CypherCompositeConstituentRemoteURIInvalid(err), err)
			}
			*idx = *idx + 1
			if strings.TrimSpace(uri) == "" {
				return nil, localizedError(localization.CypherCompositeConstituentRemoteURIEmpty(), nil)
			}
			ref["uri"] = uri
			ref["type"] = "remote"

		case strings.EqualFold(tokens[*idx], "USER"):
			*idx = *idx + 1
			if *idx >= len(tokens) {
				return nil, localizedError(localization.CypherCompositeConstituentUserEmpty(), nil)
			}
			user, err := parseCypherValueToken(tokens[*idx])
			if err != nil {
				return nil, localizedError(localization.CypherCompositeConstituentUserInvalid(err), err)
			}
			*idx = *idx + 1
			if strings.TrimSpace(user) == "" {
				return nil, localizedError(localization.CypherCompositeConstituentUserEmpty(), nil)
			}
			ref["user"] = user
			hasUserPassword = true

		case strings.EqualFold(tokens[*idx], "PASSWORD"):
			*idx = *idx + 1
			if *idx >= len(tokens) {
				return nil, localizedError(localization.CypherCompositeConstituentPasswordEmpty(), nil)
			}
			password, err := parseCypherValueToken(tokens[*idx])
			if err != nil {
				return nil, localizedError(localization.CypherCompositeConstituentPasswordInvalid(err), err)
			}
			*idx = *idx + 1
			if strings.TrimSpace(password) == "" {
				return nil, localizedError(localization.CypherCompositeConstituentPasswordEmpty(), nil)
			}
			ref["password"] = password
			hasUserPassword = true

		case strings.EqualFold(tokens[*idx], "OIDC"):
			*idx = *idx + 1
			if *idx+1 >= len(tokens) || !strings.EqualFold(tokens[*idx], "CREDENTIAL") || !strings.EqualFold(tokens[*idx+1], "FORWARDING") {
				return nil, localizedError(localization.CypherCompositeConstituentOIDCCredentialExpected(), nil)
			}
			*idx += 2
			hasOIDCForwarding = true

		case strings.EqualFold(tokens[*idx], "SECRET"):
			*idx = *idx + 1
			if *idx >= len(tokens) || !strings.EqualFold(tokens[*idx], "REF") {
				return nil, localizedError(localization.CypherCompositeConstituentSecretRefExpected(), nil)
			}
			*idx = *idx + 1
			if *idx >= len(tokens) {
				return nil, localizedError(localization.CypherCompositeConstituentSecretRefEmpty(), nil)
			}
			secretRef, err := parseCypherValueToken(tokens[*idx])
			if err != nil {
				return nil, localizedError(localization.CypherCompositeConstituentSecretRefInvalid(err), err)
			}
			*idx = *idx + 1
			if strings.TrimSpace(secretRef) == "" {
				return nil, localizedError(localization.CypherCompositeConstituentSecretRefEmpty(), nil)
			}
			ref["secret_ref"] = secretRef

		case strings.EqualFold(tokens[*idx], "TYPE"):
			*idx = *idx + 1
			if *idx >= len(tokens) {
				return nil, localizedError(localization.CypherCompositeConstituentTypeEmpty(), nil)
			}
			typeVal, err := parseCypherValueToken(tokens[*idx])
			if err != nil {
				return nil, localizedError(localization.CypherCompositeConstituentTypeInvalid(err), err)
			}
			*idx = *idx + 1
			typeVal = strings.ToLower(strings.TrimSpace(typeVal))
			if typeVal != "local" && typeVal != "remote" {
				return nil, localizedError(localization.CypherCompositeConstituentTypeUnsupported(), nil)
			}
			// Reject contradictory AT + TYPE local: AT implies remote.
			if existingType, ok := ref["type"]; ok && existingType == "remote" && typeVal == "local" {
				return nil, localizedError(localization.CypherCompositeConstituentTypeContradictsAT(), nil)
			}
			ref["type"] = typeVal

		case strings.EqualFold(tokens[*idx], "ACCESS"):
			*idx = *idx + 1
			if *idx >= len(tokens) {
				return nil, localizedError(localization.CypherCompositeConstituentAccessModeEmpty(), nil)
			}
			accessVal, err := parseCypherValueToken(tokens[*idx])
			if err != nil {
				return nil, localizedError(localization.CypherCompositeConstituentAccessModeInvalid(err), err)
			}
			*idx = *idx + 1
			accessVal = strings.ToLower(strings.TrimSpace(accessVal))
			switch accessVal {
			case "read", "write", "read_write":
				ref["access_mode"] = accessVal
			default:
				return nil, localizedError(localization.CypherCompositeConstituentAccessModeUnsupported(), nil)
			}

		default:
			return nil, localizedError(localization.CypherCompositeConstituentUnexpectedToken(tokens[*idx]), nil)
		}
	}

	if t, _ := ref["type"].(string); t == "remote" {
		switch {
		case hasOIDCForwarding && hasUserPassword:
			return nil, localizedError(localization.CypherCompositeConstituentAuthModesConflict(), nil)
		case hasUserPassword:
			user, _ := ref["user"].(string)
			password, _ := ref["password"].(string)
			user = strings.TrimSpace(user)
			password = strings.TrimSpace(password)
			if user == "" || password == "" {
				return nil, localizedError(localization.CypherCompositeConstituentUserPasswordRequired(), nil)
			}
			ref["auth_mode"] = "user_password"
		default:
			ref["auth_mode"] = "oidc_forwarding"
		}
	} else if hasUserPassword || hasOIDCForwarding {
		return nil, localizedError(localization.CypherCompositeConstituentRemoteRequired(), nil)
	}

	return ref, nil
}

// executeCreateCompositeDatabase handles CREATE COMPOSITE DATABASE command.
//
// Syntax:
//
//	CREATE COMPOSITE DATABASE name [IF NOT EXISTS]
//	  ALIAS alias1 FOR DATABASE db1
//	  ALIAS alias2 FOR DATABASE db2
//	  ...
//
// Example:
//
//	CREATE COMPOSITE DATABASE analytics
//	  ALIAS tenant_a FOR DATABASE tenant_a
//	  ALIAS tenant_b FOR DATABASE tenant_b
//
//	CREATE COMPOSITE DATABASE analytics IF NOT EXISTS
//	  ALIAS tenant_a FOR DATABASE tenant_a
func (e *StorageExecutor) executeCreateCompositeDatabase(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, localizedError(localization.CypherCompositeDatabaseManagerUnavailable("CREATE COMPOSITE DATABASE"), nil)
	}

	// Find "CREATE COMPOSITE DATABASE" keyword position
	createIdx := findMultiWordKeywordIndex(cypher, "CREATE", "COMPOSITE DATABASE")
	if createIdx == -1 {
		return nil, localizedError(localization.CypherCompositeCreateInvalidSyntax(), nil)
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
		return nil, localizedError(localization.CypherCompositeCreateDatabaseNameExpected(), nil)
	}

	// Extract composite database name (until newline or end)
	dbNameEnd := startPos
	for dbNameEnd < len(cypher) && !isWhitespace(cypher[dbNameEnd]) && cypher[dbNameEnd] != '\n' {
		dbNameEnd++
	}

	dbName := strings.TrimSpace(cypher[startPos:dbNameEnd])
	if dbName == "" {
		return nil, localizedError(localization.CypherCompositeCreateDatabaseNameEmpty(), nil)
	}

	// Check for IF NOT EXISTS after database name.
	ifNotExists := false
	remaining := strings.TrimSpace(cypher[dbNameEnd:])
	upperRemaining := strings.ToUpper(remaining)
	if strings.HasPrefix(upperRemaining, "IF NOT EXISTS") {
		ifNotExists = true
		remaining = strings.TrimSpace(remaining[len("IF NOT EXISTS"):])
	}

	// If IF NOT EXISTS and database already exists, return success silently.
	if ifNotExists && e.dbManager != nil && e.dbManager.IsCompositeDatabase(dbName) {
		return &ExecuteResult{
			Columns: []string{"name"},
			Rows:    [][]interface{}{{dbName}},
		}, nil
	}
	// Also handle IF NOT EXISTS when a standard database with that name exists.
	if ifNotExists && e.dbManager != nil && e.dbManager.Exists(dbName) {
		return &ExecuteResult{
			Columns: []string{"name"},
			Rows:    [][]interface{}{{dbName}},
		}, nil
	}

	// Parse constituents (ALIAS ... FOR DATABASE ... [AT ...] [SECRET REF ...])
	constituents := []interface{}{}
	if remaining != "" {
		tokens, err := tokenize(remaining)
		if err != nil {
			return nil, localizedError(localization.CypherCompositeCreateTokenizeFailed(err), err)
		}
		idx := 0
		for idx < len(tokens) {
			ref, err := parseConstituentFromTokens(tokens, &idx)
			if err != nil {
				return nil, err
			}
			constituents = append(constituents, ref)
		}
	}

	if len(constituents) == 0 {
		return nil, localizedError(localization.CypherCompositeCreateConstituentRequired(), nil)
	}

	// Create composite database
	err := e.dbManager.CreateCompositeDatabase(dbName, constituents)
	if err != nil {
		return nil, localizedError(localization.CypherCompositeCreateDatabaseFailed(dbName, err), err)
	}

	return &ExecuteResult{
		Columns: []string{"name"},
		Rows:    [][]interface{}{{dbName}},
	}, nil
}

// executeDropCompositeDatabase handles DROP COMPOSITE DATABASE command.
func (e *StorageExecutor) executeDropCompositeDatabase(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, localizedError(localization.CypherCompositeDatabaseManagerUnavailable("DROP COMPOSITE DATABASE"), nil)
	}

	// Find "DROP COMPOSITE DATABASE" keyword position
	dropIdx := findMultiWordKeywordIndex(cypher, "DROP", "COMPOSITE DATABASE")
	if dropIdx == -1 {
		return nil, localizedError(localization.CypherCompositeDropInvalidSyntax(), nil)
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
		return nil, localizedError(localization.CypherCompositeDropDatabaseNameExpected(), nil)
	}

	// Extract database name
	dbName := strings.TrimSpace(cypher[startPos:])
	dbName = strings.ReplaceAll(dbName, " ", "")
	dbName = strings.ReplaceAll(dbName, "\t", "")
	dbName = strings.ReplaceAll(dbName, "\n", "")
	dbName = strings.ReplaceAll(dbName, "\r", "")

	if dbName == "" {
		return nil, localizedError(localization.CypherCompositeDropDatabaseNameEmpty(), nil)
	}

	// Drop composite database
	err := e.dbManager.DropCompositeDatabase(dbName)
	if err != nil {
		return nil, localizedError(localization.CypherCompositeDropDatabaseFailed(dbName, err), err)
	}

	return &ExecuteResult{
		Columns: []string{"name"},
		Rows:    [][]interface{}{{dbName}},
	}, nil
}

// executeShowCompositeDatabases handles SHOW COMPOSITE DATABASES command.
func (e *StorageExecutor) executeShowCompositeDatabases(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, localizedError(localization.CypherCompositeDatabaseManagerUnavailable("SHOW COMPOSITE DATABASES"), nil)
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
		return nil, localizedError(localization.CypherCompositeDatabaseManagerUnavailable("SHOW CONSTITUENTS"), nil)
	}

	// Find "SHOW CONSTITUENTS" keyword position
	showIdx := findMultiWordKeywordIndex(cypher, "SHOW", "CONSTITUENTS")
	if showIdx == -1 {
		return nil, localizedError(localization.CypherCompositeShowConstituentsInvalidSyntax(), nil)
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
		return nil, localizedError(localization.CypherCompositeShowConstituentsNameExpected(), nil)
	}

	// Get constituents
	constituents, err := e.dbManager.GetCompositeConstituents(compositeName)
	if err != nil {
		return nil, localizedError(localization.CypherCompositeGetConstituentsFailed(err), err)
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
				ref.URI,
				ref.SecretRef,
				ref.AuthMode,
				ref.User,
			}
		} else if m, ok := c.(map[string]interface{}); ok {
			// Fallback for map format (if returned as map)
			rows[i] = []interface{}{
				m["alias"],
				m["database_name"],
				m["type"],
				m["access_mode"],
				m["uri"],
				m["secret_ref"],
				m["auth_mode"],
				m["user"],
			}
		} else {
			// Unknown type - return empty row
			rows[i] = []interface{}{"", "", "", "", "", "", "", ""}
		}
	}

	return &ExecuteResult{
		Columns: []string{"alias", "database", "type", "access_mode", "uri", "secret_ref", "auth_mode", "user"},
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
		return nil, localizedError(localization.CypherCompositeDatabaseManagerUnavailable("ALTER COMPOSITE DATABASE"), nil)
	}

	// Find "ALTER COMPOSITE DATABASE" keyword position
	// First find "ALTER COMPOSITE", then check for "DATABASE"
	alterIdx := findMultiWordKeywordIndex(cypher, "ALTER", "COMPOSITE")
	if alterIdx == -1 {
		return nil, localizedError(localization.CypherCompositeAlterInvalidSyntax(), nil)
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
			return nil, localizedError(localization.CypherCompositeAlterDatabaseKeywordExpected(), nil)
		}
	} else {
		return nil, localizedError(localization.CypherCompositeAlterInvalidSyntax(), nil)
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
		return nil, localizedError(localization.CypherCompositeAlterDatabaseNameExpected(), nil)
	}

	// Extract composite database name (until newline or whitespace)
	dbNameEnd := startPos
	for dbNameEnd < len(cypher) && !isWhitespace(cypher[dbNameEnd]) && cypher[dbNameEnd] != '\n' {
		dbNameEnd++
	}

	dbName := strings.TrimSpace(cypher[startPos:dbNameEnd])
	if dbName == "" {
		return nil, localizedError(localization.CypherCompositeAlterDatabaseNameEmpty(), nil)
	}

	// Check for ADD or DROP
	remaining := strings.TrimSpace(cypher[dbNameEnd:])
	upperRemaining := strings.ToUpper(remaining)

	if strings.HasPrefix(upperRemaining, "ADD ALIAS") {
		tokens, err := tokenize(remaining)
		if err != nil {
			return nil, localizedError(localization.CypherCompositeAlterTokenizeFailed(err), err)
		}
		if len(tokens) < 2 || !strings.EqualFold(tokens[0], "ADD") || !strings.EqualFold(tokens[1], "ALIAS") {
			return nil, localizedError(localization.CypherCompositeAlterAddAliasExpected(), nil)
		}
		idx := 1
		constituent, err := parseConstituentFromTokens(tokens, &idx)
		if err != nil {
			return nil, err
		}
		if idx != len(tokens) {
			return nil, localizedError(localization.CypherCompositeAddAliasUnexpectedToken(tokens[idx]), nil)
		}
		aliasName, _ := constituent["alias"].(string)
		constituentDbName, _ := constituent["database_name"].(string)

		// Add constituent using the interface
		err = e.dbManager.AddConstituent(dbName, constituent)
		if err != nil {
			return nil, localizedError(localization.CypherCompositeAddConstituentFailed(dbName, err), err)
		}

		return &ExecuteResult{
			Columns: []string{"composite_database", "action", "alias", "database", "type", "uri", "secret_ref", "auth_mode", "user"},
			Rows: [][]interface{}{{
				dbName,
				"ADD",
				aliasName,
				constituentDbName,
				constituent["type"],
				constituent["uri"],
				constituent["secret_ref"],
				constituent["auth_mode"],
				constituent["user"],
			}},
		}, nil

	} else if strings.HasPrefix(upperRemaining, "DROP ALIAS") {
		// DROP ALIAS alias
		dropIdx := findMultiWordKeywordIndex(remaining, "DROP", "ALIAS")
		if dropIdx == -1 {
			return nil, localizedError(localization.CypherCompositeAlterDropAliasExpected(), nil)
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
			return nil, localizedError(localization.CypherCompositeDropAliasNameEmpty(), nil)
		}

		// Remove constituent
		err := e.dbManager.RemoveConstituent(dbName, aliasName)
		if err != nil {
			return nil, localizedError(localization.CypherCompositeRemoveConstituentFailed(dbName, err), err)
		}

		return &ExecuteResult{
			Columns: []string{"composite_database", "action", "alias"},
			Rows:    [][]interface{}{{dbName, "DROP", aliasName}},
		}, nil

	} else {
		return nil, localizedError(localization.CypherCompositeAlterActionExpected(), nil)
	}
}
