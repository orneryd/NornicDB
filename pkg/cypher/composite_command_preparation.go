package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
	"strings"
)

type compositeCommandAction uint8

const (
	compositeCreate compositeCommandAction = iota
	compositeAdd
	compositeDrop
)

// preparedCompositeCommand owns native command validation and its declared result
// schema. Metadata reads this plan; only execute applies its catalog mutation.
type preparedCompositeCommand struct {
	database     string
	action       compositeCommandAction
	noOp         bool
	constituents []interface{}
	constituent  map[string]interface{}
	alias        string
}

func (command *preparedCompositeCommand) columns() []string {
	switch command.action {
	case compositeCreate:
		return []string{"name"}
	case compositeAdd:
		return []string{"composite_database", "action", "alias", "database", "type", "uri", "secret_ref", "auth_mode", "user"}
	default:
		return []string{"composite_database", "action", "alias"}
	}
}

func (command *preparedCompositeCommand) execute(ctx context.Context, e *StorageExecutor) (*ExecuteResult, error) {
	switch command.action {
	case compositeCreate:
		if !command.noOp {
			if err := e.dbManager.CreateCompositeDatabase(ctx, command.database, command.constituents); err != nil {
				return nil, localizedError(localization.CypherCompositeCreateDatabaseFailed(command.database, err), err)
			}
		}
		result := &ExecuteResult{Columns: command.columns(), Rows: [][]interface{}{{command.database}}}
		if !command.noOp {
			result.Administration = &AdministrationOutcome{CreatedDatabase: command.database}
		}
		return result, nil
	case compositeAdd:
		if err := e.dbManager.AddConstituent(ctx, command.database, command.constituent); err != nil {
			return nil, localizedError(localization.CypherCompositeAddConstituentFailed(command.database, err), err)
		}
		ref := command.constituent
		return &ExecuteResult{Columns: command.columns(), Rows: [][]interface{}{{command.database, "ADD", ref["alias"], ref["database_name"], ref["type"], compositeDisplayURI(ref["uri"]), ref["secret_ref"], ref["auth_mode"], compositeDisplayUser(ref["user"], ref["auth_mode"])}}}, nil
	default:
		if err := e.dbManager.RemoveConstituent(ctx, command.database, command.alias); err != nil {
			return nil, localizedError(localization.CypherCompositeRemoveConstituentFailed(command.database, err), err)
		}
		return &ExecuteResult{Columns: command.columns(), Rows: [][]interface{}{{command.database, "DROP", command.alias}}}, nil
	}
}

func (e *StorageExecutor) prepareCompositeCommand(ctx context.Context, query string) (*preparedCompositeCommand, bool, error) {
	_, body := parseExecutionMode(query)
	if startsWithKeywords(body, "CREATE", "COMPOSITE DATABASE") {
		command, err := e.prepareCreateCompositeDatabase(ctx, body)
		return command, true, err
	}
	if startsWithKeywords(body, "ALTER", "COMPOSITE DATABASE") {
		command, err := e.prepareAlterCompositeDatabase(ctx, body)
		return command, true, err
	}
	return nil, false, nil
}

func (e *StorageExecutor) prepareCreateCompositeDatabase(ctx context.Context, cypher string) (_ *preparedCompositeCommand, retErr error) {
	ctx = withStatementCredentials(ctx, cypher)
	defer redactAdminCommandError(cypher, &retErr, statementCredentials(ctx))
	cypher = cyphertext.SourceFor(ctx, cypher).Text

	// Find "CREATE COMPOSITE DATABASE" keyword position
	createIdx := findMultiWordKeywordIndex(cypher, "CREATE", "COMPOSITE DATABASE")
	if createIdx == -1 {
		return nil, localizedError(localization.CypherCompositeCreateInvalidSyntax(), nil)
	}

	// Skip "CREATE COMPOSITE DATABASE" and whitespace
	startPos := createIdx + len("CREATE")
	for startPos < len(cypher) && cyphertext.IsWhitespaceAt(cypher, startPos) {
		startPos++
	}
	// Skip "COMPOSITE DATABASE"
	if startPos+len("COMPOSITE DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE DATABASE")], "COMPOSITE DATABASE") {
		startPos += len("COMPOSITE DATABASE")
		for startPos < len(cypher) && cyphertext.IsWhitespaceAt(cypher, startPos) {
			startPos++
		}
	} else {
		// Try with flexible whitespace
		if startPos+len("COMPOSITE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE")], "COMPOSITE") {
			startPos += len("COMPOSITE")
			for startPos < len(cypher) && cyphertext.IsWhitespaceAt(cypher, startPos) {
				startPos++
			}
			if startPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("DATABASE")], "DATABASE") {
				startPos += len("DATABASE")
				for startPos < len(cypher) && cyphertext.IsWhitespaceAt(cypher, startPos) {
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
	dbName := ""
	if name, ok := cyphertext.ScanIdentifier(cypher, startPos); ok && name.Quoted {
		dbNameEnd, dbName = name.End, name.Value
	} else {
		for dbNameEnd < len(cypher) && !cyphertext.IsWhitespaceAt(cypher, dbNameEnd) {
			dbNameEnd++
		}
		dbName = cypher[startPos:dbNameEnd]
	}
	if dbName == "" {
		return nil, localizedError(localization.CypherCompositeCreateDatabaseNameEmpty(), nil)
	}

	// Check for IF NOT EXISTS after database name.
	ifNotExists := false
	remainingStart := dbNameEnd + len(cypher[dbNameEnd:]) - len(strings.TrimLeftFunc(cypher[dbNameEnd:], cyphertext.IsWhitespace))
	remaining := strings.TrimSpace(cypher[remainingStart:])
	upperRemaining := upperASCII(remaining)
	if strings.HasPrefix(upperRemaining, "IF NOT EXISTS") {
		ifNotExists = true
		before := remaining
		remaining = strings.TrimSpace(remaining[len("IF NOT EXISTS"):])
		remainingStart += len(before) - len(remaining)
	}

	// Parse constituents (ALIAS ... FOR DATABASE ... [AT ...] [SECRET REF ...])
	constituents := []interface{}{}
	if remaining != "" {
		tokens, spans, err := tokenizeSource(remaining)
		if err != nil {
			return nil, localizedError(localization.CypherCompositeCreateTokenizeFailed(err), err)
		}
		idx := 0
		for idx < len(tokens) {
			ref, err := parseConstituentFromTokens(tokens, &idx, &adminCredentialReader{source: remaining, mapping: cyphertext.SourceFor(ctx, cypher), base: remainingStart, spans: spans, set: statementCredentials(ctx)})
			if err != nil {
				return nil, err
			}
			constituents = append(constituents, ref)
		}
	}

	// Consume the complete syntax before deciding that an existing database
	// makes the command a no-op. Unused target semantics remain deferred.
	// If IF NOT EXISTS and database already exists, return success silently.
	if ifNotExists && e.dbManager != nil && e.dbManager.IsCompositeDatabase(dbName) {
		return &preparedCompositeCommand{database: dbName, action: compositeCreate, noOp: true}, nil
	}
	// Also handle IF NOT EXISTS when a standard database with that name exists.
	if ifNotExists && e.dbManager != nil && e.dbManager.Exists(dbName) {
		return &preparedCompositeCommand{database: dbName, action: compositeCreate, noOp: true}, nil
	}

	if len(constituents) == 0 {
		return nil, localizedError(localization.CypherCompositeCreateConstituentRequired(), nil)
	}

	return &preparedCompositeCommand{database: dbName, action: compositeCreate, constituents: constituents}, nil
}

func (e *StorageExecutor) prepareAlterCompositeDatabase(ctx context.Context, cypher string) (_ *preparedCompositeCommand, retErr error) {
	ctx = withStatementCredentials(ctx, cypher)
	defer redactAdminCommandError(cypher, &retErr, statementCredentials(ctx))
	cypher = cyphertext.SourceFor(ctx, cypher).Text

	// Find "ALTER COMPOSITE DATABASE" keyword position
	// First find "ALTER COMPOSITE", then check for "DATABASE"
	alterIdx := findMultiWordKeywordIndex(cypher, "ALTER", "COMPOSITE")
	if alterIdx == -1 {
		return nil, localizedError(localization.CypherCompositeAlterInvalidSyntax(), nil)
	}

	// Check that "DATABASE" follows "COMPOSITE"
	afterComposite := alterIdx + len("ALTER")
	for afterComposite < len(cypher) && cyphertext.IsWhitespaceAt(cypher, afterComposite) {
		afterComposite++
	}
	if afterComposite+len("COMPOSITE") <= len(cypher) && strings.EqualFold(cypher[afterComposite:afterComposite+len("COMPOSITE")], "COMPOSITE") {
		afterComposite += len("COMPOSITE")
		for afterComposite < len(cypher) && cyphertext.IsWhitespaceAt(cypher, afterComposite) {
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
	for startPos < len(cypher) && cyphertext.IsWhitespaceAt(cypher, startPos) {
		startPos++
	}
	// Skip "COMPOSITE DATABASE"
	if startPos+len("COMPOSITE DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE DATABASE")], "COMPOSITE DATABASE") {
		startPos += len("COMPOSITE DATABASE")
		for startPos < len(cypher) && cyphertext.IsWhitespaceAt(cypher, startPos) {
			startPos++
		}
	} else {
		// Try with flexible whitespace
		if startPos+len("COMPOSITE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE")], "COMPOSITE") {
			startPos += len("COMPOSITE")
			for startPos < len(cypher) && cyphertext.IsWhitespaceAt(cypher, startPos) {
				startPos++
			}
			if startPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("DATABASE")], "DATABASE") {
				startPos += len("DATABASE")
				for startPos < len(cypher) && cyphertext.IsWhitespaceAt(cypher, startPos) {
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
	dbName := ""
	if name, ok := cyphertext.ScanIdentifier(cypher, startPos); ok && name.Quoted {
		dbNameEnd, dbName = name.End, name.Value
	} else {
		for dbNameEnd < len(cypher) && !cyphertext.IsWhitespaceAt(cypher, dbNameEnd) {
			dbNameEnd++
		}
		dbName = cypher[startPos:dbNameEnd]
	}
	if dbName == "" {
		return nil, localizedError(localization.CypherCompositeAlterDatabaseNameEmpty(), nil)
	}

	// Check for ADD or DROP
	remainingStart := dbNameEnd + len(cypher[dbNameEnd:]) - len(strings.TrimLeftFunc(cypher[dbNameEnd:], cyphertext.IsWhitespace))
	remaining := strings.TrimSpace(cypher[remainingStart:])
	tokens, spans, err := tokenizeSource(remaining)
	if err != nil {
		return nil, localizedError(localization.CypherCompositeAlterTokenizeFailed(err), err)
	}
	if len(tokens) >= 2 && equalFoldASCII(tokens[0], "ADD") && equalFoldASCII(tokens[1], "ALIAS") {
		idx := 1
		constituent, err := parseConstituentFromTokens(tokens, &idx, &adminCredentialReader{source: remaining, mapping: cyphertext.SourceFor(ctx, cypher), base: remainingStart, spans: spans, set: statementCredentials(ctx)})
		if err != nil {
			return nil, err
		}
		if idx != len(tokens) {
			return nil, localizedError(localization.CypherCompositeAddAliasUnexpectedToken(tokens[idx]), nil)
		}
		return &preparedCompositeCommand{database: dbName, action: compositeAdd, constituent: constituent}, nil

	} else if len(tokens) >= 2 && equalFoldASCII(tokens[0], "DROP") && equalFoldASCII(tokens[1], "ALIAS") {
		if len(tokens) < 3 {
			return nil, localizedError(localization.CypherCompositeDropAliasNameEmpty(), nil)
		}
		if len(tokens) > 3 {
			return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherCompositeConstituentUnexpectedToken(tokens[3]))
		}
		aliasName, err := parseCypherValueToken(tokens[2])
		if err != nil {
			return nil, err
		}
		if aliasName == "" {
			return nil, localizedError(localization.CypherCompositeDropAliasNameEmpty(), nil)
		}
		return &preparedCompositeCommand{database: dbName, action: compositeDrop, alias: aliasName}, nil

	} else {
		return nil, localizedError(localization.CypherCompositeAlterActionExpected(), nil)
	}
}
