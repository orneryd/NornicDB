package cypher

import (
	"context"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// DatabaseOptionsManager is the optional DatabaseManagerInterface extension
// for database options: CREATE DATABASE … OPTIONS, ALTER DATABASE … SET /
// REMOVE OPTION, and SHOW DATABASES' options column (#937).
type DatabaseOptionsManager interface {
	DatabaseOptions(name string) (map[string]string, error)
	SetDatabaseOption(name, key, value string) error
}

// txLogEnrichmentOption is Neo4j's change data capture option.
const txLogEnrichmentOption = "txLogEnrichment"

// neo4jDatabaseOptions are the CREATE DATABASE options Neo4j defines that
// NornicDB doesn't implement (seeding from a backup or URI, store formats).
var neo4jDatabaseOptions = map[string]bool{
	"existingData": true, "existingDataSeedInstance": true, "existingDataSeedServer": true,
	"seedConfig": true, "seedCredentials": true, "seedURI": true, "seedRestoreUntil": true,
	"seedSourceDatabase": true, "storeFormat": true,
}

// databaseOptionValue validates an option and returns the value stored for
// it: txLogEnrichment is OFF, DIFF or FULL (case-insensitive, stored upper
// case). Another option Neo4j defines is not supported; any other name is
// unknown. Both are Neo4j's ArgumentError.
func databaseOptionValue(key string, value interface{}) (string, error) {
	if key != txLogEnrichmentOption {
		if neo4jDatabaseOptions[key] {
			return "", localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument", localization.CypherAdminDatabaseOptionUnsupported(key))
		}
		return "", localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument", localization.CypherAdminDatabaseOptionUnknown(key))
	}
	text, _ := value.(string)
	switch upper := strings.ToUpper(strings.TrimSpace(text)); upper {
	case "OFF", "DIFF", "FULL":
		return upper, nil
	}
	return "", localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument",
		localization.CypherAdminDatabaseOptionValue(key, neo4jValueRepr(value), "'OFF', 'DIFF', 'FULL'"))
}

// splitDatabaseOptions cuts a CREATE DATABASE statement at its OPTIONS
// clause (searched from from): the statement before it, and the options it
// sets, validated. No clause is no options.
func (e *StorageExecutor) splitDatabaseOptions(ctx context.Context, cypher string, from int) (string, map[string]string, error) {
	at := keywordIndexFrom(cypher, "OPTIONS", from, defaultKeywordScanOpts())
	if at < 0 {
		return cypher, nil, nil
	}
	clause := strings.TrimSpace(cypher[at+len("OPTIONS"):])
	if !strings.HasPrefix(clause, "{") || !strings.HasSuffix(clause, "}") {
		return "", nil, localizedError(localization.CypherAdminKeywordExpected("CREATE DATABASE", "OPTIONS { ... }"), nil)
	}
	fields, isMap := e.evaluateExpressionWithContext(ctx, clause, nil, nil).(map[string]interface{})
	if !isMap {
		return "", nil, localizedError(localization.CypherAdminKeywordExpected("CREATE DATABASE", "OPTIONS { ... }"), nil)
	}
	options := make(map[string]string)
	for key, raw := range fields {
		value, err := databaseOptionValue(key, raw)
		if err != nil {
			return "", nil, err
		}
		options[key] = value
	}
	return cypher[:at], options, nil
}

// applyDatabaseOptions sets a new database's options.
func (e *StorageExecutor) applyDatabaseOptions(name string, options map[string]string) error {
	if len(options) == 0 {
		return nil
	}
	manager, ok := e.dbManager.(DatabaseOptionsManager)
	if !ok {
		return localizedError(localization.CypherAdminDatabaseOptionsUnavailable("CREATE DATABASE"), nil)
	}
	keys := make([]string, 0, len(options))
	for key := range options {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := manager.SetDatabaseOption(name, key, options[key]); err != nil {
			return err
		}
	}
	return nil
}

// alterDatabaseOption runs ALTER DATABASE name [IF EXISTS] SET OPTION key
// value / REMOVE OPTION key, from the statement's text after DATABASE;
// handled is false for another ALTER DATABASE form. A missing database is
// a no-op with IF EXISTS.
func (e *StorageExecutor) alterDatabaseOption(ctx context.Context, cypher string, from int) (result *ExecuteResult, handled bool, err error) {
	setAt := findMultiWordKeywordIndex(cypher[from:], "SET", "OPTION")
	removeAt := findMultiWordKeywordIndex(cypher[from:], "REMOVE", "OPTION")
	at, remove := setAt, false
	if removeAt >= 0 && (setAt < 0 || removeAt < setAt) {
		at, remove = removeAt, true
	}
	if at < 0 {
		return nil, false, nil
	}
	name := strings.TrimSpace(cypher[from : from+at])
	ifExists := false
	if fields := strings.Fields(name); len(fields) == 3 && strings.EqualFold(fields[1], "IF") && strings.EqualFold(fields[2], "EXISTS") {
		name, ifExists = fields[0], true
	}
	name, err = unquoteBacktickIdentifier(name)
	if err != nil || name == "" {
		return nil, true, localizedError(localization.CypherAdminDatabaseNameExpected("ALTER DATABASE"), err)
	}
	rest := strings.TrimSpace(cypher[from+at:])
	rest = strings.TrimSpace(rest[strings.IndexAny(rest, " \t\r\n"):]) // past SET / REMOVE
	rest = strings.TrimSpace(rest[len("OPTION"):])
	key, valueText, _ := strings.Cut(rest, " ")
	key = strings.TrimSpace(key)
	valueText = strings.TrimSpace(valueText)
	if key == "" || remove != (valueText == "") {
		return nil, true, localizedError(localization.CypherAdminInvalidSyntax("ALTER DATABASE"), nil)
	}
	value := ""
	if !remove {
		if value, err = databaseOptionValue(key, e.parseValue(ctx, valueText)); err != nil {
			return nil, true, err
		}
	} else if _, err := databaseOptionValue(key, "OFF"); err != nil {
		return nil, true, err
	}
	resolved, resolveErr := e.dbManager.ResolveDatabase(name)
	if resolveErr != nil || !e.dbManager.Exists(resolved) {
		if ifExists {
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, true, nil
		}
		return nil, true, localizedError(localization.CypherAdminDatabaseNotFound(name, resolveErr), resolveErr)
	}
	manager, ok := e.dbManager.(DatabaseOptionsManager)
	if !ok {
		return nil, true, localizedError(localization.CypherAdminDatabaseOptionsUnavailable("ALTER DATABASE"), nil)
	}
	if err := manager.SetDatabaseOption(resolved, key, value); err != nil {
		return nil, true, err
	}
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, true, nil
}

// databaseOptionsValue is SHOW DATABASES' options column: the database's
// options as a map, empty when it has none.
func databaseOptionsValue(manager DatabaseManagerInterface, name string) map[string]interface{} {
	value := map[string]interface{}{}
	if provider, ok := manager.(DatabaseOptionsManager); ok {
		if options, err := provider.DatabaseOptions(name); err == nil {
			for key, option := range options {
				value[key] = option
			}
		}
	}
	return value
}
