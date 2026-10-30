package cypher

import (
	"context"
	"errors"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
)

type databaseDropKind uint8

const (
	dropStandardDatabase databaseDropKind = iota
	dropCompositeDatabase
)

// failure retains each administration command's existing localized identity.
func (kind databaseDropKind) failure(standard, composite localization.Message, cause error) error {
	if kind == dropCompositeDatabase {
		return localizedError(composite, cause)
	}
	return localizedError(standard, cause)
}

// executeDatabaseDrop owns target admission before either catalog deletion.
// Whitespace separates syntax; it must never join two names into a different
// database. Both commands decode quoted identities through the same reader.
func (e *StorageExecutor) executeDatabaseDrop(ctx context.Context, query string, kind databaseDropKind) (result *ExecuteResult, retErr error) {
	ctx = withStatementCredentials(ctx, query)
	defer redactAdminCommandError(query, &retErr, statementCredentials(ctx))
	query = cyphertext.SourceFor(ctx, query).Text
	if e.dbManager == nil {
		return nil, kind.failure(localization.CypherAdminDatabaseManagerUnavailable("DROP DATABASE"), localization.CypherCompositeDatabaseManagerUnavailable("DROP COMPOSITE DATABASE"), nil)
	}

	subject := "DATABASE"
	if kind == dropCompositeDatabase {
		subject = "COMPOSITE DATABASE"
	}
	invalidSyntax := func() error {
		return kind.failure(localization.CypherAdminInvalidSyntax("DROP DATABASE"), localization.CypherCompositeDropInvalidSyntax(), nil)
	}
	at := findMultiWordKeywordIndex(query, "DROP", subject)
	if at < 0 {
		return nil, invalidSyntax()
	}
	end := keywordEndAt(query, at, "DROP "+subject)
	if end == at {
		return nil, invalidSyntax()
	}
	rawName := strings.TrimSpace(query[end:])
	if rawName == "" {
		return nil, kind.failure(localization.CypherAdminDatabaseNameExpected("DROP DATABASE"), localization.CypherCompositeDropDatabaseNameExpected(), nil)
	}

	ifExists := false
	if kind == dropStandardDatabase {
		if clause := findMultiWordKeywordIndex(rawName, "IF", "EXISTS"); clause >= 0 {
			// Validate the entire suffix before admitting any deletion.
			clauseEnd := keywordEndAt(rawName, clause, "IF EXISTS")
			if strings.TrimSpace(rawName[clauseEnd:]) != "" {
				return nil, invalidSyntax()
			}
			ifExists = true
			rawName = strings.TrimSpace(rawName[:clause])
		}
	}
	name, err := unquoteBacktickIdentifier(rawName)
	if err != nil {
		return nil, localizedError(localization.CypherAdminInvalidIdentifier(rawName), err)
	}
	if name == "" {
		return nil, kind.failure(localization.CypherAdminDatabaseNameEmpty("DROP DATABASE"), localization.CypherCompositeDropDatabaseNameEmpty(), nil)
	}
	if strings.IndexFunc(name, cyphertext.IsWhitespace) >= 0 {
		return nil, localizedError(localization.CypherAdminInvalidDatabaseName(name), nil)
	}
	if kind == dropCompositeDatabase {
		err = e.dbManager.DropCompositeDatabase(ctx, name)
	} else {
		// Author: NOR-288 lane (Claude), 2026-10-30. The DROP owner alone decides
		// absent, pending completion and existing; a record-only precheck here
		// would leave an absent-record pending drop unfinished.
		err = e.dbManager.DropDatabase(ctx, name)
		if errors.Is(err, multidb.ErrDatabaseNotFound) {
			if ifExists {
				return &ExecuteResult{Columns: []string{"name"}, Rows: [][]interface{}{}}, nil
			}
			return nil, localizedError(localization.CypherAdminDatabaseDoesNotExist(name), nil)
		}
	}
	if err != nil {
		return nil, kind.failure(localization.CypherAdminDropDatabaseFailed(name, err), localization.CypherCompositeDropDatabaseFailed(name, err), err)
	}
	return &ExecuteResult{Columns: []string{"name"}, Rows: [][]interface{}{{name}}}, nil
}
