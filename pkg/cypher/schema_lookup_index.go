package cypher

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Token lookup index DDL, as Neo4j 5 (#530):
//
//	CREATE LOOKUP INDEX [name] [IF NOT EXISTS] FOR (n) ON EACH labels(n)
//	CREATE LOOKUP INDEX [name] [IF NOT EXISTS] FOR ()-[r]-() ON EACH type(r)
//
// A database has at most one lookup index per entity type
// (storage/schema_lookup_index.go).

var (
	lookupNodePattern         = regexp.MustCompile(`(?is)^FOR\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*ON\s+EACH\s+labels\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)$`)
	lookupRelationshipPattern = regexp.MustCompile(`(?is)^FOR\s*\(\s*\)\s*<?-\s*\[\s*([A-Za-z_][A-Za-z0-9_]*)\s*\]\s*->?\s*\(\s*\)\s*ON\s+EACH\s+type\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)$`)
)

// executeCreateLookupIndex handles CREATE LOOKUP INDEX. A second lookup
// index of an entity type is Neo.ClientError.Schema.IndexAlreadyExists, and a
// name another index has is IndexWithNameAlreadyExists; IF NOT EXISTS makes
// both a no-op.
func (e *StorageExecutor) executeCreateLookupIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	name, ifNotExists, entityType, ok := parseCreateLookupIndex(cypher)
	if !ok {
		return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			"Invalid CREATE LOOKUP INDEX: expected FOR (n) ON EACH labels(n) or FOR ()-[r]-() ON EACH type(r)")
	}
	if err := e.storage.GetSchema().AddLookupIndex(name, entityType); err != nil {
		var localized *localization.LocalizedError
		if errors.As(err, &localized) {
			switch localized.Message.ID {
			case localization.MessageStorageSchemaLookupIndexAlreadyExists:
				if ifNotExists {
					return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
				}
				return nil, newSemanticError("Neo.ClientError.Schema.IndexAlreadyExists", "IndexAlreadyExists", err.Error())
			case localization.MessageStorageSchemaIndexNameAlreadyExists:
				if ifNotExists {
					return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
				}
				return nil, newSemanticError("Neo.ClientError.Schema.IndexWithNameAlreadyExists", "IndexWithNameAlreadyExists", err.Error())
			}
		}
		return nil, err
	}
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

// parseCreateLookupIndex reads a CREATE LOOKUP INDEX statement: its name
// ("" for the default), IF NOT EXISTS, and the entity type it indexes.
func parseCreateLookupIndex(cypher string) (name string, ifNotExists bool, entityType storage.ConstraintEntityType, ok bool) {
	end, found := keywordSpanAt(cypher, 0, "CREATE LOOKUP INDEX")
	if !found {
		return "", false, "", false
	}
	rest := strings.TrimSpace(trimTrailingStatementDelimiters(strings.TrimSpace(cypher[end:])))
	if !startsWithKeywordFold(rest, "IF") && !startsWithKeywordFold(rest, "FOR") {
		name, rest, ok = cutSchemaName(rest)
		if !ok {
			return "", false, "", false
		}
	}
	if startsWithKeywordFold(rest, "IF") {
		clauseEnd, isIfNotExists := keywordSpanAt(rest, 0, "IF NOT EXISTS")
		if !isIfNotExists {
			return "", false, "", false
		}
		ifNotExists = true
		rest = strings.TrimSpace(rest[clauseEnd:])
	}
	if match := lookupNodePattern.FindStringSubmatch(rest); match != nil && match[1] == match[2] {
		return name, ifNotExists, storage.ConstraintEntityNode, true
	}
	if match := lookupRelationshipPattern.FindStringSubmatch(rest); match != nil && match[1] == match[2] {
		return name, ifNotExists, storage.ConstraintEntityRelationship, true
	}
	return "", false, "", false
}

// cutSchemaName reads a leading plain or backtick-quoted schema object name.
func cutSchemaName(text string) (string, string, bool) {
	if strings.HasPrefix(text, "`") {
		var name strings.Builder
		for i := 1; i < len(text); i++ {
			if text[i] != '`' {
				name.WriteByte(text[i])
				continue
			}
			if i+1 < len(text) && text[i+1] == '`' {
				name.WriteByte('`')
				i++
				continue
			}
			return name.String(), strings.TrimSpace(text[i+1:]), name.Len() > 0
		}
		return "", "", false
	}
	end := 0
	for end < len(text) && isIdentByte(text[end]) {
		end++
	}
	if end == 0 {
		return "", "", false
	}
	return text[:end], strings.TrimSpace(text[end:]), true
}

// showLookupIndexCreateStatement is the statement that recreates a lookup
// index.
func showLookupIndexCreateStatement(name, entityType string) string {
	if entityType == string(storage.ConstraintEntityRelationship) {
		return "CREATE LOOKUP INDEX " + quoteSchemaName(name) + " FOR ()-[r]-() ON EACH type(r)"
	}
	return "CREATE LOOKUP INDEX " + quoteSchemaName(name) + " FOR (n) ON EACH labels(n)"
}
