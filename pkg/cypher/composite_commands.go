// Package cypher provides composite database command execution.
package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/catalogadapter"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/observability"
)

func parseCypherValueToken(tok string) (string, error) {
	tok = strings.TrimSpace(tok)
	if tok == "" {
		return "", nil
	}
	if decoded, quoted := cyphertext.DecodeString(tok, cyphertext.AcceptedSource); quoted {
		return decoded, nil
	}
	if strings.HasPrefix(tok, "`") {
		return unquoteBacktickIdentifier(tok)
	}
	return tok, nil
}

func parseConstituentFromTokens(tokens []string, idx *int, readers ...*adminCredentialReader) (map[string]interface{}, error) {
	if *idx >= len(tokens) || !equalFoldASCII(tokens[*idx], "ALIAS") {
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

	if *idx+1 >= len(tokens) || !equalFoldASCII(tokens[*idx], "FOR") || !equalFoldASCII(tokens[*idx+1], "DATABASE") {
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
	var reader *adminCredentialReader
	if len(readers) != 0 {
		reader = readers[0]
	}
	hasUserPassword := false
	hasOIDCForwarding := false

	for *idx < len(tokens) {
		if equalFoldASCII(tokens[*idx], "ALIAS") {
			break
		}

		switch {
		case equalFoldASCII(tokens[*idx], "AT"):
			*idx = *idx + 1
			if *idx >= len(tokens) {
				return nil, localizedError(localization.CypherCompositeConstituentRemoteURIExpected(), nil)
			}
			uri, err := parseCypherValueToken(tokens[*idx])
			reader.record(*idx, uri, false)
			if err != nil {
				return nil, localizedError(localization.CypherCompositeConstituentRemoteURIInvalid(err), err)
			}
			*idx = *idx + 1
			if strings.TrimSpace(uri) == "" {
				return nil, localizedError(localization.CypherCompositeConstituentRemoteURIEmpty(), nil)
			}
			ref["uri"] = uri
			ref["type"] = "remote"

		case equalFoldASCII(tokens[*idx], "USER"):
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

		case equalFoldASCII(tokens[*idx], "PASSWORD"):
			*idx = *idx + 1
			if *idx >= len(tokens) {
				return nil, localizedError(localization.CypherCompositeConstituentPasswordEmpty(), nil)
			}
			password, err := parseCypherValueToken(tokens[*idx])
			reader.record(*idx, password, true)
			if err != nil {
				return nil, localizedError(localization.CypherCompositeConstituentPasswordInvalid(err), err)
			}
			*idx = *idx + 1
			if strings.TrimSpace(password) == "" {
				return nil, localizedError(localization.CypherCompositeConstituentPasswordEmpty(), nil)
			}
			ref["password"] = password
			hasUserPassword = true

		case equalFoldASCII(tokens[*idx], "OIDC"):
			*idx = *idx + 1
			if *idx+1 >= len(tokens) || !equalFoldASCII(tokens[*idx], "CREDENTIAL") || !equalFoldASCII(tokens[*idx+1], "FORWARDING") {
				return nil, localizedError(localization.CypherCompositeConstituentOIDCCredentialExpected(), nil)
			}
			*idx += 2
			hasOIDCForwarding = true

		case equalFoldASCII(tokens[*idx], "SECRET"):
			*idx = *idx + 1
			if *idx >= len(tokens) || !equalFoldASCII(tokens[*idx], "REF") {
				return nil, localizedError(localization.CypherCompositeConstituentSecretRefExpected(), nil)
			}
			*idx = *idx + 1
			if *idx >= len(tokens) {
				return nil, localizedError(localization.CypherCompositeConstituentSecretRefEmpty(), nil)
			}
			secretRef, err := parseCypherValueToken(tokens[*idx])
			reader.record(*idx, secretRef, true)
			if err != nil {
				return nil, localizedError(localization.CypherCompositeConstituentSecretRefInvalid(err), err)
			}
			*idx = *idx + 1
			if strings.TrimSpace(secretRef) == "" {
				return nil, localizedError(localization.CypherCompositeConstituentSecretRefEmpty(), nil)
			}
			ref["secret_ref"] = secretRef

		case equalFoldASCII(tokens[*idx], "TYPE"):
			*idx = *idx + 1
			if *idx >= len(tokens) {
				return nil, localizedError(localization.CypherCompositeConstituentTypeEmpty(), nil)
			}
			typeVal, err := parseCypherValueToken(tokens[*idx])
			if err != nil {
				return nil, localizedError(localization.CypherCompositeConstituentTypeInvalid(err), err)
			}
			*idx = *idx + 1
			typeVal = lowerASCII(strings.TrimSpace(typeVal))
			if typeVal != "local" && typeVal != "remote" {
				return nil, localizedError(localization.CypherCompositeConstituentTypeUnsupported(), nil)
			}
			// Reject contradictory AT + TYPE local: AT implies remote.
			if existingType, ok := ref["type"]; ok && existingType == "remote" && typeVal == "local" {
				return nil, localizedError(localization.CypherCompositeConstituentTypeContradictsAT(), nil)
			}
			ref["type"] = typeVal

		case equalFoldASCII(tokens[*idx], "ACCESS"):
			*idx = *idx + 1
			if *idx >= len(tokens) {
				return nil, localizedError(localization.CypherCompositeConstituentAccessModeEmpty(), nil)
			}
			accessVal, err := parseCypherValueToken(tokens[*idx])
			if err != nil {
				return nil, localizedError(localization.CypherCompositeConstituentAccessModeInvalid(err), err)
			}
			*idx = *idx + 1
			accessVal = lowerASCII(strings.TrimSpace(accessVal))
			switch accessVal {
			case "read", "write", "read_write":
				ref["access_mode"] = accessVal
			default:
				return nil, localizedError(localization.CypherCompositeConstituentAccessModeUnsupported(), nil)
			}

		default:
			value, _ := parseCypherValueToken(tokens[*idx])
			reader.record(*idx, value, false)
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

// compositeCommandFamily preserves the command selected by routing, including
// family-specific diagnostics when a direct caller supplies malformed text.
type compositeCommandFamily string

const (
	compositeCreateCommand compositeCommandFamily = "CREATE COMPOSITE DATABASE"
	compositeAlterCommand  compositeCommandFamily = "ALTER COMPOSITE DATABASE"
)

// executeCompositeCommand owns CREATE/ALTER admission and effects. Preparation
// remains shared with metadata and EXPLAIN; only this execution path mutates the
// catalog. Manager availability takes precedence over parsing on both families.
func (e *StorageExecutor) executeCompositeCommand(ctx context.Context, cypher string, family compositeCommandFamily) (_ *ExecuteResult, retErr error) {
	ctx = withStatementCredentials(ctx, cypher)
	defer redactAdminCommandError(cypher, &retErr, statementCredentials(ctx))
	if e.dbManager == nil {
		return nil, localizedError(localization.CypherCompositeDatabaseManagerUnavailable(string(family)), nil)
	}
	var command *preparedCompositeCommand
	var err error
	switch family {
	case compositeCreateCommand:
		command, err = e.prepareCreateCompositeDatabase(ctx, cypher)
	case compositeAlterCommand:
		command, err = e.prepareAlterCompositeDatabase(ctx, cypher)
	}
	if err != nil {
		return nil, err
	}
	return command.execute(ctx, e)
}

// executeShowCompositeDatabases handles SHOW COMPOSITE DATABASES command.
func (e *StorageExecutor) executeShowCompositeDatabases(ctx context.Context, cypher string) (_ *ExecuteResult, retErr error) {
	ctx = withStatementCredentials(ctx, cypher)
	defer redactAdminCommandError(cypher, &retErr, statementCredentials(ctx))
	cypher = cyphertext.SourceFor(ctx, cypher).Text
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

// compositeDisplayURI projects command output only. Keep nil local values and
// stored/runtime URIs unchanged; the shared URL owner redacts remote userinfo.
func compositeDisplayURI(value interface{}) interface{} {
	if uri, ok := value.(string); ok {
		return observability.RedactURL(uri)
	}
	return value
}

func compositeDisplayUser(value, mode interface{}) interface{} {
	if user, ok := value.(string); ok {
		authMode, _ := mode.(string)
		return multidb.RemoteUserForDisplay(user, authMode)
	}
	return value
}

// executeShowConstituents handles SHOW CONSTITUENTS FOR COMPOSITE DATABASE command.
func (e *StorageExecutor) executeShowConstituents(ctx context.Context, cypher string) (_ *ExecuteResult, retErr error) {
	ctx = withStatementCredentials(ctx, cypher)
	defer redactAdminCommandError(cypher, &retErr, statementCredentials(ctx))
	cypher = cyphertext.SourceFor(ctx, cypher).Text
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
		// A tagged map carries the same manager-owned connection authority as
		// the typed adapter. Legacy map cells retain their existing output values.
		ref, typed := c.(multidb.ConstituentRef)
		if !typed {
			if candidate, ok := catalogadapter.ConstituentRef(c, catalogadapter.StoredConstituentRecord); ok && candidate.ConnectionV1 != "" {
				ref, typed = candidate, true
			}
		}
		if typed {
			var err error
			ref, err = e.constituentDisplayView(ref)
			if err != nil {
				return nil, err
			}
			rows[i] = []interface{}{
				ref.Alias,
				ref.DatabaseName,
				ref.Type,
				ref.AccessMode,
				compositeDisplayURI(ref.URI),
				ref.SecretRef,
				ref.AuthMode,
				multidb.RemoteUserForDisplay(ref.User, ref.AuthMode),
			}
		} else if m, ok := c.(map[string]interface{}); ok {
			// Fallback for map format (if returned as map)
			rows[i] = []interface{}{
				m["alias"],
				m["database_name"],
				m["type"],
				m["access_mode"],
				compositeDisplayURI(m["uri"]),
				m["secret_ref"],
				m["auth_mode"],
				compositeDisplayUser(m["user"], m["auth_mode"]),
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
