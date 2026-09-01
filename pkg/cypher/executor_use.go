package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// parseLeadingUseClause extracts a leading `USE <database>` clause from a query fragment.
// It returns the selected database, remaining query, and whether a USE clause was found.
func parseLeadingUseClause(cypher string) (database, remaining string, hasUse bool, err error) {
	trimmed := strings.TrimSpace(cypher)
	if !startsWithKeywordFold(trimmed, "USE") {
		return "", cypher, false, nil
	}

	rest := strings.TrimSpace(trimmed[len("USE"):])
	if rest == "" {
		return "", "", true, localizedError(localization.CypherCommandRoutingUseDatabaseRequired(), nil)
	}

	if graphRef, rem, ok, err := parseDynamicGraphReference(rest); ok {
		if err != nil {
			return "", "", true, localizedError(localization.CypherCommandRoutingUseInvalid(err), err)
		}
		return graphRef, strings.TrimSpace(rem), true, nil
	}

	if strings.HasPrefix(rest, "`") {
		// Backtick-quoted identifier. Support escaped backticks using ``.
		var b strings.Builder
		escaped := false
		for i := 1; i < len(rest); i++ {
			ch := rest[i]
			if ch == '`' {
				if i+1 < len(rest) && rest[i+1] == '`' {
					b.WriteByte('`')
					i++
					continue
				}
				database = b.String()
				remaining = strings.TrimSpace(rest[i+1:])
				return database, remaining, true, nil
			}
			if escaped {
				escaped = false
			}
			b.WriteByte(ch)
		}
		return "", "", true, localizedError(localization.CypherCommandRoutingUseBacktickUnterminated(), nil)
	}

	parts := strings.Fields(rest)
	if len(parts) == 0 {
		return "", "", true, localizedError(localization.CypherCommandRoutingUseDatabaseRequired(), nil)
	}

	database = parts[0]
	if len(parts) > 1 {
		remaining = strings.TrimSpace(strings.TrimPrefix(rest, database))
	}

	return database, remaining, true, nil
}

func parseDynamicGraphReference(rest string) (database, remaining string, ok bool, err error) {
	trimmed := strings.TrimSpace(rest)
	lower := strings.ToLower(trimmed)

	for _, prefix := range []string{"graph.byname(", "graph.byelementid("} {
		if !strings.HasPrefix(lower, prefix) {
			continue
		}

		openIdx := strings.Index(trimmed, "(")
		if openIdx < 0 {
			return "", "", true, localizedError(localization.CypherCommandRoutingGraphReferenceInvalid(), nil)
		}
		closeIdx, err := findMatchingParenInUse(trimmed, openIdx)
		if err != nil {
			return "", "", true, err
		}

		arg := strings.TrimSpace(trimmed[openIdx+1 : closeIdx])
		if arg == "" {
			return "", "", true, localizedError(localization.CypherCommandRoutingGraphReferenceArgumentRequired(), nil)
		}

		db, err := parseFirstGraphRefArg(arg)
		if err != nil {
			return "", "", true, err
		}

		return db, trimmed[closeIdx+1:], true, nil
	}

	return "", "", false, nil
}

func findMatchingParenInUse(s string, pos int) (int, error) {
	if pos >= len(s) || s[pos] != '(' {
		return -1, localizedError(localization.CypherCommandRoutingGraphReferenceOpenParenExpected(pos), nil)
	}

	depth := 1
	inSingle := false
	inDouble := false
	for i := pos + 1; i < len(s); i++ {
		ch := s[i]
		if ch == '\'' && !inDouble {
			if inSingle {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inSingle = false
			} else {
				inSingle = true
			}
			continue
		}
		if ch == '"' && !inSingle {
			if inDouble {
				if i+1 < len(s) && s[i+1] == '"' {
					i++
					continue
				}
				inDouble = false
			} else {
				inDouble = true
			}
			continue
		}
		if inSingle || inDouble {
			continue
		}

		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}

	return -1, localizedError(localization.CypherCommandRoutingGraphReferenceUnterminated(), nil)
}

func parseFirstGraphRefArg(arg string) (string, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return "", localizedError(localization.CypherCommandRoutingGraphReferenceArgumentEmpty(), nil)
	}

	if arg[0] == '\'' || arg[0] == '"' {
		quote := arg[0]
		for i := 1; i < len(arg); i++ {
			if arg[i] == quote {
				if i+1 < len(arg) && arg[i+1] == quote {
					i++
					continue
				}
				return arg[1:i], nil
			}
		}
		return "", localizedError(localization.CypherCommandRoutingGraphReferenceStringUnterminated(), nil)
	}

	if arg[0] == '`' {
		for i := 1; i < len(arg); i++ {
			if arg[i] == '`' {
				if i+1 < len(arg) && arg[i+1] == '`' {
					i++
					continue
				}
				return strings.ReplaceAll(arg[1:i], "``", "`"), nil
			}
		}
		return "", localizedError(localization.CypherCommandRoutingBacktickIdentifierUnterminated(), nil)
	}

	fields := strings.Fields(arg)
	if len(fields) == 0 {
		return "", localizedError(localization.CypherCommandRoutingGraphReferenceArgumentRequired(), nil)
	}
	return fields[0], nil
}

func (e *StorageExecutor) cloneForStorage(store storage.Engine) *StorageExecutor {
	cloned := NewStorageExecutor(store)
	cloned.deferFlush = e.deferFlush
	cloned.embedder = e.embedder
	// Do not propagate the parent's search service to composite engines —
	// composite search must come from constituent-scoped executors, not
	// from a parent-namespace-scoped service.
	if !isCompositeRoot(store) {
		cloned.searchService = e.searchService
	}
	cloned.inferenceManager = e.inferenceManager
	cloned.onNodeMutated = e.onNodeMutated
	cloned.inlineEmbeddingTextOptions = e.inlineEmbeddingTextOptions
	cloned.inlineEmbeddingChunkSize = e.inlineEmbeddingChunkSize
	cloned.inlineEmbeddingChunkOverlap = e.inlineEmbeddingChunkOverlap
	cloned.allowLocalAPOCImportFileAccess = e.allowLocalAPOCImportFileAccess
	cloned.allowLocalAPOCExportFileAccess = e.allowLocalAPOCExportFileAccess
	cloned.allowRemoteAPOCURLAccess = e.allowRemoteAPOCURLAccess
	cloned.apocRemoteURLAllowlist = append([]string(nil), e.apocRemoteURLAllowlist...)
	cloned.apocLocalFileAccessRoot = e.apocLocalFileAccessRoot
	cloned.defaultEmbeddingDimensions = e.defaultEmbeddingDimensions
	cloned.dbManager = e.dbManager
	cloned.vectorRegistry = e.vectorRegistry
	cloned.vectorIndexSpaces = e.vectorIndexSpaces
	cloned.txContext = e.txContext
	cloned.fabricPlanCache = e.fabricPlanCache
	cloned.hotPathTraceState = e.hotPathTraceState

	e.shellParamsMu.RLock()
	if len(e.shellParams) > 0 {
		cloned.shellParams = make(map[string]interface{}, len(e.shellParams))
		for k, v := range e.shellParams {
			cloned.shellParams[k] = v
		}
	}
	e.shellParamsMu.RUnlock()

	return cloned
}

func (e *StorageExecutor) scopedExecutorForUse(db string, authToken string) (*StorageExecutor, string, error) {
	targetDB := strings.TrimSpace(db)
	if targetDB == "" {
		return nil, "", localizedError(localization.CypherCommandRoutingUseDatabaseRequired(), nil)
	}

	if e.dbManager != nil {
		// Handle dotted composite.constituent references (e.g. "nornic.tr").
		// Split at first dot: composite name + constituent alias.
		if dotIdx := strings.IndexByte(targetDB, '.'); dotIdx > 0 {
			compositeName := targetDB[:dotIdx]
			if e.dbManager.IsCompositeDatabase(compositeName) {
				currentDB := strings.TrimSpace(e.currentDatabaseName())
				if currentDB != "" && e.dbManager.IsCompositeDatabase(currentDB) && !strings.EqualFold(currentDB, compositeName) {
					return nil, "", localizedError(localization.CypherCommandRoutingUseConstituentOutsideComposite(targetDB, compositeName, currentDB), nil)
				}
				// Resolve the full composite.constituent via GetStorageForUse.
				// The composite engine's getConstituent will resolve the alias.
				return e.resolveCompositeConstituent(targetDB, compositeName, targetDB[dotIdx+1:], authToken)
			}
		}

		// Check if the target is itself a composite database.
		if e.dbManager.IsCompositeDatabase(targetDB) {
			return e.resolveCompositeStorage(targetDB, authToken)
		}

		// Standard database: resolve alias and switch namespace.
		resolved, err := e.dbManager.ResolveDatabase(targetDB)
		if err != nil {
			return nil, "", localizedError(localization.CypherCommandRoutingUseFailed(targetDB, err), err)
		}
		targetDB = resolved
	}

	ns, ok := e.storage.(*storage.NamespacedEngine)
	if !ok {
		return nil, "", localizedError(localization.CypherCommandRoutingUseBackendUnsupported(targetDB), nil)
	}

	if strings.EqualFold(ns.Namespace(), targetDB) {
		return e, targetDB, nil
	}

	scopedStore := storage.NewNamespacedEngine(ns.GetInnerEngine(), targetDB)
	return e.cloneForStorage(scopedStore), targetDB, nil
}

// resolveCompositeStorage resolves USE <composite> to a CompositeEngine-backed executor.
func (e *StorageExecutor) resolveCompositeStorage(compositeName string, authToken string) (*StorageExecutor, string, error) {
	if e.dbManager == nil {
		return nil, "", localizedError(localization.CypherCommandRoutingUseDatabaseManagerUnavailable(compositeName), nil)
	}

	engineIface, err := e.dbManager.GetStorageForUse(compositeName, authToken)
	if err != nil {
		return nil, "", localizedError(localization.CypherCommandRoutingUseFailed(compositeName, err), err)
	}

	engine, ok := engineIface.(storage.Engine)
	if !ok {
		return nil, "", localizedError(localization.CypherCommandRoutingUseStorageTypeInvalid(compositeName), nil)
	}

	return e.cloneForStorage(engine), compositeName, nil
}

// resolveCompositeConstituent resolves USE <composite.alias> to a specific
// constituent engine within a composite database.
func (e *StorageExecutor) resolveCompositeConstituent(fullName, compositeName, alias string, authToken string) (*StorageExecutor, string, error) {
	if e.dbManager == nil {
		return nil, "", localizedError(localization.CypherCommandRoutingUseDatabaseManagerUnavailable(fullName), nil)
	}

	// Get the composite engine first.
	engineIface, err := e.dbManager.GetStorageForUse(compositeName, authToken)
	if err != nil {
		return nil, "", localizedError(localization.CypherCommandRoutingUseFailed(fullName, err), err)
	}

	compositeEngine, ok := engineIface.(*storage.CompositeEngine)
	if !ok {
		return nil, "", localizedError(localization.CypherCommandRoutingUseDatabaseNotComposite(fullName, compositeName), nil)
	}

	// Resolve the specific constituent by alias.
	constituentEngine, err := compositeEngine.GetConstituentByAlias(alias)
	if err != nil {
		return nil, "", localizedError(localization.CypherCommandRoutingUseFailed(fullName, err), err)
	}

	return e.cloneForStorage(constituentEngine), fullName, nil
}
