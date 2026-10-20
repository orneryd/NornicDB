package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// StatementDatabase is the one database decision for administration commands,
// used by the executor and the HTTP transaction boundary. It shares the command
// reader with USE validation; statement execution modes keep the same target.
func StatementDatabase(database, query string) string {
	_, body := parseExecutionMode(query)
	if body == "" {
		return database
	}
	// These common graph statements cannot be administration commands. The
	// full decision still belongs to fabric's reader for every candidate.
	switch body[0] | 0x20 {
	case 'a', 'c', 'd', 'e', 'g', 'r', 's', '/':
		if startsWithKeywordFold(body, "RETURN") || startsWithKeywordFold(body, "CALL") || startsWithKeywordFold(body, "SET") {
			return database
		}
		if startsWithKeywordFold(body, "CREATE") && strings.HasPrefix(strings.TrimSpace(body[len("CREATE"):]), "(") {
			return database
		}
	default:
		return database
	}
	if fabric.IsAdministrationCommand(body) {
		return "system"
	}
	return database
}

func (e *StorageExecutor) admitTransactionStatement(query string) error {
	tx := e.txContext
	if tx == nil || !tx.active {
		return nil
	}
	if tx.failed != nil {
		return queryOnFailedTransactionError(tx.failed)
	}
	kind := "Data"
	if StatementDatabase("", query) == "system" {
		kind = "Administration"
	}
	if tx.statementKind != "" && tx.statementKind != kind {
		return statementTypeError(query, kind, tx.statementKind, tx.tx)
	}
	tx.statementKind = kind
	return nil
}

// statementTypeError is Neo4j's ForbiddenDueToTransactionType for query, a
// statement of kind ("Data" or "Administration"), in a transaction whose
// statements so far are of kind previousKind; its storage transaction, when
// it holds writes, makes a data transaction's type "Write query". The
// message uses Neo4j's statement type names: "Read query", "Write query",
// "Schema modification", "Administration command".
func statementTypeError(query, kind, previousKind string, transaction any) error {
	requested := "Read query"
	permissions := QueryPermissionRequirements(query)
	if permissions.Write {
		requested = "Write query"
	}
	if permissions.Schema {
		requested = "Schema modification"
	}
	if kind == "Administration" {
		requested = "Administration command"
	}
	previous := "Administration command"
	if previousKind == "Data" {
		previous = "Read query"
		if writes, ok := transaction.(interface{ OperationCount() int }); ok && writes.OperationCount() > 0 {
			previous = "Write query"
		}
	}
	return localizedStatusError("Neo.ClientError.Transaction.ForbiddenDueToTransactionType", "ForbiddenDueToTransactionType",
		localization.CypherAdminTransactionStatementType(previous, requested))
}

func (e *StorageExecutor) routeAdministrationStatement(ctx context.Context, query string) (*StorageExecutor, error) {
	if StatementDatabase("", query) != "system" || strings.EqualFold(e.currentDatabaseName(), "system") {
		return e, nil
	}
	system, err := e.systemStorage()
	if err != nil {
		return nil, err
	}
	// An embedded executor with no database manager uses its own system store.
	if system == e.storage {
		return e, nil
	}
	if e.txContext == nil || !e.txContext.active {
		// Implicit administration routing keeps the request's entitlement
		// context; it chooses the store without selecting another user graph.
		return e.cloneForStorage(system), nil
	}
	if e.txContext.storageWrapper != nil && strings.EqualFold(e.txContext.storageWrapper.Namespace(), "system") {
		return e, nil
	}
	// An administration transaction chooses the system database with its first
	// statement. Admission forbids switching after a data statement, even one
	// that wrote nothing. Replace only the unused initial snapshot and keep
	// the caller's transaction metadata; all commands then share this one view.
	metadata := map[string]any(nil)
	if tx, ok := e.txContext.tx.(*storage.BadgerTransaction); ok {
		metadata = tx.GetMetadata()
	}
	if _, err := e.handleRollback(); err != nil {
		return nil, err
	}
	owner := e.cloneForStorage(system)
	if _, err := owner.handleBegin(); err != nil {
		return nil, err
	}
	e.txContext = owner.txContext
	e.txContext.statementKind = "Administration"
	tx := e.txContext.tx.(*storage.BadgerTransaction)
	if err := tx.SetMetadata(metadata); err != nil {
		return nil, err
	}
	namespace := ""
	if scoped, ok := system.(interface{ Namespace() string }); ok {
		namespace = scoped.Namespace()
	}
	separator := ""
	if namespace != "" {
		separator = ":"
	}
	e.txContext.storageWrapper = &transactionStorageWrapper{tx: tx, underlying: system, namespace: namespace, separator: separator}
	e.txContext.storageWrapper.initNamespacePrefix()
	return e, nil
}
