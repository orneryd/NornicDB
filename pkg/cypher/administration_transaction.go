// Author: Codex, NOR-180 terminal outcome context propagation,29October2026.
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

// admitTransactionStatement is the transaction entrance for controls, scripts
// and data/administration work. Its control result lets callers route ordinary
// statements without reclassifying lifecycle text before script cloning.
func (e *StorageExecutor) admitTransactionStatement(ctx context.Context, query string) (control bool, err error) {
	word := bareTransactionCommand(query)
	if ctx.Value(transactionStatementOriginKey{}) == true && word != "" {
		message := localization.CypherTransactionsCommandNotStatement(word)
		return true, localizedError(message, newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", message.Fallback))
	}
	if word != "" || startsWithKeywordFold(query, "BEGIN") && transactionScriptShape(query) {
		if word == "" {
			word = "BEGIN"
		}
		return true, e.checkTransactionControl(ctx, word)
	}
	tx := e.txContext
	if tx == nil || !tx.active {
		return false, nil
	}
	if err := tx.failedStatementError(); err != nil {
		return false, err
	}
	kind := "Data"
	if StatementDatabase("", query) == "system" {
		kind = "Administration"
	}
	if tx.statementKind != "" && tx.statementKind != kind {
		return false, statementTypeError(ctx, query, kind, tx.statementKind, tx.tx)
	}
	tx.statementKind = kind
	return false, nil
}

// statementTypeError is Neo4j's ForbiddenDueToTransactionType for query, a
// statement of kind ("Data" or "Administration"), in a transaction whose
// statements so far are of kind previousKind; its storage transaction, when
// it holds writes, makes a data transaction's type "Write query". The
// message uses Neo4j's statement type names: "Read query", "Write query",
// "Schema modification", "Administration command".
func statementTypeError(ctx context.Context, query, kind, previousKind string, transaction any) error {
	requested := "Read query"
	permissions := QueryPermissionRequirements(ctx, query, AcceptedSource)
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
	if StatementDatabase("", query) != "system" || strings.EqualFold(e.databaseName(), "system") {
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
		return e.cloneWithStorage(system, selectedDatabaseStorage), nil
	}
	if e.txContext.storageWrapper != nil && strings.EqualFold(e.txContext.storageWrapper.Namespace(), "system") {
		return e, nil
	}
	// A terminated transaction is not re-routed: its statement is refused
	// with the termination's status (withRunningStatement).
	if e.txContext.running != nil && e.txContext.running.terminated.Load() {
		return e, nil
	}
	// An administration transaction chooses the system database with its first
	// statement. Admission forbids switching after a data statement, even one
	// that wrote nothing. Replace only the unused initial snapshot and keep
	// the caller's transaction metadata; all commands then share this one view.
	// The client's handle stays the same (its SHOW/TERMINATE registration,
	// idle timeout and failure state, NOR-284); only its storage snapshot
	// changes.
	tc := e.txContext
	metadata := map[string]any(nil)
	if tx, ok := tc.tx.(*storage.BadgerTransaction); ok {
		metadata = tx.GetMetadata()
	}
	if err, _ := e.retireTransactionStorage(ctx, nil); err != nil {
		e.releaseTransactionContext()
		return nil, err
	}
	opener := e.cloneWithStorage(system, selectedDatabaseStorage)
	opener.txContext = nil
	if _, err := opener.handleBegin(ctx); err != nil {
		e.releaseTransactionContext()
		return nil, err
	}
	snapshot := opener.txContext
	tc.tx, tc.engine, tc.txID = snapshot.tx, snapshot.engine, snapshot.txID
	tc.wal, tc.walSeqStart, tc.database = snapshot.wal, snapshot.walSeqStart, snapshot.database
	tc.storageRolledBack = false
	tc.statementKind = "Administration"
	// Transfer the already-issued native view to this handle; a replacement
	// wrapper would lose the membership that distinguishes the owner from members.
	wrapper := opener.storage.(*transactionStorageWrapper)
	wrapper.owner = tc
	tc.storageWrapper = wrapper
	e.storage = wrapper
	tx := tc.tx.(*storage.BadgerTransaction)
	if err := tx.SetMetadata(metadata); err != nil {
		return nil, err
	}

	return e, nil
}
