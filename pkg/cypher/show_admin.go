package cypher

import (
	"context"
	"sort"

	"github.com/orneryd/nornicdb/pkg/auth"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Administration SHOW commands (#718): SHOW DEFAULT / HOME DATABASE,
// SHOW USERS / SHOW CURRENT USER, SHOW TRANSACTIONS / TERMINATE
// TRANSACTIONS, and Neo4j's status codes for the administration commands
// NornicDB doesn't offer. Every listing runs through executeShowWithTail, so
// YIELD / WHERE / RETURN behave as for every SHOW command.

type ctxKeyRequestIdentity struct{}
type ctxKeyRunningTransaction struct{}

// RequestIdentity is what the executor knows about where a statement comes
// from: the connection (SHOW TRANSACTIONS' connectionId, clientAddress and
// protocol), the signed-in user (SHOW CURRENT USER, SHOW TRANSACTIONS'
// username) and the user store SHOW USERS lists. A server builds it once per
// Bolt session or HTTP request and attaches it with WithRequestIdentity, so
// a statement carries one context value for all three.
type RequestIdentity struct {
	Connection ClientConnection
	// User is nil when no one signed in.
	User *AuthenticatedUser
	// Users lists the user store; nil without one, and SHOW USERS then
	// lists the signed-in user only.
	Users func() []UserListing
}

// WithRequestIdentity attaches a request's identity.
func WithRequestIdentity(ctx context.Context, identity *RequestIdentity) context.Context {
	if identity == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyRequestIdentity{}, identity)
}

func requestIdentityFromContext(ctx context.Context) *RequestIdentity {
	identity, _ := ctx.Value(ctxKeyRequestIdentity{}).(*RequestIdentity)
	return identity
}

// AuthenticatedUser is the signed-in user of a request: SHOW CURRENT USER
// lists it, and SHOW TRANSACTIONS reports its name as the username.
type AuthenticatedUser struct {
	Name  string
	Roles []string
}

func authenticatedUserFromContext(ctx context.Context) (AuthenticatedUser, bool) {
	identity := requestIdentityFromContext(ctx)
	if identity == nil || identity.User == nil || strings.TrimSpace(identity.User.Name) == "" {
		return AuthenticatedUser{}, false
	}
	return *identity.User, true
}

// UserListing is one SHOW USERS row.
type UserListing struct {
	Name                   string
	Roles                  []string
	PasswordChangeRequired bool
	Suspended              bool
}

// UserListingsFromAuth converts the auth store's users for SHOW USERS; a
// disabled user is suspended.
func UserListingsFromAuth(users []*auth.User) []UserListing {
	listings := make([]UserListing, 0, len(users))
	for _, user := range users {
		if user == nil {
			continue
		}
		roles := make([]string, 0, len(user.Roles))
		for _, role := range user.Roles {
			roles = append(roles, string(role))
		}
		listings = append(listings, UserListing{Name: user.Username, Roles: roles, Suspended: user.Disabled})
	}
	return listings
}

// ClientConnection identifies the connection a statement arrived on, for
// SHOW TRANSACTIONS (connectionId, clientAddress, protocol).
type ClientConnection struct {
	ID       string
	Address  string
	Protocol string
}

func clientConnectionFromContext(ctx context.Context) ClientConnection {
	if identity := requestIdentityFromContext(ctx); identity != nil {
		return identity.Connection
	}
	return ClientConnection{}
}

// showDefaultDatabaseColumns / showHomeDatabaseColumns: SHOW DEFAULT
// DATABASE and SHOW HOME DATABASE list SHOW DATABASES' columns without
// default and home, as in Neo4j 5.26.
var showSingleDatabaseDefaultColumns = []string{"name", "type", "aliases", "access", "address", "role", "writer", "requestedStatus", "currentStatus", "statusMessage", "constituents"}

// executeShowDefaultDatabase lists the default database. NornicDB has no
// per-user home database, so SHOW HOME DATABASE lists the default one too.
func (e *StorageExecutor) executeShowDefaultDatabase(ctx context.Context, cypher string) (*ExecuteResult, error) {
	all, err := e.executeShowDatabases(ctx, cypher)
	if err != nil {
		return nil, err
	}
	defaultColumn := -1
	keep := make([]int, 0, len(all.Columns))
	columns := make([]string, 0, len(all.Columns))
	for i, column := range all.Columns {
		switch column {
		case "default":
			defaultColumn = i
		case "home":
		default:
			keep = append(keep, i)
			columns = append(columns, column)
		}
	}
	rows := make([][]interface{}, 0, 1)
	for _, row := range all.Rows {
		if defaultColumn < 0 || defaultColumn >= len(row) || row[defaultColumn] != true {
			continue
		}
		projected := make([]interface{}, len(keep))
		for i, source := range keep {
			projected[i] = row[source]
		}
		rows = append(rows, projected)
	}
	return withShowDefaultColumns(&ExecuteResult{Columns: columns, Rows: rows}, showSingleDatabaseDefaultColumns), nil
}

// showUsersColumns is SHOW USERS' column set in Neo4j 5.26 (default and full).
var showUsersColumns = []string{"user", "roles", "passwordChangeRequired", "suspended", "home"}

// executeShowUsers lists the user store (SHOW USERS) or the signed-in user
// (SHOW CURRENT USER). home is null: NornicDB has no per-user home database.
func (e *StorageExecutor) executeShowUsers(ctx context.Context, cypher string) (*ExecuteResult, error) {
	current, signedIn := authenticatedUserFromContext(ctx)
	currentOnly := findMultiWordKeywordIndex(cypher, "SHOW", "CURRENT USER") == 0
	var users []UserListing
	if identity := requestIdentityFromContext(ctx); identity != nil && identity.Users != nil {
		users = identity.Users()
	} else if signedIn {
		users = []UserListing{{Name: current.Name, Roles: current.Roles}}
	}
	rows := make([][]interface{}, 0, len(users))
	for _, user := range users {
		if currentOnly && (!signedIn || user.Name != current.Name) {
			continue
		}
		roles := append(make([]string, 0, len(user.Roles)), user.Roles...)
		sort.Strings(roles)
		rows = append(rows, []interface{}{user.Name, roles, user.PasswordChangeRequired, user.Suspended, nil})
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i][0].(string) < rows[j][0].(string) })
	return &ExecuteResult{Columns: append([]string(nil), showUsersColumns...), Rows: rows}, nil
}

// unsupportedAdministrationCommandError is Neo4j Community's error for an
// administration command it doesn't offer (SHOW ROLES, SHOW PRIVILEGES,
// SHOW USER … PRIVILEGES); SHOW SERVERS is NotSystemDatabaseError.
func unsupportedAdministrationCommandError(cypher string) error {
	code := "Neo.ClientError.Statement.UnsupportedAdministrationCommand"
	if findMultiWordKeywordIndex(cypher, "SHOW", "SERVERS") == 0 || findMultiWordKeywordIndex(cypher, "SHOW", "SERVER") == 0 {
		code = "Neo.ClientError.Statement.NotSystemDatabaseError"
	}
	return newSemanticError(code, "UnsupportedAdministrationCommand", "Unsupported administration command: "+strings.TrimSpace(showCommandHead(cypher)))
}

// runningTransaction is one SHOW TRANSACTIONS row: an explicit transaction
// from BEGIN to COMMIT / ROLLBACK, or an auto-commit statement while it
// runs. TERMINATE TRANSACTIONS marks it terminated and cancels its running
// statement; an explicit transaction's next statement then fails with
// Neo.ClientError.Transaction.Terminated.
//
// Ids are numbers here; the "<database>-transaction-<n>" and "query-<n>"
// strings are built only when SHOW or TERMINATE TRANSACTIONS reads them, so
// a statement registers without formatting any.
type runningTransaction struct {
	number     uint64
	database   string
	username   string
	connection ClientConnection
	started    time.Time
	terminated atomic.Bool

	mu           sync.Mutex
	query        string
	queryNumber  uint64 // 0 while no statement runs
	queryStarted time.Time
	cancel       context.CancelFunc
}

// id is the transaction's Neo4j id, <database>-transaction-<n>.
func (tx *runningTransaction) id() string {
	return tx.database + "-transaction-" + strconv.FormatUint(tx.number, 10)
}

type runningTransactionRegistry struct {
	mu       sync.RWMutex
	byNumber map[uint64]*runningTransaction
	nextTx   atomic.Uint64
	nextQry  atomic.Uint64
}

// runningTransactions is the process's registry: SHOW TRANSACTIONS lists
// the transactions of every session and protocol, as in Neo4j.
var runningTransactions = &runningTransactionRegistry{byNumber: make(map[uint64]*runningTransaction)}

func (r *runningTransactionRegistry) begin(ctx context.Context, database string) *runningTransaction {
	tx := &runningTransaction{}
	r.register(ctx, tx, database)
	return tx
}

// register fills in tx - a new transaction on database, run by ctx's
// connection and user - and lists it.
func (r *runningTransactionRegistry) register(ctx context.Context, tx *runningTransaction, database string) {
	if database == "" {
		database = "nornic"
	}
	tx.number = r.nextTx.Add(1)
	tx.database = database
	tx.connection = clientConnectionFromContext(ctx)
	tx.started = time.Now()
	if user, ok := authenticatedUserFromContext(ctx); ok {
		tx.username = user.Name
	}
	r.mu.Lock()
	r.byNumber[tx.number] = tx
	r.mu.Unlock()
}

func (r *runningTransactionRegistry) end(tx *runningTransaction) {
	if tx == nil {
		return
	}
	r.mu.Lock()
	delete(r.byNumber, tx.number)
	r.mu.Unlock()
}

// startQuery records the statement tx runs; cancel stops it on TERMINATE.
func (tx *runningTransaction) startQuery(query string, cancel context.CancelFunc) {
	tx.mu.Lock()
	tx.query = query
	tx.queryNumber = runningTransactions.nextQry.Add(1)
	tx.queryStarted = time.Now()
	tx.cancel = cancel
	tx.mu.Unlock()
	if tx.terminated.Load() && cancel != nil {
		cancel()
	}
}

func (tx *runningTransaction) endQuery() {
	tx.mu.Lock()
	tx.query, tx.queryNumber, tx.cancel = "", 0, nil
	tx.mu.Unlock()
}

func (r *runningTransactionRegistry) snapshot() []*runningTransaction {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*runningTransaction, 0, len(r.byNumber))
	for _, tx := range r.byNumber {
		out = append(out, tx)
	}
	return out
}

// terminate terminates the transaction with id, a checked
// <database>-transaction-<n> (terminateTransactionID).
func (r *runningTransactionRegistry) terminate(id string) (*runningTransaction, bool) {
	separator := strings.LastIndex(id, "-transaction-")
	if separator < 0 {
		return nil, false
	}
	number, err := strconv.ParseUint(id[separator+len("-transaction-"):], 10, 64)
	if err != nil {
		return nil, false
	}
	r.mu.RLock()
	tx, ok := r.byNumber[number]
	r.mu.RUnlock()
	if !ok || !strings.EqualFold(tx.database, id[:separator]) {
		return nil, false
	}
	tx.terminated.Store(true)
	tx.mu.Lock()
	cancel := tx.cancel
	tx.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return tx, true
}

// transactionTerminatedError is Neo4j's error for a statement of a
// terminated transaction.
func transactionTerminatedError() error {
	return newSemanticError("Neo.ClientError.Transaction.Terminated", "Terminated",
		"The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result.")
}

// runningStatement is a registered statement: done ends it (and the
// auto-commit transaction it is). The zero value's done does nothing.
type runningStatement struct {
	tx         *runningTransaction
	cancel     context.CancelFunc
	autoCommit bool
}

func (s runningStatement) done() {
	if s.tx == nil {
		return
	}
	if s.autoCommit {
		runningTransactions.end(s.tx)
	} else {
		s.tx.endQuery()
	}
	s.cancel()
}

// statementContext is a running statement's context: TERMINATE cancels it,
// and its value for ctxKeyRunningTransaction is the statement's
// transaction. An auto-commit statement's transaction lives in the same
// allocation (autoCommitStatementContext), so registering a statement costs
// the cancellable context and this one value.
type statementContext struct {
	context.Context
	tx *runningTransaction
}

func (c *statementContext) Value(key any) any {
	if _, ok := key.(ctxKeyRunningTransaction); ok {
		return c.tx
	}
	return c.Context.Value(key)
}

type autoCommitStatementContext struct {
	statementContext
	transaction runningTransaction
}

// withRunningStatement registers the statement ctx runs: in the explicit
// transaction when one is open, otherwise as its own auto-commit
// transaction. The returned context is cancelled by TERMINATE; done
// unregisters. A statement nested in another (subquery re-execution) is
// part of the outer one.
func (e *StorageExecutor) withRunningStatement(ctx context.Context, query string) (context.Context, runningStatement, error) {
	if ctx.Value(ctxKeyRunningTransaction{}) != nil {
		return ctx, runningStatement{}, nil
	}
	if e.txContext != nil && e.txContext.active && e.txContext.running != nil {
		tx := e.txContext.running
		if tx.terminated.Load() {
			return ctx, runningStatement{}, transactionTerminatedError()
		}
		cancelCtx, cancel := context.WithCancel(ctx)
		tx.startQuery(query, cancel)
		return &statementContext{Context: cancelCtx, tx: tx}, runningStatement{tx: tx, cancel: cancel}, nil
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	statement := &autoCommitStatementContext{statementContext: statementContext{Context: cancelCtx}}
	tx := &statement.transaction
	statement.tx = tx
	runningTransactions.register(ctx, tx, e.currentDatabaseName())
	tx.startQuery(query, cancel)
	return statement, runningStatement{tx: tx, cancel: cancel, autoCommit: true}, nil
}

// showTransactionsColumns is SHOW TRANSACTIONS' full column set in Neo4j
// 5.26's order; showTransactionsDefaultColumns its default set.
var (
	showTransactionsColumns        = []string{"database", "transactionId", "currentQueryId", "outerTransactionId", "connectionId", "clientAddress", "username", "metaData", "currentQuery", "parameters", "planner", "runtime", "indexes", "startTime", "currentQueryStartTime", "protocol", "requestUri", "status", "currentQueryStatus", "statusDetails", "resourceInformation", "activeLockCount", "currentQueryActiveLockCount", "elapsedTime", "cpuTime", "waitTime", "idleTime", "currentQueryElapsedTime", "currentQueryCpuTime", "currentQueryWaitTime", "currentQueryIdleTime", "currentQueryAllocatedBytes", "allocatedDirectBytes", "estimatedUsedHeapMemory", "pageHits", "pageFaults", "currentQueryPageHits", "currentQueryPageFaults", "initializationStackTrace"}
	showTransactionsDefaultColumns = []string{"database", "transactionId", "currentQueryId", "connectionId", "clientAddress", "username", "currentQuery", "startTime", "status", "elapsedTime"}
)

// transactionIDFilter reads the transaction ids after SHOW / TERMINATE
// TRANSACTION[S]: a string, a list of strings, or a parameter holding
// either. ok is false when there is no id expression.
func (e *StorageExecutor) transactionIDFilter(ctx context.Context, head, command string) ([]string, bool, error) {
	rest := strings.TrimSpace(head)
	if startsWithKeywordFold(rest, command) {
		rest = strings.TrimSpace(rest[len(command):])
	}
	for _, noun := range []string{"TRANSACTIONS", "TRANSACTION"} {
		if startsWithKeywordFold(rest, noun) {
			rest = strings.TrimSpace(rest[len(noun):])
			break
		}
	}
	if rest == "" {
		return nil, false, nil
	}
	value, ok := e.evaluateRowExpressionWithContext(ctx, rest, e.parameterRow(ctx))
	if !ok {
		return nil, true, newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidTransactionID",
			"expected a transaction id or a list of transaction ids, got: "+rest)
	}
	switch typed := value.(type) {
	case string:
		return []string{typed}, true, nil
	case nil:
		return []string{}, true, nil
	}
	if items, isList := cypherListValue(value); isList {
		ids := make([]string, 0, len(items))
		for _, item := range items {
			id, isString := item.(string)
			if !isString {
				return nil, true, typeMismatchError("String or List<String>", value)
			}
			ids = append(ids, id)
		}
		return ids, true, nil
	}
	return nil, true, typeMismatchError("String or List<String>", value)
}

// parameterRow is a row holding the statement's parameters as $name.
func (e *StorageExecutor) parameterRow(ctx context.Context) pipelineRow {
	params := getParamsFromContext(ctx)
	row := make(pipelineRow, len(params))
	for name, value := range params {
		row["$"+name] = value
	}
	return row
}

// executeShowTransactions lists the running transactions.
func (e *StorageExecutor) executeShowTransactions(ctx context.Context, cypher string) (*ExecuteResult, error) {
	ids, filtered, err := e.transactionIDFilter(ctx, cypher, "SHOW")
	if err != nil {
		return nil, err
	}
	wanted := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		wanted[id] = struct{}{}
	}
	now := time.Now()
	transactions := runningTransactions.snapshot()
	sort.Slice(transactions, func(i, j int) bool { return transactions[i].started.Before(transactions[j].started) })
	rows := make([][]interface{}, 0, len(transactions))
	for _, tx := range transactions {
		id := tx.id()
		if _, ok := wanted[id]; filtered && !ok {
			continue
		}
		tx.mu.Lock()
		query, queryNumber, queryStarted := tx.query, tx.queryNumber, tx.queryStarted
		tx.mu.Unlock()
		status := "Running"
		if tx.terminated.Load() {
			status = "Terminated with reason: Transaction terminated."
		}
		var currentQueryID, currentQueryStart, currentQueryElapsed interface{}
		currentQueryStatus := "idle"
		if queryNumber != 0 {
			currentQueryID = "query-" + strconv.FormatUint(queryNumber, 10)
			currentQueryStart = queryStarted.UTC().Format("2006-01-02T15:04:05.000Z")
			currentQueryElapsed = durationFromGo(now.Sub(queryStarted))
			currentQueryStatus = "running"
		} else {
			currentQueryID = ""
		}
		row := make([]interface{}, len(showTransactionsColumns))
		for i, column := range showTransactionsColumns {
			switch column {
			case "database":
				row[i] = tx.database
			case "transactionId":
				row[i] = id
			case "currentQueryId":
				row[i] = currentQueryID
			case "outerTransactionId":
				row[i] = ""
			case "connectionId":
				row[i] = tx.connection.ID
			case "clientAddress":
				row[i] = tx.connection.Address
			case "username":
				row[i] = tx.username
			case "metaData":
				row[i] = map[string]interface{}{}
			case "currentQuery":
				row[i] = query
			case "startTime":
				row[i] = tx.started.UTC().Format("2006-01-02T15:04:05.000Z")
			case "currentQueryStartTime":
				row[i] = currentQueryStart
			case "protocol":
				row[i] = tx.connection.Protocol
			case "status":
				row[i] = status
			case "currentQueryStatus":
				row[i] = currentQueryStatus
			case "statusDetails":
				row[i] = ""
			case "elapsedTime":
				row[i] = durationFromGo(now.Sub(tx.started))
			case "currentQueryElapsedTime":
				row[i] = currentQueryElapsed
			}
		}
		rows = append(rows, row)
	}
	return withShowDefaultColumns(&ExecuteResult{Columns: append([]string(nil), showTransactionsColumns...), Rows: rows}, showTransactionsDefaultColumns), nil
}

// executeTerminateTransactions terminates the named transactions: one row
// per id with its username and "Transaction terminated." or "Transaction
// not found.", as in Neo4j.
func (e *StorageExecutor) executeTerminateTransactions(ctx context.Context, cypher string) (*ExecuteResult, error) {
	ids, filtered, err := e.transactionIDFilter(ctx, cypher, "TERMINATE")
	if err != nil {
		return nil, err
	}
	if !filtered {
		return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidTransactionID",
			"TERMINATE TRANSACTIONS requires a transaction id or a list of transaction ids")
	}
	for index, id := range ids {
		normalized, err := terminateTransactionID(id)
		if err != nil {
			return nil, err
		}
		ids[index] = normalized
	}
	rows := make([][]interface{}, 0, len(ids))
	for _, id := range ids {
		tx, found := runningTransactions.terminate(id)
		if !found {
			rows = append(rows, []interface{}{id, nil, "Transaction not found."})
			continue
		}
		rows = append(rows, []interface{}{id, tx.username, "Transaction terminated."})
	}
	return &ExecuteResult{Columns: []string{"transactionId", "username", "message"}, Rows: rows}, nil
}

// terminateTransactionID checks a TERMINATE TRANSACTIONS id as Neo4j does:
// it must read <databasename>-transaction-<number>, with a database name of
// 3 to 63 characters, and the name is compared lower-cased. SHOW
// TRANSACTIONS doesn't check its ids.
func terminateTransactionID(id string) (string, error) {
	separator := strings.LastIndex(id, "-transaction-")
	if separator < 0 {
		return "", invalidTransactionIDError("Could not parse id (expected format: <databasename>-transaction-<id>)")
	}
	database, number := id[:separator], id[separator+len("-transaction-"):]
	if _, err := strconv.ParseUint(number, 10, 64); err != nil {
		return "", invalidTransactionIDError("Could not parse id (expected format: <databasename>-transaction-<id>)")
	}
	if length := len(database); length < 3 || length > 63 {
		return "", invalidTransactionIDError("The provided database name must have a length between 3 and 63 characters.")
	}
	return strings.ToLower(database) + "-transaction-" + number, nil
}

// invalidTransactionIDError is Neo4j's error for a malformed transaction id.
func invalidTransactionIDError(message string) error {
	return newSemanticError("Neo.ClientError.General.InvalidArguments", "InvalidTransactionID", message)
}

// durationFromGo is a Cypher duration of d.
func durationFromGo(d time.Duration) *CypherDuration {
	seconds := int64(d / time.Second)
	return &CypherDuration{Seconds: seconds, Nanos: int64(d % time.Second)}
}
