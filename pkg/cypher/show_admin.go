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

type ctxKeyAuthenticatedUser struct{}
type ctxKeyUserDirectory struct{}
type ctxKeyClientConnection struct{}
type ctxKeyRunningTransaction struct{}

// AuthenticatedUser is the signed-in user of a request: SHOW CURRENT USER
// lists it, and SHOW TRANSACTIONS reports its name as the username.
type AuthenticatedUser struct {
	Name  string
	Roles []string
}

// WithAuthenticatedUser attaches the request's signed-in user.
func WithAuthenticatedUser(ctx context.Context, user AuthenticatedUser) context.Context {
	if strings.TrimSpace(user.Name) == "" {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyAuthenticatedUser{}, user)
}

func authenticatedUserFromContext(ctx context.Context) (AuthenticatedUser, bool) {
	user, ok := ctx.Value(ctxKeyAuthenticatedUser{}).(AuthenticatedUser)
	return user, ok
}

// UserListing is one SHOW USERS row.
type UserListing struct {
	Name                   string
	Roles                  []string
	PasswordChangeRequired bool
	Suspended              bool
}

// WithUserDirectory attaches the user store SHOW USERS lists. Servers attach
// it next to the authenticated user; without it SHOW USERS lists the
// signed-in user only.
func WithUserDirectory(ctx context.Context, directory func() []UserListing) context.Context {
	if directory == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyUserDirectory{}, directory)
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

// WithClientConnection attaches the request's connection.
func WithClientConnection(ctx context.Context, connection ClientConnection) context.Context {
	return context.WithValue(ctx, ctxKeyClientConnection{}, connection)
}

func clientConnectionFromContext(ctx context.Context) ClientConnection {
	connection, _ := ctx.Value(ctxKeyClientConnection{}).(ClientConnection)
	return connection
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
	if directory, ok := ctx.Value(ctxKeyUserDirectory{}).(func() []UserListing); ok {
		users = directory()
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
type runningTransaction struct {
	id         string
	database   string
	username   string
	connection ClientConnection
	started    time.Time
	terminated atomic.Bool

	mu           sync.Mutex
	query        string
	queryID      string
	queryStarted time.Time
	cancel       context.CancelFunc
}

type runningTransactionRegistry struct {
	mu      sync.RWMutex
	byID    map[string]*runningTransaction
	nextTx  atomic.Uint64
	nextQry atomic.Uint64
}

// runningTransactions is the process's registry: SHOW TRANSACTIONS lists
// the transactions of every session and protocol, as in Neo4j.
var runningTransactions = &runningTransactionRegistry{byID: make(map[string]*runningTransaction)}

func (r *runningTransactionRegistry) begin(ctx context.Context, database string) *runningTransaction {
	if database == "" {
		database = "nornic"
	}
	tx := &runningTransaction{
		id:         database + "-transaction-" + strconv.FormatUint(r.nextTx.Add(1), 10),
		database:   database,
		connection: clientConnectionFromContext(ctx),
		started:    time.Now(),
	}
	if user, ok := authenticatedUserFromContext(ctx); ok {
		tx.username = user.Name
	}
	r.mu.Lock()
	r.byID[tx.id] = tx
	r.mu.Unlock()
	return tx
}

func (r *runningTransactionRegistry) end(tx *runningTransaction) {
	if tx == nil {
		return
	}
	r.mu.Lock()
	delete(r.byID, tx.id)
	r.mu.Unlock()
}

// startQuery records the statement tx runs; cancel stops it on TERMINATE.
func (tx *runningTransaction) startQuery(query string, cancel context.CancelFunc) {
	tx.mu.Lock()
	tx.query = query
	tx.queryID = "query-" + strconv.FormatUint(runningTransactions.nextQry.Add(1), 10)
	tx.queryStarted = time.Now()
	tx.cancel = cancel
	tx.mu.Unlock()
	if tx.terminated.Load() && cancel != nil {
		cancel()
	}
}

func (tx *runningTransaction) endQuery() {
	tx.mu.Lock()
	tx.query, tx.queryID, tx.cancel = "", "", nil
	tx.mu.Unlock()
}

func (r *runningTransactionRegistry) snapshot() []*runningTransaction {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*runningTransaction, 0, len(r.byID))
	for _, tx := range r.byID {
		out = append(out, tx)
	}
	return out
}

func (r *runningTransactionRegistry) terminate(id string) (*runningTransaction, bool) {
	r.mu.RLock()
	tx, ok := r.byID[id]
	r.mu.RUnlock()
	if !ok {
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

// withRunningStatement registers the statement ctx runs: in the explicit
// transaction when one is open, otherwise as its own auto-commit
// transaction. The returned context is cancelled by TERMINATE; done
// unregisters. A statement nested in another (subquery re-execution) is
// part of the outer one.
func (e *StorageExecutor) withRunningStatement(ctx context.Context, query string) (context.Context, func(), error) {
	if ctx.Value(ctxKeyRunningTransaction{}) != nil {
		return ctx, func() {}, nil
	}
	if e.txContext != nil && e.txContext.active && e.txContext.running != nil {
		tx := e.txContext.running
		if tx.terminated.Load() {
			return ctx, func() {}, transactionTerminatedError()
		}
		statementCtx, cancel := context.WithCancel(context.WithValue(ctx, ctxKeyRunningTransaction{}, tx))
		tx.startQuery(query, cancel)
		return statementCtx, func() {
			tx.endQuery()
			cancel()
		}, nil
	}
	tx := runningTransactions.begin(ctx, e.currentDatabaseName())
	statementCtx, cancel := context.WithCancel(context.WithValue(ctx, ctxKeyRunningTransaction{}, tx))
	tx.startQuery(query, cancel)
	return statementCtx, func() {
		runningTransactions.end(tx)
		cancel()
	}, nil
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
		if _, ok := wanted[tx.id]; filtered && !ok {
			continue
		}
		tx.mu.Lock()
		query, queryID, queryStarted := tx.query, tx.queryID, tx.queryStarted
		tx.mu.Unlock()
		status := "Running"
		if tx.terminated.Load() {
			status = "Terminated with reason: Transaction terminated."
		}
		var currentQueryID, currentQueryStart, currentQueryElapsed interface{}
		currentQueryStatus := "idle"
		if queryID != "" {
			currentQueryID = queryID
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
				row[i] = tx.id
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
