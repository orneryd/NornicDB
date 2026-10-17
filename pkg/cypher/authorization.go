package cypher

import (
	"context"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// PermissionRequirements describes the entitlements required to execute a query.
// Procedure requirements come from the registered procedure contract, while query
// requirements preserve the established top-level Cypher classification.
type PermissionRequirements struct {
	// Unclassified refuses an operation whose registration has no valid contract.
	Unclassified bool
	Read         bool
	Write        bool
	Schema       bool
	Admin        bool
	// BoostedProcedures name the procedures the query calls when they alone
	// make the query need administrator rights: a role holding EXECUTE
	// BOOSTED PROCEDURE for every one of them may run the query without
	// them.
	BoostedProcedures []string
	// UpdatesDBMS is set when the query calls a procedure that changes the
	// DBMS rather than a database (ProcedureSpec.UpdatesDBMS), which no
	// statement may do while impersonating a user.
	UpdatesDBMS bool
	// SchemaAction is Neo4j's action for a statement needing schema
	// ("create_index", "drop_index", "create_constraint",
	// "drop_constraint"), named by its refusal.
	SchemaAction string
	// UnrestrictedView is set when the query calls a callable that reads
	// around a principal's graph view (PrivilegeUnrestrictedView), which a
	// principal restricted on the statement's database may not;
	// UnrestrictedViewCallable names it for the refusal.
	UnrestrictedView         bool
	UnrestrictedViewCallable string
}

// AdminGrantedBy reports whether a role's procedure grants satisfy the
// query's administrator requirement: it needs admin only for the procedures
// it calls, and allowsBoosted grants every one of them boosted.
func (r PermissionRequirements) AdminGrantedBy(allowsBoosted func(procedure string) bool) bool {
	if !r.Admin || len(r.BoostedProcedures) == 0 || allowsBoosted == nil {
		return false
	}
	for _, procedure := range r.BoostedProcedures {
		if !allowsBoosted(procedure) {
			return false
		}
	}
	return true
}

// QueryPermissionRequirements returns the permissions required by a query and
// a registered procedure invoked by that query, when present.
func QueryPermissionRequirements(query string) PermissionRequirements {
	query = classificationText(query)
	class, command := administrationCommandClass(query)
	requirements := PermissionRequirements{}
	if command {
		requirements = requirementsForClass(class)
	} else {
		keywords := queryKeywords(query)
		requirements.Read = statementReadsGraphText(query) || keywords["LOAD"]
		requirements.Write = keywords["CREATE"] || keywords["DELETE"] || keywords["SET"] ||
			keywords["MERGE"] || keywords["REMOVE"]
	}
	if requirements.Schema {
		requirements.SchemaAction = schemaCommandAction(query)
	}
	// Callable effects compose with the enclosing command/data operation.
	adminCommand := requirements.Admin
	for _, procedure := range registeredProceduresForCalls(query) {
		contract := requirementsForClass(procedure.Privilege)
		requirements.Unclassified = requirements.Unclassified || contract.Unclassified
		requirements.Read = requirements.Read || contract.Read
		requirements.Write = requirements.Write || contract.Write
		requirements.Schema = requirements.Schema || contract.Schema
		requirements.UpdatesDBMS = requirements.UpdatesDBMS || contract.UpdatesDBMS
		if contract.UnrestrictedView && !requirements.UnrestrictedView {
			requirements.UnrestrictedView, requirements.UnrestrictedViewCallable = true, procedure.Name
		}
		if contract.Schema && requirements.SchemaAction == "" {
			requirements.SchemaAction = schemaProcedureAction(procedure.Name)
		}
		if contract.Admin {
			if !adminCommand {
				requirements.BoostedProcedures = append(requirements.BoostedProcedures, procedure.Name)
			}
			requirements.Admin = true
		}
	}
	_ = forEachFunctionCall(query, func(name, _ string) error {
		if class, found := functionPrivilegeClass(name); found {
			contract := requirementsForClass(class)
			requirements.Unclassified = requirements.Unclassified || contract.Unclassified
			requirements.Read = requirements.Read || contract.Read
			requirements.Write = requirements.Write || contract.Write
			requirements.Schema = requirements.Schema || contract.Schema
			requirements.Admin = requirements.Admin || contract.Admin
			requirements.UpdatesDBMS = requirements.UpdatesDBMS || contract.UpdatesDBMS
			if contract.UnrestrictedView && !requirements.UnrestrictedView {
				requirements.UnrestrictedView, requirements.UnrestrictedViewCallable = true, name
			}
			if contract.Admin {
				// Procedure boosting cannot discharge an independent function
				// requirement in the same statement.
				requirements.BoostedProcedures = nil
			}
		}
		return nil
	})
	return requirements
}

// registeredProceduresForCalls returns the registered procedures named by
// every CALL clause of query, wherever it stands (after another CALL's
// YIELD, in a CALL { } / EXISTS { } / COUNT { } subquery, ...). CALL inside
// string literals and comments is not a clause. Backticked name parts are
// unquoted, as the executor resolves them.
func registeredProceduresForCalls(query string) []ProcedureSpec {
	ensureBuiltInProceduresRegistered()
	opts := defaultKeywordScanOpts()
	opts.SkipParens = false
	opts.SkipBrackets = false
	opts.SkipBraces = false
	var procedures []ProcedureSpec
	for from := 0; from < len(query); {
		at := keywordIndexFrom(query, "CALL", from, opts)
		if at < 0 {
			break
		}
		from = at + len("CALL")
		name, ok := procedureNameAt(query, from)
		if !ok {
			continue // CALL { ... } or CALL (vars) { ... }: its body is scanned on
		}
		if procedure, found := globalProcedureRegistry.Get(name); found {
			procedures = append(procedures, procedure.Spec)
			continue
		}
		// Execution resolves exact names. Preserve conservative authorization
		// for an invalid case variant without making it executable: every
		// matching contract counts, since case-distinct registrations can
		// require different permissions. An exact match above remains decisive.
		for _, procedure := range globalProcedureRegistry.List() {
			if strings.EqualFold(name, procedure.Name) {
				procedures = append(procedures, procedure)
			}
		}
	}
	return procedures
}

// procedureNameAt parses the dotted procedure name after a CALL keyword
// ending at offset: segments of identifier characters or backticked text,
// separated by dots, with whitespace allowed around them.
func procedureNameAt(query string, offset int) (string, bool) {
	i := offset
	skipSpace := func() {
		for i < len(query) && isASCIISpace(query[i]) {
			i++
		}
	}
	// Keep the ordinary dotted name as a source slice. Only quoted parts
	// or spaces around separators need normalization, shared by auth and CALL.
	var normalized strings.Builder
	normalizing := false
	startName, endName, components := offset, offset, 0
	for {
		skipSpace()
		if i >= len(query) {
			break
		}
		partStart := i
		quoted := query[i] == '`'
		var part string
		if quoted {
			end := strings.IndexByte(query[i+1:], '`')
			if end < 0 {
				return "", false
			}
			part = query[i+1 : i+1+end]
			i += end + 2
		} else {
			for i < len(query) && (query[i] == '_' || query[i] == '$' || query[i] >= '0' && query[i] <= '9' || query[i] >= 'a' && query[i] <= 'z' || query[i] >= 'A' && query[i] <= 'Z') {
				i++
			}
			if i == partStart {
				break
			}
			part = query[partStart:i]
		}
		if components == 0 {
			startName = partStart
		}
		if !normalizing && (quoted || components > 0 && partStart != endName+1) {
			if components > 0 {
				normalized.WriteString(query[startName:endName])
			}
			normalizing = true
		}
		if normalizing {
			if components > 0 {
				normalized.WriteByte('.')
			}
			normalized.WriteString(part)
		}
		endName = i
		components++
		skipSpace()
		if i < len(query) && query[i] == '.' {
			i++
			continue
		}
		break
	}
	if components == 0 {
		return "", false
	}
	if normalizing {
		return normalized.String(), true
	}
	return query[startName:endName], true
}

func isSchemaPermissionQuery(query string) bool {
	class, found := administrationCommandClass(query)
	return found && requirementsForClass(class).Schema
}

// isTerminateTransactionsQuery reports TERMINATE TRANSACTION[S] as the
// statement's command.
func isTerminateTransactionsQuery(query string) bool {
	command := query[firstExecutableCypherOffset(query):]
	return startsWithKeywords(command, "TERMINATE", "TRANSACTIONS") || startsWithKeywords(command, "TERMINATE", "TRANSACTION")
}

func isAdminPermissionQuery(query string) bool {
	class, found := administrationCommandClass(query)
	return found && requirementsForClass(class).Admin
}

func isUpdatingAdministrationQuery(query string) bool {
	class, found := administrationCommandClass(query)
	return found && requirementsForClass(class).UpdatesDBMS
}

// isCreateOrReplaceDatabaseQuery reports whether query is
// CREATE OR REPLACE DATABASE <name>.
func isCreateOrReplaceDatabaseQuery(query string) bool {
	commandOffset := firstExecutableCypherOffset(query)
	opts := defaultKeywordScanOpts()
	create := keywordIndexFrom(query, "CREATE", commandOffset, opts)
	if create != commandOffset {
		return false
	}
	orReplace := keywordIndexFrom(query, "OR REPLACE", create+len("CREATE"), opts)
	if orReplace < 0 {
		return false
	}
	database := keywordIndexFrom(query, "DATABASE", orReplace+len("OR REPLACE"), opts)
	return database >= 0
}

func firstExecutableCypherOffset(query string) int {
	index := 0
	if strings.HasPrefix(query, "\xef\xbb\xbf") {
		index = 3
	}
	for {
		for index < len(query) && isASCIISpace(query[index]) {
			index++
		}
		switch {
		case index+1 < len(query) && query[index] == '/' && query[index+1] == '/':
			index += 2
			for index < len(query) && query[index] != '\n' {
				index++
			}
		case index+1 < len(query) && query[index] == '/' && query[index+1] == '*':
			index += 2
			for index+1 < len(query) && (query[index] != '*' || query[index+1] != '/') {
				index++
			}
			if index+1 >= len(query) {
				return len(query)
			}
			index += 2
		default:
			return index
		}
	}
}

func queryKeywords(query string) map[string]bool {
	keywords := make(map[string]bool)
	for index := 0; index < len(query); {
		switch query[index] {
		case '/', '\'', '"', '`':
			if query[index] == '/' && index+1 < len(query) && query[index+1] == '/' {
				index += 2
				for index < len(query) && query[index] != '\n' {
					index++
				}
				continue
			}
			if query[index] == '/' && index+1 < len(query) && query[index+1] == '*' {
				index += 2
				for index+1 < len(query) && (query[index] != '*' || query[index+1] != '/') {
					index++
				}
				if index+1 < len(query) {
					index += 2
				}
				continue
			}
			if query[index] != '/' {
				quote := query[index]
				index++
				for index < len(query) {
					if query[index] == '\\' && quote != '`' && index+1 < len(query) {
						index += 2
						continue
					}
					if query[index] == quote {
						if index+1 < len(query) && query[index+1] == quote {
							index += 2
							continue
						}
						index++
						break
					}
					index++
				}
				continue
			}
		}

		if !isIdentStartByte(query[index]) {
			index++
			continue
		}
		start := index
		index++
		for index < len(query) && isIdentByte(query[index]) {
			index++
		}
		if isQualifiedOrMapKey(query, start, index) {
			continue
		}
		keywords[upperASCII(query[start:index])] = true
	}
	return keywords
}

func isQualifiedOrMapKey(query string, start, end int) bool {
	left := start - 1
	for left >= 0 && isASCIISpace(query[left]) {
		left--
	}
	if left >= 0 && (query[left] == '.' || query[left] == ':' || query[left] == '$') {
		return true
	}
	right := end
	for right < len(query) && isASCIISpace(query[right]) {
		right++
	}
	return right < len(query) && query[right] == ':'
}

// PermissionChecker answers whether the caller holds an entitlement.
type PermissionChecker func(permission string) bool

type permissionCheckerKey struct{}

// DatabasePermissionResolver answers whether the caller holds an entitlement
// for the database where a statement is executing. "access" checks selection
// of a containing composite or alias; data permissions apply to its target.
type DatabasePermissionResolver func(database, permission string) bool

type databaseAuthorization struct {
	database string
	resolver DatabasePermissionResolver
	visible  func(database string) (*GraphVisibility, error)
}

type databaseAuthorizationKey struct{}

// WithPermissionChecker attaches a caller's effective entitlements to an
// execution context. Nested dynamic statements inherit the same checker.
func WithPermissionChecker(ctx context.Context, checker PermissionChecker) context.Context {
	if checker == nil {
		return ctx
	}
	return context.WithValue(ctx, permissionCheckerKey{}, checker)
}

// WithDatabasePermissionResolver attaches database-scoped entitlements to an
// execution context. USE routing updates the selected database automatically.
func WithDatabasePermissionResolver(ctx context.Context, database string, resolver DatabasePermissionResolver) context.Context {
	if resolver == nil {
		return ctx
	}
	// A permission-only attachment keeps an already attached principal's
	// graph visibility (WithPrincipalAccess).
	previous, _ := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization)
	return context.WithValue(ctx, databaseAuthorizationKey{}, databaseAuthorization{
		database: database,
		resolver: resolver,
		visible:  previous.visible,
	})
}

func withExecutionDatabase(ctx context.Context, database string) context.Context {
	ctx = context.WithValue(ctx, ctxKeyUseDatabase, database)
	authorization, ok := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization)
	if !ok {
		return ctx
	}
	authorization.database = database
	return context.WithValue(ctx, databaseAuthorizationKey{}, authorization)
}

// AccessDatabases returns the databases a principal must be allowed to use
// for a statement on database, the graph name it selects (a request or
// session database, a USE, :USE or subquery USE, a composite constituent):
//   - a composite constituent "composite.alias": the composite and the
//     constituent's database (the full name when the composite has no such
//     alias);
//   - any other name: the name and, when it is an alias, the database it
//     names.
//
// The executor checks each one (authorizeSelectedDatabase), and Bolt and
// HTTP check a request's or session's database with the same list, so a
// graph is allowed or refused the same way however a statement selects it.
// manager may be nil (no database manager): the name alone. When the
// composite's constituents can't be read, the list is the composite and the
// error is returned with it.
func AccessDatabases(manager DatabaseManagerInterface, database string) ([]string, error) {
	if manager == nil {
		return []string{database}, nil
	}
	if dot := strings.IndexByte(database, '.'); dot > 0 && manager.IsCompositeDatabase(database[:dot]) {
		composite := database[:dot]
		constituents, err := manager.GetCompositeConstituents(composite)
		if err != nil {
			return []string{composite}, err
		}
		for _, item := range constituents {
			if ref, ok := toConstituentRef(item); ok && strings.EqualFold(ref.Alias, database[dot+1:]) {
				return []string{composite, ref.DatabaseName}, nil
			}
		}
		return []string{composite, database}, nil
	}
	if resolved, err := manager.ResolveDatabase(database); err == nil && resolved != database {
		return []string{database, resolved}, nil
	}
	return []string{database}, nil
}

// GraphAccess decides a request's or session's graph the way the executor
// decides a graph a statement selects: allowed is whether canAccess allows
// every database AccessDatabases lists for graph (a nil canAccess allows
// all), and dataDatabase is the last one, the database graph's data is in,
// whose privileges apply. When the databases can't be read (a composite's
// constituents), access is refused. Bolt and HTTP use it for the database a
// request names.
func GraphAccess(manager DatabaseManagerInterface, graph string, canAccess func(database string) bool) (dataDatabase string, allowed bool) {
	databases, err := AccessDatabases(manager, graph)
	if err != nil {
		return graph, false
	}
	dataDatabase = databases[len(databases)-1]
	for _, database := range databases {
		if canAccess != nil && !canAccess(database) {
			return dataDatabase, false
		}
	}
	return dataDatabase, true
}

// GraphIsRemote reports whether graph's data is, or may be, on another
// server: a remote composite constituent (composite.alias), or a composite
// database (or an alias of one) with a remote constituent. When a
// composite's constituents can't be read it reports true, so a caller that
// refuses remote graphs fails closed.
func GraphIsRemote(manager DatabaseManagerInterface, graph string) bool {
	if manager == nil {
		return false
	}
	remote := func(item interface{}) bool {
		ref, ok := toConstituentRef(item)
		return !ok || strings.EqualFold(strings.TrimSpace(ref.Type), "remote")
	}
	if dot := strings.IndexByte(graph, '.'); dot > 0 && manager.IsCompositeDatabase(graph[:dot]) {
		constituents, err := manager.GetCompositeConstituents(graph[:dot])
		if err != nil {
			return true
		}
		for _, item := range constituents {
			if ref, ok := toConstituentRef(item); !ok || strings.EqualFold(ref.Alias, graph[dot+1:]) {
				return remote(item)
			}
		}
		return false
	}
	database := graph
	if resolved, err := manager.ResolveDatabase(graph); err == nil {
		database = resolved
	}
	if !manager.IsCompositeDatabase(database) {
		return false
	}
	constituents, err := manager.GetCompositeConstituents(database)
	if err != nil {
		return true
	}
	for _, item := range constituents {
		if remote(item) {
			return true
		}
	}
	return false
}

// authorizeSelectedDatabase owns the selection and permission decision for a
// statement or fragment. Each containing composite or alias needs access;
// the physical target needs the statement's data permissions. An empty name
// selects the request's execution database from the context.
func (e *StorageExecutor) authorizeSelectedDatabase(ctx context.Context, database, query string) error {
	selection := database != ""
	if database == "" {
		if authorization, ok := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization); ok {
			database = authorization.database
		}
		if database == "" {
			database = e.currentDatabaseName()
		}
	}
	return admitSelectedDatabase(ctx, e.dbManager, database, query, selection)
}

// admitGraphImpersonation refuses, while the statement impersonates a user,
// a graph on another server (GraphIsRemote): the remote server would run it
// as the login's forwarded credential or the constituent's stored one,
// never as the impersonated user, and Neo4j doesn't impersonate a user
// through a remote database alias.
func admitGraphImpersonation(ctx context.Context, manager DatabaseManagerInterface, database string) error {
	if impersonating(ctx) && GraphIsRemote(manager, database) {
		return localizedStatusError("Neo.ClientError.Security.Forbidden", "Forbidden", localization.CypherAdminImpersonationRemoteDenied(database))
	}
	return nil
}

func admitSelectedDatabase(ctx context.Context, manager DatabaseManagerInterface, database, query string, selection bool) error {
	if err := admitGraphImpersonation(ctx, manager, database); err != nil {
		return err
	}
	if _, ok := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization); !ok {
		return AuthorizeQuery(ctx, query)
	}
	_, body := parseExecutionMode(query)
	body = body[firstExecutableCypherOffset(body):]
	shellSelection := false
	for commands := body; ; {
		command, rest, ok := nextShellCommand(commands)
		if !ok {
			break
		}
		fields := strings.Fields(command)
		if len(fields) > 0 && strings.EqualFold(fields[0], ":use") {
			shellSelection = true
			break
		}
		commands = rest
	}
	// USE and Fabric shells authorize graph selection here; fragments decide
	// data rights on their own targets before resolving those targets.
	selectionOnly := selection && (shellSelection || startsWithKeywordFold(body, "USE") ||
		manager != nil && manager.IsCompositeDatabase(database) && statementUsesFabric(manager, database, body))
	databases, listErr := AccessDatabases(manager, database)
	for i, selected := range databases {
		if selectionOnly || i < len(databases)-1 && listErr == nil {
			if err := authorizeDatabase(ctx, selected, "access"); err != nil {
				return err
			}
			continue
		}
		if err := AuthorizeQuery(withExecutionDatabase(ctx, selected), query); err != nil {
			return err
		}
	}
	return listErr
}

// PermissionDeniedError identifies the entitlement needed for a query, and
// what its message names in Neo4j's words: the refused principal
// (localization.PrincipalDescription; empty for a bare check, then a user
// with no name and no roles), and for schema the action and database, for
// an admin procedure its name.
type PermissionDeniedError struct {
	Permission string
	// Principal is the refused principal's description.
	Principal string
	// SchemaAction is Neo4j's schema action ("create_index", "drop_index",
	// "create_constraint", "drop_constraint") and Database the database it
	// was refused on.
	SchemaAction string
	Database     string
	// Procedure is the admin procedure refused, when the statement needs
	// admin only for procedures.
	Procedure string
}

// BoltErrorCode is the Neo4j status of a permission failure, on Bolt and
// HTTP alike (nornicerrors.Neo4jStatus).
func (e *PermissionDeniedError) BoltErrorCode() string {
	return "Neo.ClientError.Security.Forbidden"
}

// StatusMessage is the message clients see for a permission failure, in
// Neo4j 5.26's words where Neo4j has one (Error).
func (e *PermissionDeniedError) StatusMessage() string {
	return e.Error()
}

// Error is the refusal in Neo4j 5.26's words:
//   - schema: "Schema operation 'create_index' on database 'neo4j' is not
//     allowed for user 'alice' with roles [reader]."
//     (SecurityAuthorizationHandler.assertSchemaWrites);
//   - admin, for an admin procedure: "Executing admin procedure
//     'dbms.listConfig' permission has not been granted for user ..."
//     (ProcedureRegistry); for an administration command: "Permission
//     denied." (AuthorizationViolationException.PERMISSION_DENIED);
//   - write: "Write operations are not allowed for user ...", Neo4j's
//     static access-mode wording (Neo4j 5 refuses each write operation
//     separately, which a statement-level check can't name);
//   - read keeps NornicDB's message: Neo4j 5 hides what a principal may not
//     read instead of failing.
func (e *PermissionDeniedError) Error() string {
	principal := e.Principal
	if principal == "" {
		principal = localization.PrincipalDescription("", nil)
	}
	switch e.Permission {
	case "schema":
		action := e.SchemaAction
		if action == "" {
			action = "create_index"
		}
		return "Schema operation '" + action + "' on database '" + e.Database + "' is not allowed for " + principal + "."
	case "admin":
		if e.Procedure != "" {
			return "Executing admin procedure '" + e.Procedure + "' permission has not been granted for " + principal + "."
		}
		return "Permission denied."
	case "write":
		return "Write operations are not allowed for " + principal + "."
	default:
		return "Read operations require read permission"
	}
}

// permissionDenied is the refusal of permission for ctx's executing
// principal on the statement's database, with requirements' schema action
// and admin procedure.
func permissionDenied(ctx context.Context, permission string, requirements PermissionRequirements) *PermissionDeniedError {
	user, roles := principalOf(ctx)
	denied := &PermissionDeniedError{Permission: permission, Principal: localization.PrincipalDescription(user, roles)}
	if authorization, ok := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization); ok {
		denied.Database = authorization.database
	}
	switch permission {
	case "schema":
		denied.SchemaAction = requirements.SchemaAction
	case "admin":
		if len(requirements.BoostedProcedures) > 0 {
			denied.Procedure = requirements.BoostedProcedures[0]
		}
	}
	return denied
}

// statementAdmittedKey carries WithStatementAdmitted's executor and callback.
type statementAdmittedKey struct{}

type statementAdmission struct {
	executor *StorageExecutor
	admitted func()
}

// WithStatementAdmitted runs admitted when executor has authorized a
// statement on its own database (the decision Execute and nested execution
// take before running one, authorizeSelectedDatabase) and before that
// statement runs; it can run once per statement. A
// caller that builds an executor for a statement uses it to finish and share
// that executor only once the principal is admitted there. Other executors
// the statement reaches (a USE target, a nested statement) don't run it.
func WithStatementAdmitted(ctx context.Context, executor *StorageExecutor, admitted func()) context.Context {
	if executor == nil || admitted == nil {
		return ctx
	}
	return context.WithValue(ctx, statementAdmittedKey{}, statementAdmission{executor: executor, admitted: admitted})
}

// statementAdmitted runs e's first-admission step (SetFirstAdmission) the
// first time, and the WithStatementAdmitted callback bound to e.
func (e *StorageExecutor) statementAdmitted(ctx context.Context, query string) {
	// Opening/closing a protocol transaction selects storage, but admits no
	// graph statement. Services wait for a real statement's permissions.
	if bareTransactionCommand(query) != "" {
		return
	}
	if first := e.firstAdmission; first != nil {
		first.once.Do(first.run)
	}
	if admission, ok := ctx.Value(statementAdmittedKey{}).(statementAdmission); ok && admission.executor == e {
		admission.admitted()
	}
}

type firstAdmission struct {
	once sync.Once
	run  func()
}

// SetFirstAdmission sets work the executor does once, after it has
// authorized its first statement on its own database and before that
// statement runs: a transport builds a minimal executor for a database
// (storage, nothing shared) and finishes it there (search service, embed
// queue, publishing it as the database's shared executor), so a principal
// whose statement is refused leaves nothing opened, started or cached. Call
// it before the executor runs any statement.
func (e *StorageExecutor) SetFirstAdmission(run func()) {
	if run == nil {
		e.firstAdmission = nil
		return
	}
	e.firstAdmission = &firstAdmission{run: run}
}

// procedureGrantCheckerKey carries the principal's boosted procedure grants
// (WithProcedureGrantChecker).
type procedureGrantCheckerKey struct{}

// WithProcedureGrantChecker attaches allowsBoosted, which reports whether the
// principal's roles hold EXECUTE BOOSTED PROCEDURE for a procedure: a query
// that needs administrator rights only for the procedure it calls is then
// authorized without them (AuthorizeQuery).
func WithProcedureGrantChecker(ctx context.Context, allowsBoosted func(procedure string) bool) context.Context {
	if allowsBoosted == nil {
		return ctx
	}
	return context.WithValue(ctx, procedureGrantCheckerKey{}, allowsBoosted)
}

// AuthorizeQuery enforces requirements when an entitlement checker is present.
// Top-level and nested execution boundaries both call it so dynamically
// supplied procedure statements cannot bypass transport authorization.
func AuthorizeQuery(ctx context.Context, query string) error {
	if authorization, ok := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization); ok {
		if err := AdmitGraph(ctx, authorization.resolver, authorization.database); err != nil {
			return err
		}
	}
	impersonated := impersonating(ctx)
	if impersonated && isUpdatingAdministrationQuery(query) {
		return impersonationSystemUpdateDenied()
	}
	hasPermission := contextPermissions(ctx)
	if hasPermission == nil && !impersonated {
		// Registration has already validated each callable contract. An
		// embedded/auth-disabled caller has no entitlement policy to evaluate.
		return nil
	}
	requirements := QueryPermissionRequirements(query)
	return authorizeRequirements(ctx, requirements)
}

// authorizeRequirements is the shared decision for a statement contract and
// a resolved callable effect. Both use the same executing-principal resolver.
func authorizeRequirements(ctx context.Context, requirements PermissionRequirements) error {
	if requirements.Unclassified {
		return localizedStatusError("Neo.ClientError.Security.Forbidden", "Forbidden", localization.OperationContractRequired())
	}
	if impersonating(ctx) && requirements.UpdatesDBMS {
		return impersonationSystemUpdateDenied()
	}
	if requirements.UnrestrictedView && statementRestricted(ctx) {
		return localizedStatusError("Neo.ClientError.Security.Forbidden", "Forbidden", localization.CypherAdminRestrictedOperation(requirements.UnrestrictedViewCallable))
	}
	hasPermission := contextPermissions(ctx)
	if hasPermission == nil {
		return nil
	}
	allowsBoosted, _ := ctx.Value(procedureGrantCheckerKey{}).(func(string) bool)
	if missing := MissingPermission(requirements, hasPermission, allowsBoosted); missing != "" {
		return permissionDenied(ctx, missing, requirements)
	}
	return nil
}

// impersonationSystemUpdateDenied refuses a change to the DBMS (an updating
// administration command, or a procedure whose contract says it updates the
// DBMS) while impersonating: as in Neo4j, impersonation lends the
// impersonated user's privileges for graph work and SHOW commands, never
// for changing the DBMS, whatever those privileges are.
func impersonationSystemUpdateDenied() error {
	return localizedStatusError("Neo.ClientError.Security.Forbidden", "Forbidden", localization.CypherAdminImpersonationSystemUpdateDenied())
}

// contextPermissions is the principal's permission check the context
// carries (WithDatabasePermissionResolver for the statement's database, else
// WithPermissionChecker), or nil when it carries none (no authorization).
func contextPermissions(ctx context.Context) func(permission string) bool {
	if databaseAuth, ok := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization); ok {
		return func(permission string) bool {
			return databaseAuth.resolver(databaseAuth.database, permission)
		}
	}
	if checker, _ := ctx.Value(permissionCheckerKey{}).(PermissionChecker); checker != nil {
		return checker
	}
	return nil
}

// principalPermissions is the permission check of the statement's principal,
// for what a principal sees rather than what it may run (dbms.listConnections
// lists other users' connections only to a principal holding admin): the
// transport's resolver (contextPermissions); without one (an embedded
// caller), the signed-in user's roles, resolved as HTTP and Bolt resolve an
// entitlement (auth.PermissionsForRoles, auth.EntitlementsAllow) with no
// stored entitlements, which the executor doesn't have, so the built-in
// roles' defaults decide. It is nil when the statement has neither a resolver
// nor a signed-in user: the executor authorizes nothing for that caller.
func principalPermissions(ctx context.Context) func(permission string) bool {
	if allows := contextPermissions(ctx); allows != nil {
		return allows
	}
	user, signedIn := authenticatedUserFromContext(ctx)
	if !signedIn {
		return nil
	}
	entitlements := auth.PermissionsForRoles(user.Roles, nil)
	return func(permission string) bool { return auth.EntitlementsAllow(entitlements, permission) }
}

// MissingPermission is the entitlement decision used by statement execution
// (AuthorizeQuery) and non-Cypher consumers: the first
// permission requirements need that allows denies, in Neo4j's order
// (schema, admin, write, read), or "" when allows grants them all. A query
// that needs admin only for procedures that allowsBoosted grants every one
// of boosted needs no admin (AdminGrantedBy).
func MissingPermission(requirements PermissionRequirements, allows func(permission string) bool, allowsBoosted func(procedure string) bool) string {
	if requirements.Admin && !allows("admin") && requirements.AdminGrantedBy(allowsBoosted) {
		requirements.Admin = false
	}
	for _, permission := range []struct {
		name     string
		required bool
	}{
		{name: "schema", required: requirements.Schema},
		{name: "admin", required: requirements.Admin},
		{name: "write", required: requirements.Write},
		{name: "read", required: requirements.Read},
	} {
		if permission.required && !allows(permission.name) {
			return permission.name
		}
	}
	return ""
}

// PrincipalAccess is the one permission resolver for a principal, which
// HTTP and Bolt both build from their stores with NewPrincipalAccess:
// whether the principal holds a permission on a graph.
type PrincipalAccess struct {
	// GraphAccess resolves a graph name (a database, alias or composite
	// constituent) to the database its data is in, and reports whether the
	// principal may use it; nil allows every graph.
	GraphAccess func(graph string) (database string, allowed bool)
	// DatabaseAccess is the principal's read and write on a database; nil
	// leaves read and write to Entitlement.
	DatabaseAccess func(database string) auth.ResolvedAccess
	// Entitlement reports a global entitlement (schema, admin, and read and
	// write when DatabaseAccess is nil); nil holds every one.
	Entitlement func(permission string) bool
	// Visibility is the principal's graph visibility on a database: nil
	// when it is unrestricted there (GraphVisibility). It never grants or
	// denies an operation; it narrows what reads see. A nil Visibility
	// restricts nothing.
	Visibility func(database string) (*GraphVisibility, error)
}

// PrincipalAccessSources are a transport's stores behind a principal's
// resolver (NewPrincipalAccess).
type PrincipalAccessSources struct {
	// GraphAccess resolves a graph name to the database its data is in and
	// whether the principal's database access mode allows it (GraphAccess);
	// nil allows every graph.
	GraphAccess func(graph string) (database string, allowed bool)
	// DatabaseAccess is a set of roles' read and write on a database (the
	// server's database privileges); nil leaves read and write to the
	// principal's entitlements.
	DatabaseAccess func(roles []string, database string) auth.ResolvedAccess
	// Visibility is a set of roles' graph visibility on a database (the
	// server's graph privileges and access scopes); nil restricts no one.
	Visibility func(roles []string, database string) (*GraphVisibility, error)
}

// AccessPrincipal is whose privileges a resolver applies: the executing
// user, which is the impersonated user while the signed-in user
// impersonates one (Bolt imp_user), never a union of the two.
type AccessPrincipal struct {
	Roles []string
	// Entitlement reports one of the principal's global entitlements.
	Entitlement func(permission string) bool
}

// NewPrincipalAccess is the one constructor of a principal's permission
// resolver, which HTTP and Bolt both call: graph access from the sources,
// read and write from the principal's roles' database privileges, its graph
// visibility from those roles' graph privileges, the other permissions from
// its entitlements. Without a principal (no authentication) every
// permission the graph access allows is open.
func NewPrincipalAccess(sources PrincipalAccessSources, principal *AccessPrincipal) PrincipalAccess {
	access := PrincipalAccess{GraphAccess: sources.GraphAccess}
	if principal == nil {
		return access
	}
	roles := principal.Roles
	if resolve := sources.DatabaseAccess; resolve != nil {
		access.DatabaseAccess = func(database string) auth.ResolvedAccess { return resolve(roles, database) }
	}
	if resolve := sources.Visibility; resolve != nil {
		access.Visibility = func(database string) (*GraphVisibility, error) { return resolve(roles, database) }
	}
	access.Entitlement = principal.Entitlement
	if access.Entitlement == nil {
		access.Entitlement = func(string) bool { return false }
	}
	return access
}

// Allows reports whether the principal holds permission on graph: it may use
// the graph, read and write come from its access to the database the graph's
// data is in, and the other permissions from its global entitlements.
func (p PrincipalAccess) Allows(graph, permission string) bool {
	database := graph
	if p.GraphAccess != nil {
		target, allowed := p.GraphAccess(graph)
		if !allowed {
			return false
		}
		database = target
	}
	if permission == "access" {
		return true
	}
	if p.DatabaseAccess != nil && (permission == "read" || permission == "write") {
		access := p.DatabaseAccess(database)
		if permission == "read" {
			return access.Read
		}
		return access.Write
	}
	return p.Entitlement == nil || p.Entitlement(permission)
}

// RegisteredProcedureForCall resolves a CALL statement through the canonical
// procedure registry without exposing handlers to callers.
func RegisteredProcedureForCall(query string) (ProcedureSpec, bool) {
	ensureBuiltInProceduresRegistered()
	procedure, found := globalProcedureRegistry.Get(extractProcedureName(query))
	if !found {
		return ProcedureSpec{}, false
	}
	return procedure.Spec, true
}

// authorizeDatabase requires permission on database when the context
// carries database-scoped entitlements, so a procedure that names another
// database (gds.graph.drop's dbName) is checked there, not only where the
// statement runs. A refused "access" is Neo4j's database-access refusal
// (DatabaseAccessDeniedError), as AdmitGraph gives it.
func authorizeDatabase(ctx context.Context, database, permission string) error {
	authorization, ok := ctx.Value(databaseAuthorizationKey{}).(databaseAuthorization)
	if !ok || authorization.resolver(database, permission) {
		return nil
	}
	if permission == "access" {
		return DatabaseAccessDeniedError(ctx)
	}
	denied := permissionDenied(ctx, permission, PermissionRequirements{})
	denied.Database = database
	return denied
}

// schemaCommandAction is Neo4j's schema action for a schema command:
// create_ or drop_, then index or constraint.
func schemaCommandAction(query string) string {
	offset := firstExecutableCypherOffset(query)
	if offset < 0 || offset > len(query) {
		offset = 0
	}
	command := query[offset:]
	verb := "create"
	if startsWithKeywordFold(command, "DROP") {
		verb = "drop"
	}
	object := "index"
	if constraint := findKeywordIndex(command, "CONSTRAINT"); constraint >= 0 {
		if index := findKeywordIndex(command, "INDEX"); index < 0 || constraint < index {
			object = "constraint"
		}
	}
	return verb + "_" + object
}

// schemaProcedureAction is Neo4j's schema action for a schema procedure: the
// index procedures create an index, or drop one.
func schemaProcedureAction(name string) string {
	if strings.Contains(lowerASCII(name), ".drop") {
		return "drop_index"
	}
	return "create_index"
}
