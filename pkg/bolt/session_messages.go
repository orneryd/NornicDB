package bolt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// handleRun handles the RUN message (execute Cypher).
func (s *Session) handleRun(data []byte) error {
	// Check authentication
	if s.server != nil && s.server.config.RequireAuth && !s.authenticated {
		return s.sendRunFailure("Neo.ClientError.Security.Unauthorized", "Not authenticated")
	}

	// Parse PackStream to extract query, params, and metadata
	query, params, metadata, err := s.parseRunMessage(data)
	if err != nil {
		return s.sendRunFailure("Neo.ClientError.Request.Invalid", fmt.Sprintf("Failed to parse RUN message: %v", err))
	}

	// Store metadata for potential use (bookmarks, tx_timeout, etc.)
	s.lastRunMetadata = metadata

	// Validate bookmarks if present (for causal consistency)
	if bookmarks, ok := metadata["bookmarks"].([]any); ok && len(bookmarks) > 0 {
		if err := s.validateBookmarks(bookmarks); err != nil {
			return s.sendRunFailure("Neo.ClientError.Transaction.BookmarkValidationFailed",
				fmt.Sprintf("Bookmark validation failed: %v", err))
		}
	}

	// Build the per-RUN context. Lifetime hierarchy (outermost → innermost):
	//
	//   connCtx                  cancels when the connection handler exits
	//     └── traceparent        carries W3C tracing headers (TRC-14)
	//           └── runCtx       cancels on connection close, RESET, RUN
	//                            timeout, or successful return
	//
	// Rooting at connCtx — not spanCtx (which is rooted at
	// context.Background) — closes the v1.1.2 #184 bug where a
	// long-running Cypher pinned a CPU core for ~34 minutes after the
	// client disconnected. Now: handleConnection's defer connCancel()
	// propagates here, the Cypher executor sees ctx.Err() at the next
	// traversal/match boundary, and the RUN goroutine returns.
	parentCtx := s.connCtx
	if parentCtx == nil {
		// Defensive fallback for tests that construct a Session by hand
		// without going through handleConnection. Production always sets
		// connCtx in handleConnection.
		parentCtx = s.spanCtx
		if parentCtx == nil {
			parentCtx = context.Background()
		}
	}
	// Pick the effective timeout: client tx_timeout wins over
	// server-side BoltStatementTimeout (Neo4j semantics — the driver's
	// per-call cap is authoritative; the server cap is the fallback
	// for clients that didn't set one).
	var runTimeout time.Duration
	if txTimeout, ok := metadata["tx_timeout"].(int64); ok && txTimeout > 0 {
		runTimeout = time.Duration(txTimeout) * time.Millisecond
	} else if s.server != nil && s.server.config.BoltStatementTimeout > 0 {
		runTimeout = s.server.config.BoltStatementTimeout
	}

	// Resolve registered procedure modes in addition to preserving the existing
	// Cypher classification. Procedure arguments can hide a mutation from the
	// outer query text, so keyword matching alone is not an authorization boundary.
	requirements := cypher.QueryPermissionRequirements(query)
	isWrite := requirements.Write
	isSchema := requirements.Schema
	isAdmin := requirements.Admin
	upperQuery := strings.ToUpper(query)

	// Check permissions based on query type (use canonical entitlement IDs from auth)
	if s.authResult != nil {
		if isSchema && !s.authResult.HasPermission(string(auth.PermSchema)) {
			return s.sendRunFailure("Neo.ClientError.Security.Forbidden", "Schema operations require schema permission")
		}
		if isAdmin && !s.authResult.HasPermission(string(auth.PermAdmin)) {
			return s.sendRunFailure("Neo.ClientError.Security.Forbidden", "Admin operations require admin permission")
		}
		if isWrite && !s.authResult.HasPermission(string(auth.PermWrite)) {
			return s.sendRunFailure("Neo.ClientError.Security.Forbidden", "Write operations require write permission")
		}
		if !s.authResult.HasPermission(string(auth.PermRead)) {
			return s.sendRunFailure("Neo.ClientError.Security.Forbidden", "Read operations require read permission")
		}
	}
	// Log query if enabled
	if s.server != nil && s.server.config.LogQueries {
		remoteAddr := "unknown"
		if s.conn != nil {
			remoteAddr = s.conn.RemoteAddr().String()
		}
		user := "unknown"
		if s.authResult != nil {
			user = s.authResult.Username
		}
		// D-10a: "[BOLT]" bracket dropped (component attribute carries
		// it). D-03a: any "credentials"/"password"/"token" key in the
		// `params` map is auto-redacted by the Plan 02-01 redactingHandler
		// chain via DefaultRedactKeys.
		if len(params) > 0 {
			s.server.logger().Debug("query",
				"user", user, "remote", remoteAddr,
				"query", truncateQuery(query, 200),
				"params", params,
			)
		} else {
			s.server.logger().Debug("query",
				"user", user, "remote", remoteAddr,
				"query", truncateQuery(query, 200),
			)
		}
	}

	// Resolve effective database name (Neo4j-compatible precedence):
	// 1) RUN metadata db/database
	// 2) active transaction metadata db/database (BEGIN)
	// 3) session database (HELLO)
	// 4) server default
	dbName := ""
	if runDB, ok := databaseFromMetadata(metadata); ok {
		dbName = runDB
	} else if s.inTransaction {
		if txDB, ok := databaseFromMetadata(s.txMetadata); ok {
			dbName = txDB
		}
	}
	if dbName == "" {
		dbName = s.database
	}
	if dbName == "" {
		if s.server != nil && s.server.dbManager != nil {
			dbName = s.server.dbManager.DefaultDatabaseName()
		} else {
			dbName = "nornic" // single-DB mode default
		}
	}
	if s.server != nil && s.server.dbManager != nil && dbName != "" && !constituentAwareExists(s.server.dbManager, dbName) {
		return s.sendRunFailure("Neo.ClientError.Database.DatabaseNotFound",
			fmt.Sprintf("Database '%s' does not exist", dbName))
	}
	// Per-database access: deny if principal may not access this database (Neo4j-aligned).
	var mode auth.DatabaseAccessMode
	if s.server != nil && s.server.databaseAccessModeResolver != nil {
		var roles []string
		if s.authResult != nil {
			roles = s.authResult.Roles
		}
		mode = s.server.databaseAccessModeResolver(roles)
	} else if s.server != nil && s.server.databaseAccessMode != nil {
		mode = s.server.databaseAccessMode
	}
	// When auth is required but no resolver/mode was set (e.g. standalone Bolt), deny all DB access (secure default).
	if mode == nil && s.server != nil && s.server.config.RequireAuth {
		mode = auth.DenyAllDatabaseAccessMode
	}
	if mode != nil && !mode.CanAccessDatabase(dbName) {
		return s.sendRunFailure("Neo.ClientError.Security.Forbidden",
			fmt.Sprintf("Access to database '%s' is not allowed.", dbName))
	}

	// Per-DB write: for mutations, require ResolvedAccess.Write for this (principal, db).
	if isWrite && s.server != nil && s.server.resolvedAccessResolver != nil {
		var roles []string
		if s.authResult != nil {
			roles = s.authResult.Roles
		}
		ra := s.server.resolvedAccessResolver(roles, dbName)
		if !ra.Write {
			return s.sendRunFailure("Neo.ClientError.Security.Forbidden",
				fmt.Sprintf("Write on database '%s' is not allowed.", dbName))
		}
	}

	// Keep explicit transactions pinned to the executor selected at BEGIN.
	executor := s.executor
	if s.inTransaction {
		if s.txDatabase != "" && !strings.EqualFold(dbName, s.txDatabase) {
			return s.sendRunFailure("Neo.ClientError.Transaction.InvalidBookmark",
				fmt.Sprintf("Explicit transaction is bound to database '%s', got '%s'", s.txDatabase, dbName))
		}
	} else if s.server != nil && s.server.dbManager != nil {
		dbExecutor, err := s.getExecutorForDatabase(dbName)
		if err != nil {
			return s.sendRunFailure("Neo.ClientError.Database.DatabaseNotFound",
				fmt.Sprintf("Database '%s' not found: %v", dbName, err))
		}
		executor = dbExecutor
	}

	// Claim explicit-transaction operation ownership only after every
	// response-producing pre-execution check. The claim remains atomic with
	// timeout arbitration, so expiry between dispatch and this point fails
	// closed without making an early-return path responsible for cleanup.
	if s.inTransaction {
		parentCtx, err = s.claimTransactionOperation()
		if errors.Is(err, errTransactionTimedOut) {
			return s.sendTransactionTimeoutFailure()
		}
		if err != nil {
			if s.transactionCleanupFailed {
				return err
			}
			return s.sendRunFailure(
				"Neo.ClientError.Transaction.TransactionNotFound", "No active transaction for RUN")
		}
		defer func() { _ = s.txLifecycle.finishRun() }()
	}
	parentCtx = extractTraceparent(parentCtx, metadata)
	var ctx context.Context
	var runCancel context.CancelFunc
	if runTimeout > 0 {
		ctx, runCancel = context.WithTimeout(parentCtx, runTimeout)
	} else {
		ctx, runCancel = context.WithCancel(parentCtx)
	}
	defer runCancel()
	s.setActiveRun(runCancel)
	defer s.clearActiveRun()
	ctx = cypher.WithAuthToken(ctx, s.forwardedAuthHeader)
	if s.authResult != nil {
		ctx = cypher.WithPermissionChecker(ctx, func(permission string) bool {
			return s.authResult.HasPermission(permission)
		})
	}

	runStart := time.Now()
	result, err := executor.Execute(ctx, query, params)
	// Complete any RUN-owned timeout cleanup before sending a response. The
	// defer remains the panic/early-return safety net; finishRun is idempotent.
	var finishErr error
	if s.inTransaction {
		finishErr = s.txLifecycle.finishRun()
	}
	if finishErr != nil {
		return s.sendTransactionTimeoutAfterJoin()
	}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			switch s.activeRunReason() {
			case MsgReset, MsgGoodbye:
				return nil
			}
		}
		s.logRunTiming("ERROR", dbName, query, time.Since(runStart), 0, err)
		if s.server != nil && s.server.config.LogQueries {
			s.server.logger().Warn("query error")
		}
		code, msg := mapBoltQueryErrorForQuery(err, query)
		return s.sendRunFailure(code, msg)
	}
	rows := 0
	if result != nil && result.Rows != nil {
		rows = len(result.Rows)
	}
	s.logRunTiming("OK", dbName, query, time.Since(runStart), rows, nil)

	// Per-database RBAC: filter SHOW DATABASES results by CanSeeDatabase so principals only see DBs they may access
	if isShowDatabasesQuery(query) && result.Rows != nil && mode != nil {
		filtered := make([][]interface{}, 0, len(result.Rows))
		for _, row := range result.Rows {
			if len(row) > 0 {
				if name, ok := row[0].(string); ok && mode.CanSeeDatabase(name) {
					filtered = append(filtered, row)
				}
			}
		}
		result.Rows = filtered
	}

	// Track write operation for deferred flush
	if isWrite {
		s.pendingFlush = true
	}
	s.lastQueryIsWrite = isWrite
	s.lastQueryDatabase = dbName
	if s.inTransaction {
		s.recordExplicitTransactionWrite(upperQuery, isWrite)
	}

	// Store result for PULL
	s.lastResult = result
	s.resultIndex = 0
	s.queryId++

	// Return SUCCESS with field names (Neo4j compatible metadata)
	// Note: Neo4j only sends qid for EXPLICIT transactions, not implicit/autocommit
	// For implicit transactions, only send fields and t_first
	if s.inTransaction {
		if err := s.sendSuccessNoFlush(map[string]any{
			"fields":  result.Columns,
			"t_first": int64(0),
			"qid":     s.queryId,
		}); err != nil {
			return err
		}
		return s.flushIfPending()
	}
	if err := s.sendSuccessNoFlush(map[string]any{
		"fields":  result.Columns,
		"t_first": int64(0),
	}); err != nil {
		return err
	}
	return s.flushIfPending()
}

func (s *Session) logRunTiming(status, dbName, query string, duration time.Duration, rows int, runErr error) {
	includeQuery := s.server != nil && s.server.config.LogQueries
	if runErr == nil && !includeQuery {
		return
	}
	if s.server == nil {
		return
	}

	// D-10a: "[BOLT]" bracket dropped (component attribute carries it).
	// Errors emit at WARN; successful query timing (only fires when
	// LogQueries=true) emits at DEBUG so it doesn't pollute production
	// stdout at INFO level.
	if runErr != nil {
		attrs := []any{
			"database", dbName, "status", status,
			"rows", rows, "duration", duration,
			"error", runErr.Error(),
		}
		s.server.logger().Warn("run", attrs...)
		return
	}

	attrs := []any{
		"database", dbName, "status", status,
		"rows", rows, "duration", duration,
	}
	s.server.logger().Debug("run", attrs...)
}

func mapBoltQueryError(err error) (code, message string) {
	if err == nil {
		return "Neo.ClientError.Statement.SyntaxError", ""
	}
	var permissionDenied *cypher.PermissionDeniedError
	if errors.As(err, &permissionDenied) {
		return "Neo.ClientError.Security.Forbidden", permissionDenied.Error()
	}
	msg := err.Error()
	if strings.HasPrefix(msg, "Neo.") {
		if idx := strings.Index(msg, ":"); idx > 0 {
			return strings.TrimSpace(msg[:idx]), strings.TrimSpace(msg[idx+1:])
		}
		return msg, msg
	}
	if start := strings.Index(msg, "Neo."); start >= 0 {
		rest := msg[start:]
		if idx := strings.Index(rest, ":"); idx > 0 {
			return strings.TrimSpace(rest[:idx]), strings.TrimSpace(rest[idx+1:])
		}
	}
	if transientCode, ok := nornicerrors.MapTransientTransactionError(err); ok {
		return transientCode, msg
	}
	return "Neo.ClientError.Statement.SyntaxError", msg
}

func mapBoltQueryErrorForQuery(err error, query string) (code, message string) {
	code, message = mapBoltQueryError(err)
	if err != nil && nornicerrors.IsMergeCommitTimeUniqueConflict(err) {
		return nornicerrors.TransientOutdated, message
	}
	return code, message
}

// mapBoltCommitError preserves Bolt's commit-failed fallback for ordinary
// errors while allowing MERGE transaction conflicts to surface as Neo4j
// transient errors for clients that choose retry-managed transaction APIs.
//
// A commit-time UNIQUE constraint violation from a concurrent MERGE race is
// resolvable by a fresh attempt: the loser, on retry, observes the peer's
// now-committed node via the constraint cache and the MERGE matches. The
// raw storage error does not wrap ErrTransactionConflict, so the mapper
// uses the explicit transaction's observed MERGE shape plus the commit error
// body. Non-MERGE transactions keep the ordinary commit-failed code even
// when the body is a UNIQUE violation.
func mapBoltCommitError(err error, canRetryMergeConflict bool) (code, message string) {
	if canRetryMergeConflict {
		err = nornicerrors.MarkMergeCommitTimeUniqueConflict(err)
	}
	code, message = mapBoltQueryError(err)
	if err != nil && canRetryMergeConflict && nornicerrors.IsMergeCommitTimeUniqueConflict(err) {
		return nornicerrors.TransientOutdated, message
	}
	if code == "Neo.ClientError.Statement.SyntaxError" {
		return "Neo.ClientError.Transaction.TransactionCommitFailed", message
	}
	return code, message
}

func (s *Session) recordExplicitTransactionWrite(query string, isWrite bool) {
	if !isWrite {
		return
	}
	info := boltTxWriteAnalyzer.Analyze(query)
	if info != nil && info.HasMerge {
		s.txHasMerge = true
	}
	if info == nil {
		s.txHasNonMergeWrite = true
		return
	}
	if !cypher.IsRetrySafeMergeCommitQuery(info) {
		s.txHasNonMergeWrite = true
	}
}

func (s *Session) canRetryMergeCommitConflict() bool {
	return s.txHasMerge && !s.txHasNonMergeWrite
}

// truncateQuery truncates a query for logging.
func truncateQuery(q string, maxLen int) string {
	if len(q) <= maxLen {
		return q
	}
	return q[:maxLen] + "..."
}

// isShowDatabasesQuery returns true if the normalized statement is SHOW DATABASES (flexible whitespace).
// Used to filter SHOW DATABASES results by CanSeeDatabase when per-database RBAC is enabled.
func isShowDatabasesQuery(query string) bool {
	norm := strings.TrimSpace(query)
	norm = strings.Join(strings.Fields(norm), " ")
	return strings.EqualFold(norm, "SHOW DATABASES")
}

// parseRunMessage parses a RUN message to extract query, parameters, and metadata.
// Bolt v4+ RUN message format: [query: String, parameters: Map, extra: Map]
// Returns: query, parameters, metadata, error
func (s *Session) parseRunMessage(data []byte) (string, map[string]any, map[string]any, error) {
	if len(data) == 0 {
		return "", nil, nil, fmt.Errorf("empty RUN message")
	}

	offset := 0

	// Parse query string
	query, n, err := decodePackStreamString(data, offset)
	if err != nil {
		return "", nil, nil, fmt.Errorf("failed to parse query: %w", err)
	}
	offset += n

	// Parse parameters map
	params := make(map[string]any)
	if offset < len(data) {
		p, consumed, err := decodePackStreamMap(data, offset)
		if err != nil {
			// Params parse failed, use empty map
			params = make(map[string]any)
		} else {
			params = p
			offset += consumed
		}
	}

	// Bolt v4+ has an extra metadata map after params (for bookmarks, tx_timeout, etc.)
	// Parse metadata if present
	metadata := make(map[string]any)
	if offset < len(data) {
		m, _, err := decodePackStreamMap(data, offset)
		if err == nil {
			metadata = m
		}
		// If parsing fails, continue with empty metadata (non-fatal)
	}

	return query, params, metadata, nil
}

// handlePull handles the PULL message.
func (s *Session) handlePull(data []byte) error {
	if s.lastResult == nil {
		// Neo4j doesn't send has_more when false - just empty metadata
		if err := s.sendSuccessNoFlush(map[string]any{}); err != nil {
			return err
		}
		return s.flushIfPending()
	}

	// Parse PULL options (n = number of records to pull)
	pullN := -1 // Default: all records
	if len(data) > 0 {
		opts, _, err := decodePackStreamMap(data, 0)
		if err == nil {
			if n, ok := opts["n"]; ok {
				switch v := n.(type) {
				case int64:
					pullN = int(v)
				case int:
					pullN = v
				}
			}
		}
	}

	// Stream records - use batched writing for large result sets
	remaining := len(s.lastResult.Rows) - s.resultIndex
	if pullN > 0 && remaining > pullN {
		remaining = pullN
	}

	// For large batches (>50 records), use batched writing to reduce syscalls
	if remaining > 50 {
		if err := s.sendRecordsBatched(s.lastResult.Rows[s.resultIndex : s.resultIndex+remaining]); err != nil {
			return err
		}
		s.resultIndex += remaining
	} else {
		// Small batches: send individually (avoids buffer allocation overhead)
		for s.resultIndex < len(s.lastResult.Rows) {
			if pullN == 0 {
				break
			}

			row := s.lastResult.Rows[s.resultIndex]
			if err := s.writeRecordNoFlush(row); err != nil {
				return err
			}

			s.resultIndex++
			if pullN > 0 {
				pullN--
			}
		}
	}

	// Check if more records available
	hasMore := s.resultIndex < len(s.lastResult.Rows)

	// Clear result if done
	if !hasMore {
		// Capture stats before clearing the result reference.
		resultStats := s.lastResult.Stats
		s.lastResult = nil
		s.resultIndex = 0

		// Neo4j-style deferred commit: flush pending writes after streaming completes.
		if err := s.flushPendingExecutorWrites(); err != nil {
			return s.sendFlushLifecycleFailure(err)
		}

		// Return metadata for completed query (Neo4j compatibility)
		// Neo4j sends: type, bookmark, t_last, stats, db (but NOT has_more when false)
		queryType := "r"
		if s.lastQueryIsWrite {
			queryType = "w"
		}

		bookmark := s.currentBookmark()
		if s.lastQueryIsWrite {
			if receiptBookmark, ok := s.bookmarkFromReceipt(); ok {
				bookmark = receiptBookmark
			} else {
				bookmark = s.generateBookmark()
			}
		}

		// Build stats matching Neo4j format (only if there are updates)
		metadata := map[string]any{
			"bookmark": bookmark,
			"type":     queryType,
			"t_last":   int64(0), // Streaming time
		}
		if s.lastQueryDatabase != "" {
			metadata["db"] = s.lastQueryDatabase
		} else if s.database != "" {
			metadata["db"] = s.database
		} else {
			metadata["db"] = "nornic"
		}

		// Neo4j Bolt protocol: emit "stats" map when any counter is non-zero.
		// The Go driver reads this via summary.Counters().
		if resultStats != nil && (resultStats.NodesCreated > 0 ||
			resultStats.NodesDeleted > 0 ||
			resultStats.RelationshipsCreated > 0 ||
			resultStats.RelationshipsDeleted > 0 ||
			resultStats.PropertiesSet > 0 ||
			resultStats.LabelsAdded > 0) {
			metadata["stats"] = map[string]any{
				"nodes-created":         int64(resultStats.NodesCreated),
				"nodes-deleted":         int64(resultStats.NodesDeleted),
				"relationships-created": int64(resultStats.RelationshipsCreated),
				"relationships-deleted": int64(resultStats.RelationshipsDeleted),
				"properties-set":        int64(resultStats.PropertiesSet),
				"labels-added":          int64(resultStats.LabelsAdded),
			}
		}

		// Note: Neo4j does NOT send has_more when it's false
		if err := s.sendSuccessNoFlush(metadata); err != nil {
			return err
		}
		return s.flushIfPending()
	}

	// When there are more records, send has_more: true
	if err := s.sendSuccessNoFlush(map[string]any{
		"has_more": true,
	}); err != nil {
		return err
	}
	return s.flushIfPending()
}

func databaseFromMetadata(metadata map[string]any) (string, bool) {
	if len(metadata) == 0 {
		return "", false
	}
	if raw, ok := metadata["db"]; ok {
		if db, ok := raw.(string); ok {
			db = strings.TrimSpace(db)
			if db != "" {
				return db, true
			}
		}
	}
	if raw, ok := metadata["database"]; ok {
		if db, ok := raw.(string); ok {
			db = strings.TrimSpace(db)
			if db != "" {
				return db, true
			}
		}
	}
	return "", false
}

// handleDiscard handles the DISCARD message.
func (s *Session) handleDiscard(data []byte) error {
	// Capture stats before clearing the result.
	var resultStats *QueryStats
	if s.lastResult != nil {
		resultStats = s.lastResult.Stats
	}
	s.lastResult = nil
	s.resultIndex = 0

	// Neo4j-style deferred commit: flush pending writes after discard.
	if err := s.flushPendingExecutorWrites(); err != nil {
		return s.sendFlushLifecycleFailure(err)
	}

	// Build completion metadata with stats (same contract as PULL completion).
	queryType := "r"
	if s.lastQueryIsWrite {
		queryType = "w"
	}
	bookmark := s.currentBookmark()
	if s.lastQueryIsWrite {
		if receiptBookmark, ok := s.bookmarkFromReceipt(); ok {
			bookmark = receiptBookmark
		} else {
			bookmark = s.generateBookmark()
		}
	}
	metadata := map[string]any{
		"bookmark": bookmark,
		"type":     queryType,
		"t_last":   int64(0),
	}
	if s.lastQueryDatabase != "" {
		metadata["db"] = s.lastQueryDatabase
	} else if s.database != "" {
		metadata["db"] = s.database
	} else {
		metadata["db"] = "nornic"
	}
	if resultStats != nil && (resultStats.NodesCreated > 0 ||
		resultStats.NodesDeleted > 0 ||
		resultStats.RelationshipsCreated > 0 ||
		resultStats.RelationshipsDeleted > 0 ||
		resultStats.PropertiesSet > 0 ||
		resultStats.LabelsAdded > 0) {
		metadata["stats"] = map[string]any{
			"nodes-created":         int64(resultStats.NodesCreated),
			"nodes-deleted":         int64(resultStats.NodesDeleted),
			"relationships-created": int64(resultStats.RelationshipsCreated),
			"relationships-deleted": int64(resultStats.RelationshipsDeleted),
			"properties-set":        int64(resultStats.PropertiesSet),
			"labels-added":          int64(resultStats.LabelsAdded),
		}
	}

	if err := s.sendSuccessNoFlush(metadata); err != nil {
		return err
	}
	return s.flushIfPending()
}

// handleRoute handles the ROUTE message (for cluster routing).
func (s *Session) handleRoute(data []byte) error {
	address := "localhost:7687"
	if s.conn != nil {
		if tcp, ok := s.conn.LocalAddr().(*net.TCPAddr); ok {
			host := tcp.IP.String()
			if host == "" || host == "0.0.0.0" || host == "::" {
				host = "localhost"
			}
			address = fmt.Sprintf("%s:%d", host, tcp.Port)
		}
	}

	if err := s.sendSuccessNoFlush(map[string]any{
		"rt": map[string]any{
			"ttl": 300,
			"servers": []map[string]any{
				{"role": "ROUTE", "addresses": []string{address}},
				{"role": "READ", "addresses": []string{address}},
				{"role": "WRITE", "addresses": []string{address}},
			},
		},
	}); err != nil {
		return err
	}
	return s.flushIfPending()
}

// handleReset handles the RESET message.
// Resets the session state and rolls back any active transaction.
func (s *Session) handleReset(data []byte) error {
	// RESET restores a reusable session only after transaction cleanup proves
	// that backend ownership was released. A failed rollback leaves storage
	// state unknown, so fail the connection closed instead of admitting more
	// work on a potentially open transaction.
	if err := s.rollbackExplicitTransaction(transactionTerminalReset); err != nil {
		return fmt.Errorf("RESET transaction cleanup failed: %w", err)
	}
	s.failedUntilReset = false
	s.lastResult = nil
	s.resultIndex = 0
	if err := s.sendSuccessNoFlush(nil); err != nil {
		return err
	}
	return s.flushIfPending()
}

// handleBegin handles the BEGIN message.
// If the executor implements TransactionalExecutor, starts a real transaction.
// Otherwise, just tracks the transaction state for protocol compliance.
func (s *Session) handleBegin(data []byte) error {
	if s.explicitTransactionsUnsupported {
		return s.sendTransactionControlFailure(
			"Neo.ClientError.Transaction.TransactionStartFailed",
			"explicit transactions require a SessionExecutorFactory that returns a distinct executor per connection",
		)
	}
	// Reject a protocol-invalid nested BEGIN before decoding metadata or
	// resolving another database executor. The active transaction retains its
	// original executor and remains the sole owner of its storage transaction.
	if s.inTransaction {
		return s.sendTransactionControlFailure("Neo.ClientError.Transaction.TransactionStartFailed",
			"an explicit transaction is already active")
	}

	// Parse BEGIN metadata (contains tx_timeout, bookmarks, etc.)
	var metadata map[string]any
	if len(data) > 0 {
		m, _, err := decodePackStreamMap(data, 0)
		if err != nil {
			return s.sendTransactionControlFailure("Neo.ClientError.Request.Invalid",
				fmt.Sprintf("Failed to parse BEGIN metadata: %v", err))
		}
		metadata = m
	}
	txTimeout, err := validateTransactionTimeout(metadata)
	if err != nil {
		return s.sendTransactionControlFailure("Neo.ClientError.Request.Invalid", err.Error())
	}

	// Pin every explicit transaction provider to the BEGIN database, not only
	// database-manager adapters. Factory and single-connection raw executors are
	// session-scoped rather than database-resolving, so a later RUN naming a
	// different database must fail before it reaches that executor.
	txDatabase := s.database
	if metadataDB, ok := databaseFromMetadata(metadata); ok {
		txDatabase = metadataDB
	}
	if txDatabase == "" {
		txDatabase = "nornic"
	}
	if s.server != nil && s.server.dbManager != nil {
		dbName := txDatabase
		if _, explicitlyNamed := databaseFromMetadata(metadata); !explicitlyNamed && s.database == "" {
			dbName = s.server.dbManager.DefaultDatabaseName()
		}
		txExec, err := s.getTransactionalExecutorForDatabase(dbName)
		if err != nil {
			return s.sendTransactionControlFailure("Neo.ClientError.Database.DatabaseNotFound",
				fmt.Sprintf("Database '%s' not found: %v", dbName, err))
		}
		s.executor = txExec
		txDatabase = dbName
	}

	// TRC-14: extract traceparent from BEGIN metadata for distributed tracing.
	txParent := s.connCtx
	if txParent == nil {
		txParent = s.spanCtx
		if txParent == nil {
			txParent = context.Background()
		}
	}
	txParent = extractTraceparent(txParent, metadata)

	txExec, _ := s.executor.(TransactionalExecutor)
	s.txLifecycle.setTimeoutCleanupFailureHandler(s.failClosedTransactionCleanup)
	if err := s.txLifecycle.begin(
		txParent,
		txTimeout,
		txDatabase,
		txExec,
		metadata,
		s.observeTransactionTerminal,
	); err != nil {
		if s.baseExec != nil {
			s.executor = s.baseExec
		}
		if s.txLifecycle.isDefunct() {
			s.markTransactionCleanupFailed()
			if sendErr := s.sendTransactionControlFailure(
				"Neo.ClientError.Transaction.TransactionStartFailed", err.Error()); sendErr != nil {
				return sendErr
			}
			if flushErr := s.flushIfPending(); flushErr != nil {
				return flushErr
			}
			return fmt.Errorf("BEGIN transaction cleanup failed: %w", err)
		}
		return s.sendTransactionControlFailure("Neo.ClientError.Transaction.TransactionStartFailed", err.Error())
	}

	s.inTransaction = true
	s.txMetadata = metadata
	s.txDatabase = txDatabase
	s.txHasMerge = false
	s.txHasNonMergeWrite = false
	if err := s.sendSuccessNoFlush(nil); err != nil {
		return err
	}
	return s.flushIfPending()
}

// handleCommit handles the COMMIT message.
// If the executor implements TransactionalExecutor, commits the real transaction.
func (s *Session) handleCommit(data []byte) error {
	if !s.inTransaction {
		return s.sendTransactionControlFailure("Neo.ClientError.Transaction.TransactionNotFound",
			"No transaction to commit")
	}
	txExec, claimErr := s.txLifecycle.claimCommit()
	if errors.Is(claimErr, errTransactionTimedOut) {
		return s.sendTransactionTimeoutAfterJoin()
	}
	if claimErr != nil {
		s.clearExplicitTransactionState()
		return s.sendTransactionControlFailure("Neo.ClientError.Transaction.TransactionNotFound",
			"No transaction to commit")
	}
	commitCallStarted := false
	commitCallReturned := false
	var commitCallErr error
	defer func() {
		if recovered := recover(); recovered != nil {
			if commitCallStarted {
				if !commitCallReturned {
					commitCallErr = fmt.Errorf("transaction commit panicked: %v", recovered)
				}
				s.txLifecycle.finishCommit(commitCallErr)
				if commitCallErr != nil {
					s.markTransactionCleanupFailed()
				} else {
					s.clearExplicitTransactionState()
				}
			} else {
				if cleanupErr := s.txLifecycle.abortCommitPanic(); cleanupErr != nil {
					s.markTransactionCleanupFailed()
				} else {
					s.clearExplicitTransactionState()
				}
			}
			panic(recovered)
		}
	}()
	committedDatabase := s.txDatabase
	hadWrites := true
	if reporter, ok := s.executor.(pendingTransactionWriteReporter); ok {
		hadWrites = reporter.HasPendingTransactionWrites()
	}

	// The lifecycle claim above stops the timeout timer before entering the
	// executor, so a COMMIT that starts before the deadline remains the sole
	// terminal owner even if storage work crosses the deadline.
	if txExec != nil {
		ctx := context.Background()
		commitCallStarted = true
		commitCallErr = txExec.CommitTransaction(ctx)
		commitCallReturned = true
		if err := commitCallErr; err != nil {
			s.txLifecycle.finishCommit(err)
			code, message := mapBoltCommitError(err, s.canRetryMergeCommitConflict())
			s.markTransactionCleanupFailed()
			if sendErr := s.sendTransactionControlFailure(code, message); sendErr != nil {
				return sendErr
			}
			if flushErr := s.flushIfPending(); flushErr != nil {
				return flushErr
			}
			return fmt.Errorf("COMMIT outcome is unknown: %w", err)
		}
	}
	s.txLifecycle.finishCommit(nil)
	if hadWrites {
		s.invalidateCommittedWriteCaches(committedDatabase)
	}

	s.clearExplicitTransactionState()

	// Generate and store new bookmark for causal consistency
	// This increments the server's transaction sequence and creates a bookmark
	bookmark := s.generateBookmark()

	// Return bookmark for client tracking
	if err := s.sendSuccessNoFlush(map[string]any{
		"bookmark": bookmark,
	}); err != nil {
		return err
	}
	return s.flushIfPending()
}

// generateBookmark generates a unique bookmark for causal consistency tracking.
// Format: "nornicdb:bookmark:<sequence>"
// The sequence number represents the transaction order for causal consistency.
func (s *Session) generateBookmark() string {
	if s.server == nil {
		panic("cannot generate bookmark: session has no server reference")
	}

	// Get next transaction sequence number from server
	s.server.txSequenceMu.Lock()
	s.server.txSequence++
	seqNum := s.server.txSequence
	s.server.txSequenceMu.Unlock()

	// Store sequence in session
	s.lastTxSequence = seqNum

	return formatBookmark(uint64(seqNum))
}

func (s *Session) currentBookmark() string {
	if s.server == nil {
		return formatBookmark(0)
	}
	s.server.txSequenceMu.RLock()
	seqNum := s.server.txSequence
	s.server.txSequenceMu.RUnlock()
	if seqNum < 0 {
		seqNum = 0
	}
	return formatBookmark(uint64(seqNum))
}

func (s *Session) bookmarkFromReceipt() (string, bool) {
	if s.lastResult == nil || s.lastResult.Metadata == nil {
		return "", false
	}

	receiptAny, ok := s.lastResult.Metadata["receipt"]
	if !ok || receiptAny == nil {
		return "", false
	}

	var seq uint64
	switch r := receiptAny.(type) {
	case *storage.Receipt:
		if r != nil {
			seq = r.WALSeqEnd
		}
	case storage.Receipt:
		seq = r.WALSeqEnd
	case map[string]interface{}:
		if val, ok := r["wal_seq_end"].(uint64); ok {
			seq = val
		} else if val, ok := r["wal_seq_end"].(float64); ok {
			seq = uint64(val)
		} else if val, ok := r["wal_seq_end"].(int64); ok {
			seq = uint64(val)
		}
	}

	if seq == 0 {
		return "", false
	}

	s.updateBookmarkSequence(seq)
	return formatBookmark(seq), true
}

func (s *Session) updateBookmarkSequence(seq uint64) {
	if s.server == nil {
		return
	}

	s.server.txSequenceMu.Lock()
	if int64(seq) > s.server.txSequence {
		s.server.txSequence = int64(seq)
	}
	s.server.txSequenceMu.Unlock()
}

func formatBookmark(seq uint64) string {
	return fmt.Sprintf("nornicdb:bookmark:%d", seq)
}

// validateBookmarks validates bookmarks for causal consistency.
// Ensures that all transactions up to the bookmark's sequence number have been committed.
// This provides causal consistency: reads will see all writes that committed before the bookmark.
func (s *Session) validateBookmarks(bookmarks []any) error {
	if len(bookmarks) == 0 {
		return nil // No bookmarks to validate
	}

	if s.server == nil {
		return fmt.Errorf("cannot validate bookmarks: session has no server reference")
	}

	// Get current transaction sequence from server
	s.server.txSequenceMu.RLock()
	currentSequence := s.server.txSequence
	s.server.txSequenceMu.RUnlock()

	// Validate each bookmark
	for _, bookmarkAny := range bookmarks {
		bookmark, ok := bookmarkAny.(string)
		if !ok {
			return fmt.Errorf("invalid bookmark type: expected string, got %T", bookmarkAny)
		}

		// Backward compatibility: older server versions returned this placeholder in SUCCESS.
		// Treat it as "no bookmark" rather than failing the session.
		if bookmark == "nornicdb:tx:auto" {
			continue
		}

		// Only accept NornicDB bookmark format: "nornicdb:bookmark:<sequence>"
		if !strings.HasPrefix(bookmark, "nornicdb:bookmark:") {
			return fmt.Errorf("invalid bookmark format: expected 'nornicdb:bookmark:<sequence>', got %q", bookmark)
		}

		// Parse sequence number from bookmark
		seqStr := strings.TrimPrefix(bookmark, "nornicdb:bookmark:")
		if seqStr == "" {
			return fmt.Errorf("invalid bookmark format: missing sequence number in %q", bookmark)
		}

		bookmarkSeq, err := strconv.ParseInt(seqStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid bookmark format: cannot parse sequence number from %q: %w", bookmark, err)
		}

		// Validate sequence number is non-negative
		if bookmarkSeq < 0 {
			return fmt.Errorf("invalid bookmark: sequence number must be non-negative, got %d", bookmarkSeq)
		}

		// Causal consistency check: bookmark sequence must be <= current sequence
		// This ensures all transactions up to the bookmark have been committed
		if bookmarkSeq > currentSequence {
			return fmt.Errorf("bookmark sequence %d is from the future (current: %d)", bookmarkSeq, currentSequence)
		}

		// Bookmark is valid - all transactions up to this sequence have been committed
	}

	return nil
}

// handleRollback handles the ROLLBACK message.
// If the executor implements TransactionalExecutor, rolls back the real transaction.
func (s *Session) handleRollback(data []byte) error {
	if !s.inTransaction {
		// Not an error to rollback when not in transaction (Neo4j behavior)
		if err := s.sendSuccessNoFlush(nil); err != nil {
			return err
		}
		return s.flushIfPending()
	}

	if err := s.rollbackExplicitTransaction(transactionTerminalRollback); err != nil {
		if err := s.sendTransactionControlFailure(
			"Neo.ClientError.Transaction.TransactionRollbackFailed", err.Error()); err != nil {
			return err
		}
		if flushErr := s.flushIfPending(); flushErr != nil {
			return flushErr
		}
		return fmt.Errorf("ROLLBACK transaction cleanup failed: %w", err)
	}
	if err := s.sendSuccessNoFlush(nil); err != nil {
		return err
	}
	return s.flushIfPending()
}

// sendRecord sends a RECORD response.
// Uses buffer pooling to reduce allocations for high-frequency record sending.
func (s *Session) sendRecord(fields []any) error {
	buf := s.recordBuf
	if cap(buf) < 16*1024 {
		buf = make([]byte, 0, 16*1024)
	}
	buf = buf[:0]

	// Format: <struct marker 0xB1> <signature 0x71> <list of fields>
	buf = append(buf, recordHeader...)
	buf = encodePackStreamListIntoWithUTC(buf, fields, s.useUTCDateTimeStructs())

	// sendChunk flushes immediately, so it's safe to reuse the buffer after.
	err := s.sendChunk(buf)
	s.recordBuf = buf[:0]
	return err
}

// writeRecordNoFlush writes a RECORD message but does not flush.
// It is used by PULL streaming to batch many records into a single flush
// (the final SUCCESS message flushes everything).
func (s *Session) writeRecordNoFlush(fields []any) error {
	buf := s.recordBuf
	if cap(buf) < 16*1024 {
		buf = make([]byte, 0, 16*1024)
	}
	buf = buf[:0]

	buf = append(buf, recordHeader...)
	buf = encodePackStreamListIntoWithUTC(buf, fields, s.useUTCDateTimeStructs())

	err := s.writeMessageNoFlush(buf)
	s.recordBuf = buf[:0]
	return err
}

// sendRecordsBatched sends multiple RECORD responses using buffered I/O.
// This dramatically reduces syscall overhead for large result sets.
// For 500 records: ~500 syscalls → 1 syscall = ~8x faster
// Uses buffer pooling to reduce allocations per record.
func (s *Session) sendRecordsBatched(rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	buf := s.recordBuf
	if cap(buf) < 16*1024 {
		buf = make([]byte, 0, 16*1024)
	}

	// Write all records to buffer (each record is a separate chunk)
	for _, row := range rows {
		// Reset buffer length but keep capacity
		buf = buf[:0]

		// Build record: struct marker + signature + list of fields
		buf = append(buf, recordHeader...)
		buf = encodePackStreamListIntoWithUTC(buf, row, s.useUTCDateTimeStructs())

		// bufio.Writer does not retain the provided slice after Write returns,
		// so it's safe to reuse the pooled buffer on the next iteration.
		if err := s.writeMessageNoFlush(buf); err != nil {
			s.recordBuf = buf[:0]
			return err
		}
	}

	// Don't flush here - let the final SUCCESS message flush everything
	s.recordBuf = buf[:0]
	return nil
}

// sendSuccess sends a SUCCESS response with PackStream encoding.
// Pre-allocated success header
var successHeader = []byte{0xB1, MsgSuccess}
var recordHeader = []byte{0xB1, MsgRecord}
var failureHeader = []byte{0xB1, MsgFailure}
var ignoredMessage = []byte{0xB0, MsgIgnored}

func (s *Session) sendSuccess(metadata map[string]any) error {
	buf := s.recordBuf
	if cap(buf) < 16*1024 {
		buf = make([]byte, 0, 16*1024)
	}
	buf = buf[:0]

	buf = append(buf, successHeader...)
	buf = encodePackStreamMapIntoWithUTC(buf, metadata, s.useUTCDateTimeStructs())

	// sendChunk flushes immediately, so it's safe to reuse the buffer after.
	err := s.sendChunk(buf)
	s.recordBuf = buf[:0]
	return err
}

func (s *Session) sendSuccessNoFlush(metadata map[string]any) error {
	buf := s.recordBuf
	if cap(buf) < 16*1024 {
		buf = make([]byte, 0, 16*1024)
	}
	buf = buf[:0]

	buf = append(buf, successHeader...)
	buf = encodePackStreamMapIntoWithUTC(buf, metadata, s.useUTCDateTimeStructs())

	if err := s.writeMessageNoFlush(buf); err != nil {
		return err
	}
	s.recordBuf = buf[:0]
	s.flushPending = true
	return nil
}

// sendFailure sends a FAILURE response.
// Uses buffer pooling to reduce allocations.
func (s *Session) sendFailure(code, message string) error {
	buf := s.recordBuf
	if cap(buf) < 16*1024 {
		buf = make([]byte, 0, 16*1024)
	}
	buf = buf[:0]

	buf = append(buf, failureHeader...)
	metadata := map[string]any{
		"code":    code,
		"message": message,
	}
	buf = encodePackStreamMapIntoWithUTC(buf, metadata, s.useUTCDateTimeStructs())

	// sendChunk flushes immediately, so it's safe to reuse the buffer after.
	err := s.sendChunk(buf)
	s.recordBuf = buf[:0]
	return err
}

func (s *Session) sendRunFailure(code, message string) error {
	if s.inTransaction {
		s.failedUntilReset = true
	}
	return s.sendFailure(code, message)
}

func (s *Session) sendTransactionControlFailure(code, message string) error {
	s.failedUntilReset = true
	return s.sendFailure(code, message)
}

func (s *Session) sendTransactionTimeoutFailure() error {
	s.failedUntilReset = true
	return s.sendFailure(transactionTimedOutCode, transactionTimedOutMsg)
}

func (s *Session) sendIgnored() error {
	return s.sendChunk(ignoredMessage)
}

// sendChunk sends a chunk to the client using buffered I/O.
// The buffer is flushed after each complete message response.
func (s *Session) sendChunk(data []byte) error {
	if err := s.writeMessageNoFlush(data); err != nil {
		return err
	}
	return s.writer.Flush()
}

func (s *Session) flushIfPending() error {
	if !s.flushPending {
		return nil
	}
	s.flushPending = false
	return s.writer.Flush()
}

// writeMessageNoFlush writes a complete Bolt message using chunk framing, but does
// not flush the underlying buffered writer.
//
// Bolt messages are chunked with 2-byte big-endian sizes and a 0-sized terminator.
// A single message may span multiple chunks (max chunk size is 65535 bytes).
func (s *Session) writeMessageNoFlush(data []byte) error {
	const maxChunkSize = 0xFFFF

	// Preserve existing behavior: even for empty messages, write an explicit
	// 0-sized chunk header followed by the terminator chunk.
	if len(data) == 0 {
		if err := s.writer.WriteByte(0x00); err != nil {
			return err
		}
		if err := s.writer.WriteByte(0x00); err != nil {
			return err
		}
		if err := s.writer.WriteByte(0x00); err != nil {
			return err
		}
		if err := s.writer.WriteByte(0x00); err != nil {
			return err
		}
		return nil
	}

	remaining := data

	for len(remaining) > 0 {
		chunkSize := len(remaining)
		if chunkSize > maxChunkSize {
			chunkSize = maxChunkSize
		}

		if err := s.writer.WriteByte(byte(chunkSize >> 8)); err != nil {
			return err
		}
		if err := s.writer.WriteByte(byte(chunkSize)); err != nil {
			return err
		}
		if _, err := s.writer.Write(remaining[:chunkSize]); err != nil {
			return err
		}

		remaining = remaining[chunkSize:]
	}

	// Terminator chunk (size 0)
	if err := s.writer.WriteByte(0x00); err != nil {
		return err
	}
	return s.writer.WriteByte(0x00)
}
