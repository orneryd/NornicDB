package bolt

import (
	"context"
	"errors"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
)

// lazyResultThreshold is how many rows an auto-commit read produces before
// RUN answers with the result still running (#939). A result of at most
// this many rows is produced whole at RUN, as before; a larger one is
// produced as PULL asks for it.
const lazyResultThreshold = 1000

// lazyResult is an auto-commit read still producing rows (#939). Its
// statement runs on the session's goroutine as any statement does; once it
// has more than lazyResultThreshold rows, a helper goroutine answers RUN and
// the client's PULL and DISCARD (serveLazyResult) while the statement
// produces the rows they ask for (cypher.ResultStream). Errors the statement
// meets fail the PULL that reaches them, after the rows before them were
// sent, as in Neo4j. DISCARD, RESET, GOODBYE, any other message and a closed
// connection cancel it.
type lazyResult struct {
	session  *Session
	stream   *cypher.ResultStream
	cancel   context.CancelFunc
	query    string
	database string
	start    time.Time
	rows     int

	// helperDone is closed when serveLazyResult returns; nil until the
	// statement streams.
	helperDone chan struct{}

	// Execute's return values, read after stream.Done; finished is false
	// when Execute panicked.
	result   *QueryResult
	err      error
	finished bool
}

// errStatementAborted is the error a lazy result reports when its statement
// panicked; the panic goes on as the statement's.
var errStatementAborted = errors.New("statement aborted")

// runStreamed runs an auto-commit read with a result stream. When the
// statement finishes within lazyResultThreshold rows (or doesn't stream), it
// returns the whole result as Execute does, and the caller answers RUN.
// When the statement starts streaming, serveLazyResult answers RUN and the
// client's PULLs on a helper goroutine while the statement runs here;
// runStreamed then returns served once both are done, with nothing left to
// answer. Only a statement that streams starts a goroutine.
func (s *Session) runStreamed(ctx context.Context, cancel context.CancelFunc, executor QueryExecutor, query string, params map[string]any, database string, start time.Time) (result *QueryResult, served bool, err error) {
	lazy := &lazyResult{session: s, cancel: cancel, query: query, database: database, start: start}
	lazy.stream = cypher.NewResultStream(lazyResultThreshold, lazy)
	defer lazy.endStatement()
	result, err = executor.Execute(cypher.WithResultStream(ctx, lazy.stream), query, params)
	lazy.result, lazy.err, lazy.finished = result, err, true
	if lazy.helperDone == nil {
		return result, false, err
	}
	return nil, true, nil
}

// ResultStreamStarted starts serveLazyResult when the statement starts
// streaming (cypher.ResultStreamStarter). It is called on the statement's
// goroutine, inside Execute, before the helper exists: the session state
// is still that goroutine's.
func (l *lazyResult) ResultStreamStarted() {
	s := l.session
	s.lastQueryIsWrite = false
	s.lastQueryDatabase = l.database
	s.lastResult = &QueryResult{Columns: l.stream.Columns()}
	s.resultIndex = 0
	s.lastLazy = l
	l.helperDone = make(chan struct{})
	go func() {
		defer close(l.helperDone)
		s.serveLazyResult(l)
	}()
}

// endStatement runs when the statement's Execute returns or panics: once
// the statement streamed, it ends the stream and waits for the helper to
// answer the PULL that reaches the end.
func (l *lazyResult) endStatement() {
	if l.helperDone == nil {
		return
	}
	if !l.finished {
		l.err = errStatementAborted
	}
	l.stream.Finish()
	<-l.helperDone
}

// serveLazyResult answers RUN for a lazy result, then the client's PULL and
// DISCARD until the result ends. Any other message, or a read or write
// error, ends the result and goes back to the session's message loop
// (pendingMessage, pendingErr).
func (s *Session) serveLazyResult(lazy *lazyResult) {
	err := s.sendSuccessNoFlush(map[string]any{
		"fields":  s.lastResult.Columns,
		"t_first": int64(0),
	})
	if err == nil {
		err = s.flushRunResponse()
	}
	for err == nil && s.lastLazy == lazy {
		var msg *boltMessage
		if msg, err = s.nextQueuedMessage(); err != nil {
			break
		}
		if msg.msgType != MsgPull && msg.msgType != MsgDiscard {
			s.pendingMessage = msg
			break
		}
		err = s.processMessage(msg)
	}
	if err != nil {
		s.pendingErr = err
	}
	s.closeLazyResult()
}

// next returns up to n rows (all remaining when n <= 0) and whether the
// statement has finished; once it has, err is the statement's error and
// result its summary.
func (l *lazyResult) next(n int) ([][]any, bool) {
	rows, exhausted := l.stream.Next(n)
	l.rows += len(rows)
	return rows, exhausted
}

// close cancels the statement and waits for it to stop.
func (l *lazyResult) close() {
	l.cancel()
	<-l.stream.Done()
}

// closeLazyResult cancels the session's lazy result, if any, and forgets
// it.
func (s *Session) closeLazyResult() {
	if s.lastLazy == nil {
		return
	}
	lazy := s.lastLazy
	s.lastLazy = nil
	s.lastResult = nil
	s.resultIndex = 0
	lazy.close()
	s.clearActiveRun()
}

// lazyStatementError is the error a lazy result's statement finished with,
// with what cancelled it, if anything (MsgReset, MsgGoodbye or 0).
type lazyStatementError struct {
	err    error
	query  string
	reason byte
}

func (e *lazyStatementError) Error() string { return e.err.Error() }

// fillLazyResult refills stream's rows from its lazy result when PULL or
// DISCARD has used the rows it holds. It returns a *lazyStatementError once
// the statement finished with an error; the stream then has no lazy
// result, and its rows are the ones the statement produced before the
// error.
func (s *Session) fillLazyResult(stream *resultStream, n int) error {
	if stream.lazy == nil || stream.index < len(stream.result.Rows) {
		return nil
	}
	lazy := stream.lazy
	rows, exhausted := lazy.next(n)
	stream.result.Rows, stream.index = rows, 0
	if !exhausted {
		return nil
	}
	stream.lazy = nil
	reason := s.activeRunReason()
	lazy.cancel()
	s.clearActiveRun()
	if lazy.err != nil {
		s.logRunTiming("ERROR", stream.database, lazy.query, time.Since(lazy.start), lazy.rows, lazy.err)
		return &lazyStatementError{err: lazy.err, query: lazy.query, reason: reason}
	}
	s.logRunTiming("OK", stream.database, lazy.query, time.Since(lazy.start), lazy.rows, nil)
	if lazy.result != nil {
		stream.result.Stats = lazy.result.Stats
		stream.result.Metadata = lazy.result.Metadata
	}
	return nil
}

// sendLazyFailure answers the PULL or DISCARD that reached a lazy result's
// error: IGNORED when RESET or GOODBYE cancelled the statement, otherwise
// the statement's FAILURE, as RUN reports it.
func (s *Session) sendLazyFailure(failure *lazyStatementError) error {
	if errors.Is(failure.err, context.Canceled) && (failure.reason == MsgReset || failure.reason == MsgGoodbye) {
		if err := s.sendIgnored(); err != nil {
			return err
		}
		return s.flushIfPending()
	}
	code, message := mapBoltQueryErrorForQuery(failure.err, failure.query)
	return s.sendRunFailureWithDetail(code, message, boltErrorDetail(failure.err))
}
