package cypher

import (
	"context"
)

// ResultStream lets a caller consume the rows of an auto-commit read as the
// statement produces them, instead of after the whole result is built
// (#939). The caller puts it in the context it passes to Execute
// (WithResultStream) and calls Finish when Execute returns. Execute and Next
// run on different goroutines: the caller either runs Execute on its own
// goroutine, or starts consuming on another one when the stream starts
// (NewResultStream's onStart).
//
// A statement the stream applies to buffers its first rows. If it ends
// within the threshold, nothing changes: Execute returns every row in its
// result, and Started never closes. Once the statement has produced more
// rows than the threshold, Started closes: Columns names the result's
// columns, Next hands out rows, and the statement only produces the rows
// Next asks for, plus at most one batch ahead (Next). Execute then returns
// once the statement has finished,
// with the statement's counters and error and without the streamed rows. An
// error the statement meets after Started is the error Execute returns, so
// a caller reports it where the client reaches it, as Neo4j does.
//
// The statement stops when the context passed to Execute is cancelled;
// a caller that stops reading cancels it and waits for Done.
//
// Execute uses the stream only for a top-level auto-commit read that ends
// in a plain RETURN (no ORDER BY, DISTINCT, SKIP, LIMIT or aggregation)
// over rows the pipeline produces one at a time. Every other statement runs
// as before and returns its whole result.
type ResultStream struct {
	threshold int
	onStart   ResultStreamStarter

	// Set by Execute before the statement runs; read by the statement.
	statement     string
	mapColumns    func([]string) []string
	recordAccess  func([][]interface{})
	frame         *pipelineClause
	columns       []string

	// Rows the statement produced before it started streaming. The
	// statement appends while it hasn't started; Next pops them after.
	buffered [][]interface{}
	// Rows produced after the last full batch, read by Next after Done.
	tail [][]interface{}
	// The batch size: the rows of the first request after the stream
	// started (0 until then, -1 for every remaining row).
	want int

	startedSignal chan struct{}
	started       bool
	demand        chan int
	batches       chan [][]interface{}
	done          chan struct{}
	demanded      bool
	exhausted     bool
}

// ResultStreamStarter is told when a statement starts streaming.
type ResultStreamStarter interface {
	// ResultStreamStarted is called on the statement's goroutine when it
	// starts streaming, after Started is closed.
	ResultStreamStarted()
}

// NewResultStream returns a stream whose statement starts streaming after
// threshold rows (at least 1). onStart, when set, is told when it does: a
// caller running the statement on its own goroutine uses it to start
// consuming the rows on another.
func NewResultStream(threshold int, onStart ResultStreamStarter) *ResultStream {
	if threshold < 1 {
		threshold = 1
	}
	return &ResultStream{
		onStart:       onStart,
		threshold:     threshold,
		startedSignal: make(chan struct{}),
		done:          make(chan struct{}),
	}
}

type resultStreamKey struct{}

// WithResultStream returns ctx carrying stream for the next Execute.
func WithResultStream(ctx context.Context, stream *ResultStream) context.Context {
	return context.WithValue(ctx, resultStreamKey{}, stream)
}

// takeResultStream returns the stream the caller passed to Execute, and ctx
// without it: statements Execute runs on the statement's behalf (a USE
// target, a procedure's query) are not the client's statement.
func takeResultStream(ctx context.Context) (*ResultStream, context.Context) {
	stream, _ := ctx.Value(resultStreamKey{}).(*ResultStream)
	if stream == nil {
		return nil, ctx
	}
	return stream, context.WithValue(ctx, resultStreamKey{}, (*ResultStream)(nil))
}

type armedResultStreamKey struct{}

// armResultStream makes stream available to the statement text statement,
// the text Execute routes. mapColumns maps the statement's column names to
// the ones the client sees; recordAccess records the access to the nodes
// and relationships of rows handed to the client.
func armResultStream(ctx context.Context, stream *ResultStream, statement string, mapColumns func([]string) []string, recordAccess func([][]interface{})) context.Context {
	stream.statement = statement
	stream.mapColumns = mapColumns
	stream.recordAccess = recordAccess
	return context.WithValue(ctx, armedResultStreamKey{}, stream)
}

// bindResultStream binds the armed stream to the clause list the pipeline
// runs for the statement itself, when cypher is the armed statement's text
// and nothing has bound it yet. Subqueries and UNION branches run other
// texts, so they never bind it.
func bindResultStream(ctx context.Context, cypher string, clauses []pipelineClause) {
	stream, _ := ctx.Value(armedResultStreamKey{}).(*ResultStream)
	if stream == nil || stream.frame != nil || stream.statement != cypher || len(clauses) == 0 {
		return
	}
	stream.frame = &clauses[len(clauses)-1]
}

// boundResultStream returns the armed stream when clause is the last clause
// of the clause list it is bound to: the statement's own RETURN, not one of
// a subquery the statement runs.
func boundResultStream(ctx context.Context, clause *pipelineClause) *ResultStream {
	stream, _ := ctx.Value(armedResultStreamKey{}).(*ResultStream)
	if stream == nil || stream.frame == nil || stream.frame != clause {
		return nil
	}
	return stream
}

// streamsScan reports whether the MATCH at clauses[index] belongs to the
// statement the stream is bound to, so its scan may feed rows as they are
// read: the statement only reads, and only reads what it streams.
func streamsScan(ctx context.Context, clauses []pipelineClause) bool {
	stream, _ := ctx.Value(armedResultStreamKey{}).(*ResultStream)
	return stream != nil && stream.frame != nil && len(clauses) > 0 && stream.frame == &clauses[len(clauses)-1]
}

// begin records the RETURN's columns before its first row.
func (s *ResultStream) begin(columns []string) {
	s.columns = columns
}

// emit hands one RETURN row to the stream. It returns false when the
// statement must stop: ctx was cancelled while the stream waited for the
// consumer.
func (s *ResultStream) emit(ctx context.Context, row []interface{}) bool {
	if !s.started {
		s.buffered = append(s.buffered, row)
		if len(s.buffered) > s.threshold {
			s.start()
		}
		return true
	}
	if s.want == 0 {
		select {
		case s.want = <-s.demand:
		case <-ctx.Done():
			return false
		}
	}
	s.tail = append(s.tail, row)
	if s.want < 0 || len(s.tail) < s.want {
		return true
	}
	batch := s.tail
	s.recordBatch(batch)
	select {
	case s.batches <- batch:
	case <-ctx.Done():
		return false
	}
	// The next batch is made while the consumer sends this one: at most
	// one batch is produced ahead of the rows asked for.
	s.tail = nil
	return true
}

func (s *ResultStream) start() {
	if s.mapColumns != nil {
		s.columns = s.mapColumns(s.columns)
	}
	s.recordBatch(s.buffered)
	// The hand-over channels are only used once the statement streams;
	// Next reads them after Started is closed.
	s.demand = make(chan int)
	s.batches = make(chan [][]interface{})
	s.started = true
	close(s.startedSignal)
	if s.onStart != nil {
		s.onStart.ResultStreamStarted()
	}
}

func (s *ResultStream) recordBatch(rows [][]interface{}) {
	if s.recordAccess != nil && len(rows) > 0 {
		s.recordAccess(rows)
	}
}

// end closes the RETURN. It returns the rows still buffered when the
// statement never started streaming (they are the result's rows), and
// whether it streamed. The rows of a last partial batch stay in the stream
// for Next.
func (s *ResultStream) end() ([][]interface{}, bool) {
	if s.started {
		s.recordBatch(s.tail)
		return nil, true
	}
	rows := s.buffered
	s.buffered = nil
	return rows, false
}

// discard drops what a RETURN buffered before it started streaming: the
// statement is running another way.
func (s *ResultStream) discard() {
	if !s.started {
		s.buffered = nil
	}
}

// streamed reports whether the statement started streaming.
func (s *ResultStream) streamed() bool {
	return s != nil && s.started
}

// Started is closed once the statement streams: Columns and Next apply.
func (s *ResultStream) Started() <-chan struct{} { return s.startedSignal }

// Done is closed by Finish.
func (s *ResultStream) Done() <-chan struct{} { return s.done }

// Finish tells the stream that Execute returned. The caller that runs
// Execute calls it exactly once, after Execute returns.
func (s *ResultStream) Finish() { close(s.done) }

// Columns returns the result's columns once Started is closed.
func (s *ResultStream) Columns() []string { return s.columns }

// Next returns up to n rows (every remaining row when n <= 0) once Started
// is closed, producing them as needed, and whether the result is
// exhausted: the statement finished and every row was returned. Once it
// reports exhaustion, the caller reads Execute's return values. Next is not
// safe for concurrent use.
//
// The first Next that needs the statement to produce rows sets the batch
// size to its n. The statement then makes one batch ahead of the rows
// asked for, so producing a batch overlaps sending the previous one; rows
// a batch holds beyond n are kept for the next Next.
func (s *ResultStream) Next(n int) ([][]interface{}, bool) {
	var out [][]interface{}
	out = s.take(out, s.buffered, n, &s.buffered)
	if !s.exhausted && !s.demanded && (n <= 0 || len(out) < n) {
		size := n
		if n <= 0 {
			size = -1
		}
		select {
		case s.demand <- size:
			s.demanded = true
		case <-s.done:
			out = s.finishRows(out, n)
		}
	}
	for !s.exhausted && (n <= 0 || len(out) < n) {
		select {
		case batch := <-s.batches:
			out = s.take(out, batch, n, &s.buffered)
		case <-s.done:
			out = s.finishRows(out, n)
		}
	}
	return out, s.exhausted && len(s.buffered) == 0
}

// take appends rows to out up to n rows in all (every row when n <= 0) and
// leaves the rest in *rest.
func (s *ResultStream) take(out, rows [][]interface{}, n int, rest *[][]interface{}) [][]interface{} {
	room := len(rows)
	if n > 0 && room > n-len(out) {
		room = n - len(out)
	}
	*rest = rows[room:]
	return append(out, rows[:room:room]...)
}

// finishRows takes the statement's last partial batch once it finished.
func (s *ResultStream) finishRows(out [][]interface{}, n int) [][]interface{} {
	out = s.take(out, s.tail, n, &s.buffered)
	s.tail = nil
	s.exhausted = true
	return out
}
