package antlr

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	antlr4 "github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/require"
)

type validationTestWork struct {
	ctx   context.Context
	visit func()
}

func (work *validationTestWork) Continue() bool {
	if work.visit != nil {
		work.visit()
	}
	return work.ctx.Err() == nil
}
func (work *validationTestWork) Err() error          { return work.ctx.Err() }
func (work *validationTestWork) CheckBoundary() bool { return work.ctx.Err() == nil }

func freshValidationObjects() *pooledValidator {
	value := validatorPool.New().(*pooledValidator)
	lexerATN := value.lexer.GetATN()
	lexerDFA := make([]*antlr4.DFA, len(lexerATN.DecisionToState))
	for i, state := range lexerATN.DecisionToState {
		lexerDFA[i] = antlr4.NewDFA(state, i)
	}
	value.lexer.Interpreter = antlr4.NewLexerATNSimulator(value.lexer, lexerATN, lexerDFA, antlr4.NewPredictionContextCache())
	parserATN := value.parser.GetATN()
	parserDFA := make([]*antlr4.DFA, len(parserATN.DecisionToState))
	for i, state := range parserATN.DecisionToState {
		parserDFA[i] = antlr4.NewDFA(state, i)
	}
	value.parser.Interpreter = antlr4.NewParserATNSimulator(value.parser, parserATN, parserDFA, antlr4.NewPredictionContextCache())
	return value
}

func TestValidateCancellationInsideColdPrediction(t *testing.T) {
	query := "UNWIND [1,2,3] AS n WITH n, [x IN range(1,n) WHERE x>0 | x+n] AS xs RETURN DISTINCT n,xs ORDER BY n"
	for _, target := range []string{"ParserATNSimulator).closureWork", "LexerATNSimulator).execATN"} {
		t.Run(target, func(t *testing.T) {
			statement := query
			if strings.Contains(target, "Lexer") {
				statement = "RETURN '" + strings.Repeat("x", 8192) + "'"
			}
			var validator *pooledValidator
			create := func() *pooledValidator { validator = freshValidationObjects(); return validator }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reached := false
			var cancelledAt time.Time
			work := &validationTestWork{ctx: ctx}
			work.visit = func() {
				if strings.Contains(target, "Lexer") && (validator == nil || validator.lexer.GetInputStream() == nil || validator.lexer.GetCharIndex() < 4096) {
					return
				}
				var callers [32]uintptr
				count := runtime.Callers(2, callers[:])
				frames := runtime.CallersFrames(callers[:count])
				for {
					frame, more := frames.Next()
					if strings.Contains(frame.Function, target) {
						reached = true
						cancelledAt = time.Now()
						cancel()
						break
					}
					if !more {
						break
					}
				}
			}
			err := validateWithCancellation(statement, work, create)
			require.True(t, reached, "fixture must cancel inside the selected owner")
			require.ErrorIs(t, err, context.Canceled)
			require.Less(t, time.Since(cancelledAt), 250*time.Millisecond)
			require.NoError(t, validateWithCancellation(statement, nil, freshValidationObjects), "cancellation must not poison shared ATN data")
		})
	}
}

func TestValidateCancellationSingleTokenDeadline(t *testing.T) {
	query := "RETURN '" + strings.Repeat("x", 128<<20) + "'"
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	err := validateWithCancellation(query, &validationTestWork{ctx: ctx}, freshValidationObjects)
	returned := time.Now()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, returned.Sub(deadline), 250*time.Millisecond)
	t.Logf("deadline=%s returned=%s overrun=%s", deadline.Format(time.RFC3339Nano), returned.Format(time.RFC3339Nano), returned.Sub(deadline))
	require.NoError(t, Validate("RETURN 1"))
}

func TestValidateCancellationBoundaryAndPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, ValidateWithCancellation(strings.Repeat(" ", 1<<20), &validationTestWork{ctx: ctx}), context.Canceled)
	work := &validationTestWork{ctx: context.Background(), visit: func() { panic(errors.New("unrelated validator panic")) }}
	require.Panics(t, func() { ValidateWithCancellation("RETURN 1", work) })
	require.NoError(t, Validate("RETURN 1"))
}

func TestValidationGeneratedGrammarCopyBounds(t *testing.T) {
	validator := freshValidationObjects()
	maxAlternatives := 0
	for _, decision := range validator.parser.GetATN().DecisionToState {
		maxAlternatives = max(maxAlternatives, len(decision.GetTransitions()))
	}
	maxTokenCount := len(validator.parser.GetSymbolicNames())
	require.LessOrEqual(t, uintptr(maxTokenCount+2)*unsafe.Sizeof((*antlr4.DFAState)(nil)), uintptr(4096), "parser token-edge array must remain grammar-bounded")
	require.LessOrEqual(t, uintptr(maxAlternatives+1)*unsafe.Sizeof(interface{}(nil)), uintptr(4096), "prediction alternative scratch must remain grammar-bounded")
	t.Logf("fixed grammar rules=%d tokenSlots=%d maxDecisionAlternatives=%d parserEdgeBytes=%d alternativeBytes=%d", len(validator.parser.GetRuleNames()), maxTokenCount, maxAlternatives, uintptr(maxTokenCount+2)*unsafe.Sizeof((*antlr4.DFAState)(nil)), uintptr(maxAlternatives+1)*unsafe.Sizeof(interface{}(nil)))
}

func TestValidateCancellationErrorPagesAndUnicode(t *testing.T) {
	work := &validationTestWork{ctx: context.Background()}
	for _, query := range []string{"", "\u2003RETURN 'α😀'\u2003", "RETURN (", "RETURN n WHERE", "RETURN [x IN [1,2] |]"} {
		nilError := Validate(query)
		checked := ValidateWithCancellation(query, work)
		require.Equal(t, nilError == nil, checked == nil, "%q", query)
		if nilError != nil {
			require.Equal(t, nilError.Error(), checked.Error())
		}
	}
	listener := &parseErrorListener{work: func() {}}
	for i := 0; i < 130; i++ {
		listener.SyntaxError(nil, nil, 1, i, strings.Repeat("x", 8192), nil)
	}
	require.Equal(t, 130, listener.count())
	require.Equal(t, 130, strings.Count(listener.join("; "), "line 1:"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	listener.work = func() {
		if ctx.Err() != nil {
			panic(validationCancelled{})
		}
	}
	require.PanicsWithValue(t, validationCancelled{}, func() { listener.join("; ") })
	require.PanicsWithValue(t, validationCancelled{}, func() { listener.SyntaxError(nil, nil, 1, 1, "error", nil) })
	listener.reset()
	require.Zero(t, listener.count())
	require.Nil(t, listener.head)
	require.Nil(t, listener.tail)
}

func TestValidateCancellationConcurrentColdCaches(t *testing.T) {
	seed := freshValidationObjects()
	lexerATN, parserATN := seed.lexer.GetATN(), seed.parser.GetATN()
	lexerDFA := make([]*antlr4.DFA, len(lexerATN.DecisionToState))
	for i, state := range lexerATN.DecisionToState {
		lexerDFA[i] = antlr4.NewDFA(state, i)
	}
	parserDFA := make([]*antlr4.DFA, len(parserATN.DecisionToState))
	for i, state := range parserATN.DecisionToState {
		parserDFA[i] = antlr4.NewDFA(state, i)
	}
	lexerCache, parserCache := antlr4.NewPredictionContextCache(), antlr4.NewPredictionContextCache()
	create := func() *pooledValidator {
		value := validatorPool.New().(*pooledValidator)
		value.lexer.Interpreter = antlr4.NewLexerATNSimulator(value.lexer, lexerATN, lexerDFA, lexerCache)
		value.parser.Interpreter = antlr4.NewParserATNSimulator(value.parser, parserATN, parserDFA, parserCache)
		return value
	}
	const query = "UNWIND [1,2,3] AS n WITH n, [x IN range(1,n) WHERE x>0 | x+n] AS xs RETURN DISTINCT n,xs ORDER BY n"
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func(cancelled bool) {
			defer workers.Done()
			<-start
			if !cancelled {
				if err := validateWithCancellation(query, nil, create); err != nil {
					t.Error(err)
				}
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			deadline, _ := ctx.Deadline()
			err := validateWithCancellation(query, &validationTestWork{ctx: ctx}, create)
			if err == nil {
				return
			} // A healthy early success is allowed.
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Less(t, time.Since(deadline), 250*time.Millisecond)
		}(i%2 == 0)
	}
	close(start)
	workers.Wait()
	require.NoError(t, validateWithCancellation(query, nil, create), "cancelled concurrent predictions must leave reusable complete cache entries")
}
