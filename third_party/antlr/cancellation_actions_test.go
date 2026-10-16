package antlr

import (
	"strings"
	"testing"
)

type cancellationPositionAction struct {
	*BaseLexerAction
	value int
}

func (action *cancellationPositionAction) Hash() int { return action.value }
func (action *cancellationPositionAction) Equals(other LexerAction) bool {
	value, ok := other.(*cancellationPositionAction)
	return ok && value.value == action.value
}
func (action *cancellationPositionAction) execute(lexer Lexer) { lexer.SetChannel(action.value) }

func TestCancellationLexerActionOwnership(t *testing.T) {
	actions := make([]LexerAction, 130)
	for i := range actions {
		if i%2 == 0 {
			actions[i] = NewLexerChannelAction(i)
		} else {
			base := NewBaseLexerAction(LexerActionTypeCustom)
			base.isPositionDependent = true
			actions[i] = &cancellationPositionAction{base, i}
		}
	}
	original := NewLexerActionExecutor(actions)
	copy := newLexerActionExecutorWithWork(actions, func() {})
	if !original.Equals(copy) || original.Hash() != copy.Hash() {
		t.Fatal("lexer actions changed")
	}
	if original.Equals(1) || original.Equals((*LexerActionExecutor)(nil)) || original.Equals(NewLexerActionExecutor(actions[:2])) {
		t.Fatal("unequal actions accepted")
	}
	if !original.Equals(original) || original.equalsWithWork(NewLexerActionExecutor([]LexerAction{NewLexerChannelAction(999)}), func() {}) {
		t.Fatal("action identity changed")
	}
	appended := LexerActionExecutorappend(original, NewLexerChannelAction(999))
	if appended.lexerActions.Len() != 131 || original.lexerActions.Len() != 130 {
		t.Fatal("append mutated published actions")
	}
	fixed := original.fixOffsetBeforeMatch(10)
	if fixed == original || original.lexerActions.Len() != fixed.lexerActions.Len() {
		t.Fatal("offset preparation changed source")
	}
	if NewLexerActionExecutor([]LexerAction{NewLexerChannelAction(1)}).fixOffsetBeforeMatch(1) == nil {
		t.Fatal("position-independent actions lost")
	}
	for _, run := range []func(func()){
		func(check func()) { newLexerActionExecutorWithWork(actions, check) },
		func(check func()) { lexerActionExecutorAppendWithWork(original, NewLexerChannelAction(999), check) },
		func(check func()) { original.fixOffsetBeforeMatchWithWork(10, check) },
		func(check func()) { original.equalsWithWork(copy, check) },
	} {
		replayRuntimeCancellation(t, run)
	}
	input := NewInputStream(strings.Repeat("x", 100))
	input.Seek(50)
	lexer := NewBaseLexer(input)
	lexer.Interpreter = NewLexerATNSimulator(lexer, NewATN(ATNTypeLexer, 1), nil, nil)
	fixed.execute(lexer, input, 0)
	if input.Index() != 50 {
		t.Fatal("lexer actions did not restore input")
	}
	replayRuntimeCancellation(t, func(check func()) {
		lexer.Interpreter.(*LexerATNSimulator).SetCancellationCheck(check)
		fixed.execute(lexer, input, 0)
	})
	lexer.Interpreter.(*LexerATNSimulator).SetCancellationCheck(nil)
	if !original.Equals(copy) {
		t.Fatal("cancelled actions mutated published executor")
	}
}

func cancellationTokenStream() *CommonTokenStream {
	stream := NewCommonTokenStream(nil, TokenDefaultChannel)
	stream.index = 0
	stream.fetchedEOF = true
	for i := 0; i < 260; i++ {
		channel := TokenHiddenChannel
		if i == 0 || i == 259 {
			channel = TokenDefaultChannel
		}
		kind := 1
		if i == 259 {
			kind = TokenEOF
		}
		token := NewCommonToken(&TokenSourceCharStreamPair{}, kind, channel, i, i)
		token.SetTokenIndex(i)
		token.SetText("x")
		stream.tokens.Append(token, nil)
	}
	return stream
}

func TestCancellationTokenCollectionGrowth(t *testing.T) {
	stream := cancellationTokenStream()
	if len(stream.GetAllTokens()) != 260 || len(stream.GetTokens(0, 260, nil)) != 259 {
		t.Fatal("token materialization changed")
	}
	if len(stream.GetHiddenTokensToRight(0, -1)) != 258 || len(stream.GetHiddenTokensToLeft(259, TokenHiddenChannel)) != 258 {
		t.Fatal("hidden channel materialization changed")
	}
	if stream.LT(-1) != nil || stream.LT(2).GetTokenType() != TokenEOF {
		t.Fatal("token lookahead changed")
	}
	stream.index = 259
	if stream.LT(-1).GetTokenIndex() != 0 {
		t.Fatal("previous token changed")
	}
	if len(stream.GetAllText()) != 259 {
		t.Fatal("token text changed")
	}
	for _, run := range []func(*CommonTokenStream){
		func(s *CommonTokenStream) { s.GetAllTokens() },
		func(s *CommonTokenStream) { s.GetTokens(0, 260, nil) },
		func(s *CommonTokenStream) { s.GetHiddenTokensToRight(0, -1) },
		func(s *CommonTokenStream) { s.GetHiddenTokensToLeft(259, TokenHiddenChannel) },
		func(s *CommonTokenStream) { s.GetAllText() },
	} {
		replayRuntimeCancellation(t, func(check func()) { value := cancellationTokenStream(); value.SetCancellationCheck(check); run(value) })
	}
	stream.SetTokenSource(nil)
	if stream.tokens.Len() != 0 || stream.index != -1 {
		t.Fatal("stream retained old tokens")
	}
}
