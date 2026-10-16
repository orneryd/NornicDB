package antlr

import (
	"strings"
	"testing"
)

type cancellationMessageListener struct {
	*DefaultErrorListener
	message string
}

func (listener *cancellationMessageListener) SyntaxError(_ Recognizer, _ interface{}, _, _ int, message string, _ RecognitionException) {
	listener.message = message
}

func TestCancellationLexerDiagnosticText(t *testing.T) {
	text := strings.Repeat("αx", 4096)
	run := func(check func()) string {
		input := NewInputStreamWithWork(text, nil)
		input.Seek(input.Size())
		input.cancellationCheck = check
		lexer := NewBaseLexer(input)
		lexer.Interpreter = NewLexerATNSimulator(lexer, NewATN(ATNTypeLexer, 1), nil, nil)
		lexer.TokenStartCharIndex = 0
		lexer.Interpreter.(*LexerATNSimulator).SetCancellationCheck(check)
		listener := &cancellationMessageListener{DefaultErrorListener: &DefaultErrorListener{}}
		lexer.RemoveErrorListeners()
		lexer.AddErrorListener(listener)
		lexer.notifyListeners(nil)
		return listener.message
	}
	if message := run(nil); message != "token recognition error at: '"+text+"'" {
		t.Fatal("diagnostic text changed")
	}
	replayRuntimeCancellation(t, func(check func()) { run(check) })
}

type cancellationMixedPredicateParser struct{ *BaseParser }

func (parser *cancellationMixedPredicateParser) Sempred(_ RuleContext, _ int, predicate int) bool {
	return predicate%2 == 0
}

func TestCancellationFailedPredicatePartition(t *testing.T) {
	simulator, _ := cancellationPrecedenceParser()
	base := simulator.parser.(*BaseParser)
	simulator.parser = &cancellationMixedPredicateParser{base}
	state := NewBasicState()
	state.SetStateNumber(10)
	configs := NewATNConfigSet(false)
	for alt := 1; alt <= 2; alt++ {
		config := NewATNConfig6(state, alt, BasePredictionContextEMPTY)
		config.semanticContext = NewPredicate(0, alt, false)
		configs.Add(config, nil)
	}
	parts := simulator.splitAccordingToSemanticValidity(configs, nil)
	if parts[0].configs.Len() != 1 || parts[1].configs.Len() != 1 {
		t.Fatal("failed predicates partition changed")
	}
	replayRuntimeCancellation(t, func(check func()) {
		simulator.SetCancellationCheck(check)
		simulator.splitAccordingToSemanticValidity(configs, nil)
	})
	simulator.SetCancellationCheck(nil)
}
