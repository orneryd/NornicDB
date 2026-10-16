// Copyright (c) 2012-2022 The ANTLR Project. All rights reserved.
// Use of this file is governed by the BSD 3-clause license that
// can be found in the LICENSE.txt file in the project root.

package antlr

// Represents an executor for a sequence of lexer actions which traversed during
// the Matching operation of a lexer rule (token).
//
// <p>The executor tracks position information for position-dependent lexer actions
// efficiently, ensuring that actions appearing only at the end of the rule do
// not cause bloating of the {@link DFA} created for the lexer.</p>

type LexerActionExecutor struct {
	lexerActions workSequence[LexerAction]
	cachedHash   int
}

func NewLexerActionExecutor(actions []LexerAction) *LexerActionExecutor {
	return newLexerActionExecutorWithWork(actions, nil)
}
func newLexerActionExecutorWithWork(actions []LexerAction, check func()) *LexerActionExecutor {
	values := workSequence[LexerAction]{}
	for _, action := range actions {
		values.Append(action, check)
	}
	return lexerActionExecutorFromSequence(values, check)
}
func lexerActionExecutorFromSequence(actions workSequence[LexerAction], check func()) *LexerActionExecutor {
	hash := murmurInit(0)
	for i := 0; i < actions.Len(); i++ {
		hash = murmurUpdate(hash, actions.At(i, check).Hash())
	}
	hash = murmurFinish(hash, actions.Len())
	chargeWork(check)
	return &LexerActionExecutor{lexerActions: actions, cachedHash: hash}
}

// LexerActionExecutorappend creates a [LexerActionExecutor] which executes the actions for
// the input [LexerActionExecutor] followed by a specified
// [LexerAction].
// TODO: This does not match the Java code
func LexerActionExecutorappend(executor *LexerActionExecutor, action LexerAction) *LexerActionExecutor {
	return lexerActionExecutorAppendWithWork(executor, action, nil)
}
func lexerActionExecutorAppendWithWork(executor *LexerActionExecutor, action LexerAction, check func()) *LexerActionExecutor {
	values := workSequence[LexerAction]{}
	if executor != nil {
		for i := 0; i < executor.lexerActions.Len(); i++ {
			values.Append(executor.lexerActions.At(i, check), check)
		}
	}
	values.Append(action, check)
	return lexerActionExecutorFromSequence(values, check)
}

// fixOffsetBeforeMatch creates a [LexerActionExecutor] which encodes the current offset
// for position-dependent lexer actions.
//
// Normally, when the executor encounters lexer actions where
// [LexerAction.isPositionDependent] returns true, it calls
// [IntStream.Seek] on the input [CharStream] to set the input
// position to the end of the current token. This behavior provides
// for efficient [DFA] representation of lexer actions which appear at the end
// of a lexer rule, even when the lexer rule Matches a variable number of
// characters.
//
// Prior to traversing a Match transition in the [ATN], the current offset
// from the token start index is assigned to all position-dependent lexer
// actions which have not already been assigned a fixed offset. By storing
// the offsets relative to the token start index, the [DFA] representation of
// lexer actions which appear in the middle of tokens remains efficient due
// to sharing among tokens of the same Length, regardless of their absolute
// position in the input stream.
//
// If the current executor already has offsets assigned to all
// position-dependent lexer actions, the method returns this instance.
//
// The offset is assigned to all position-dependent
// lexer actions which do not already have offsets assigned.
//
// The func returns a [LexerActionExecutor] that stores input stream offsets
// for all position-dependent lexer actions.
func (l *LexerActionExecutor) fixOffsetBeforeMatch(offset int) *LexerActionExecutor {
	return l.fixOffsetBeforeMatchWithWork(offset, nil)
}
func (l *LexerActionExecutor) fixOffsetBeforeMatchWithWork(offset int, check func()) *LexerActionExecutor {
	var updated *workSequence[LexerAction]
	for i := 0; i < l.lexerActions.Len(); i++ {
		action := l.lexerActions.At(i, check)
		_, indexed := action.(*LexerIndexedCustomAction)
		if action.getIsPositionDependent() && !indexed {
			if updated == nil {
				updated = &workSequence[LexerAction]{}
				for j := 0; j < l.lexerActions.Len(); j++ {
					updated.Append(l.lexerActions.At(j, check), check)
				}
			}
			updated.Set(i, NewLexerIndexedCustomAction(offset, action), check)
		}
	}
	if updated == nil {
		return l
	}
	return lexerActionExecutorFromSequence(*updated, check)
}

// Execute the actions encapsulated by l executor within the context of a
// particular {@link Lexer}.
//
// <p>This method calls {@link IntStream//seek} to set the position of the
// {@code input} {@link CharStream} prior to calling
// {@link LexerAction//execute} on a position-dependent action. Before the
// method returns, the input position will be restored to the same position
// it was in when the method was invoked.</p>
//
// @param lexer The lexer instance.
// @param input The input stream which is the source for the current token.
// When l method is called, the current {@link IntStream//index} for
// {@code input} should be the start of the following token, i.e. 1
// character past the end of the current token.
// @param startIndex The token start index. This value may be passed to
// {@link IntStream//seek} to set the {@code input} position to the beginning
// of the token.
// /
func (l *LexerActionExecutor) execute(lexer Lexer, input CharStream, startIndex int) {
	requiresSeek := false
	stopIndex := input.Index()

	defer func() {
		if requiresSeek {
			input.Seek(stopIndex)
		}
	}()

	for i := 0; i < l.lexerActions.Len(); i++ {
		lexerAction := l.lexerActions.At(i, recognizerWork(lexer))
		if la, ok := lexerAction.(*LexerIndexedCustomAction); ok {
			offset := la.offset
			input.Seek(startIndex + offset)
			lexerAction = la.lexerAction
			requiresSeek = (startIndex + offset) != stopIndex
		} else if lexerAction.getIsPositionDependent() {
			input.Seek(stopIndex)
			requiresSeek = false
		}
		lexerAction.execute(lexer)
	}
}

func (l *LexerActionExecutor) Hash() int {
	if l == nil {
		// TODO: Why is this here? l should not be nil
		return 61
	}

	// TODO: This is created from the action itself when the struct is created - will this be an issue at some point? Java uses the runtime assign hashcode
	return l.cachedHash
}

func (l *LexerActionExecutor) Equals(other interface{}) bool { return l.equalsWithWork(other, nil) }
func (l *LexerActionExecutor) equalsWithWork(other interface{}, check func()) bool {
	chargeWork(check)
	if l == other {
		return true
	}
	value, ok := other.(*LexerActionExecutor)
	if !ok || value == nil || l.cachedHash != value.cachedHash || l.lexerActions.Len() != value.lexerActions.Len() {
		return false
	}
	for i := 0; i < l.lexerActions.Len(); i++ {
		if !l.lexerActions.At(i, check).Equals(value.lexerActions.At(i, check)) {
			return false
		}
	}
	return true
}
