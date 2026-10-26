package cypher

import (
	"context"
	"errors"
	"testing"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNextShellCommand_Branches(t *testing.T) {
	t.Run("non command input", func(t *testing.T) {
		cmd, rest, ok := nextShellCommand("RETURN 1")
		require.False(t, ok)
		require.Empty(t, cmd)
		require.Equal(t, "RETURN 1", rest)
	})

	t.Run("single line command", func(t *testing.T) {
		cmd, rest, ok := nextShellCommand(":use neo4j\nRETURN 1")
		require.True(t, ok)
		require.Equal(t, ":use neo4j", cmd)
		require.Equal(t, "RETURN 1", rest)
	})

	t.Run("map style param consumes multiline block", func(t *testing.T) {
		input := ":param {a: 1, b: {nested: 2}}\nRETURN $a"
		cmd, rest, ok := nextShellCommand(input)
		require.True(t, ok)
		require.Equal(t, ":param {a: 1, b: {nested: 2}}", cmd)
		require.Equal(t, "RETURN $a", rest)
	})

	t.Run("malformed map style command returns no command", func(t *testing.T) {
		input := ":param {a: 1\nRETURN 1"
		cmd, rest, ok := nextShellCommand(input)
		require.False(t, ok)
		require.Empty(t, cmd)
		require.Equal(t, input, rest)
	})
}

func TestConsumeShellMapCommand_ComplexLexing(t *testing.T) {
	t.Run("supports comments and string braces", func(t *testing.T) {
		input := ":param {a: '{not-a-brace}', // line comment\n b: \"x}y\", /* block {comment} */ c: 3};\nRETURN 1"
		cmd, consumed, ok := consumeShellMapCommand(input)
		require.True(t, ok)
		require.Equal(t, ":param {a: '{not-a-brace}', // line comment\n b: \"x}y\", /* block {comment} */ c: 3}", cmd)
		require.Equal(t, "RETURN 1", input[consumed:])
	})

	t.Run("fails when no opening brace", func(t *testing.T) {
		cmd, consumed, ok := consumeShellMapCommand(":param x => 1")
		require.False(t, ok)
		require.Empty(t, cmd)
		require.Zero(t, consumed)
	})
}

func TestExecuteShellCommand_Errors(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	_, _, err := exec.executeShellCommand(ctx, originalBraceView("   "), originalBraceView(""), nil)
	require.EqualError(t, err, "empty command")

	_, _, err = exec.executeShellCommand(ctx, originalBraceView(":use"), originalBraceView(""), nil)
	require.EqualError(t, err, ":use requires a database name")

	_, _, err = exec.executeShellCommand(ctx, originalBraceView(":unknown thing"), originalBraceView(""), nil)
	require.EqualError(t, err, "unknown command: :unknown")
}

func TestPreprocessShellCommands_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	t.Run("no shell command returns query unchanged with empty result", func(t *testing.T) {
		remaining, outCtx, last, err := exec.preprocessShellCommands(ctx, "RETURN 1", nil)
		require.NoError(t, err)
		require.Equal(t, "RETURN 1", remaining)
		require.NotNil(t, outCtx)
		require.NotNil(t, last)
		require.Empty(t, last.Columns)
		require.Empty(t, last.Rows)
	})

	t.Run("unknown command after valid command", func(t *testing.T) {
		_, _, _, err := exec.preprocessShellCommands(ctx, ":param x => 1\n:oops\nRETURN 1", nil)
		require.EqualError(t, err, "unknown command: :oops")
	})

	t.Run("command execution error bubbles up", func(t *testing.T) {
		_, _, _, err := exec.preprocessShellCommands(ctx, ":use", nil)
		require.EqualError(t, err, ":use requires a database name")
	})
}

func TestExecuteParamCommand_ErrorPaths(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	_, err := exec.executeParamCommand(ctx, expressionBraceView("{}"), nil)
	require.EqualError(t, err, "parameter expression must evaluate to a map")

	_, err = exec.executeParamCommand(ctx, expressionBraceView("{a}"), nil)
	require.ErrorContains(t, err, "invalid map entry")

	_, err = exec.executeParamCommand(ctx, expressionBraceView("{a: 1,, b: 2}"), nil)
	require.EqualError(t, err, "empty map entry")

	_, err = exec.executeParamCommand(ctx, expressionBraceView("{a: missingVar}"), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, nerrors.ErrExpressionEvaluationFailed)
	require.ErrorContains(t, err, "unresolved expression")
}

func TestEvaluateParamMapExpression_ErrorPaths(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()

	_, err := exec.evaluateParamMapExpression(ctx, expressionBraceView("notAMap"), nil)
	require.EqualError(t, err, "parameter expression must evaluate to a map")

	_, err = exec.evaluateParamMapExpression(ctx, expressionBraceView("{badEntry}"), nil)
	require.ErrorContains(t, err, "invalid map entry")

	_, err = exec.evaluateParamMapExpression(ctx, expressionBraceView("{\"\": 1}"), nil)
	require.ErrorContains(t, err, "invalid map entry")

	got, err := exec.evaluateParamMapExpression(ctx, expressionBraceView("{a: 1 + 1, nested: {x: 1}}"), nil)
	require.NoError(t, err)
	require.EqualValues(t, int64(2), got["a"])
	require.Equal(t, map[string]interface{}{"x": int64(1)}, got["nested"])

	_, err = exec.evaluateParamMapExpression(ctx, expressionBraceView("{a: missingVar}"), nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, nerrors.ErrExpressionEvaluationFailed))
}
