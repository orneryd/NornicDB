package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// Statement text admission shares mode, gap and comment facts across normalization,
// framing and execution. Cold malformed-input handling stays with this owner.
// readExecutionModes is the shared header reader for routing, framing and
// mode execution. Conflict reporting waits until a statement body exists.
func readExecutionModes(query string, mode ExecutionMode) (ExecutionMode, string, bool) {
	conflict := false
	for {
		next, inner := executionModePrefix(query)
		if next == ModeNormal {
			if mode != ModeNormal {
				inner = strings.TrimSpace(inner[queryGapEnd(inner, 0):])
			}
			return mode, inner, conflict
		}
		if mode == ModeNormal {
			mode = next
		} else if next != mode {
			conflict = true
		}
		query = inner
	}
}

func executionModePrefix(query string) (ExecutionMode, string) {
	trimmed := strings.TrimSpace(query)
	if len(trimmed) == 0 {
		return ModeNormal, trimmed
	}
	// Only a mode keyword or a leading comment can introduce a mode.
	// Keep ordinary statements on the prefix reader's allocation-free exit.
	switch trimmed[0] {
	case 'e', 'E', 'p', 'P', '/':
	default:
		return ModeNormal, trimmed
	}
	start := queryGapEnd(trimmed, 0)
	if matchKeywordAt(trimmed, start, "EXPLAIN") {
		return ModeExplain, executionModeTail(trimmed, start+7)
	}
	if matchKeywordAt(trimmed, start, "PROFILE") {
		return ModeProfile, executionModeTail(trimmed, start+7)
	}
	return ModeNormal, trimmed
}

func executionModeTail(query string, start int) string {
	end, unclosed := readQueryGap(query, start)
	if unclosed >= 0 {
		// Preamble stripping retains invalid source for lexical admission.
		end = start
	}
	return strings.TrimSpace(query[end:])
}

// executionModeStatement consumes repeated matching prefixes and admits the
// body before planning or re-entering execution. Every accepted prefix makes
// progress, including bare keywords, comments and non-space separators.
func executionModeStatement(mode ExecutionMode, query string) (string, error) {
	if _, unclosed := inspectOutsideStrings(query, ";"); unclosed >= 0 {
		return "", unclosedCommentError(query, unclosed, mode)
	}
	_, statement, conflict := readExecutionModes(query, mode)
	if queryGapEnd(statement, 0) == len(statement) {
		return "", emptyStatementError()
	}
	if conflict {
		return "", executionModeConflictError()
	}
	return statement, nil
}

// normalizeQueryText admits comments in the existing canonicalization scan.
// Canonical ordinary statements use exactly the same quick exit as classification.
func normalizeQueryText(ctx context.Context, query string) (string, *queryRewrite, error) {
	if err := ctx.Err(); err != nil {
		return query, nil, err
	}
	if !queryMayNeedCanonicalRewrite(query) {
		return query, nil, nil
	}
	canonical, rewrite, err := readCanonicalQueryText(query, true)
	if err == nil && rewrite != nil && strings.TrimSpace(canonical) == "" {
		err = emptyStatementError()
	}
	if err == nil {
		canonical, rewrite = preserveStatementPresentation(ctx, query, canonical, rewrite)
	}
	if err := ctx.Err(); err != nil {
		return query, nil, err
	}
	return canonical, rewrite, err
}

func emptyStatementError() error {
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherCoreEmptyQuery())
}

// Only a failing lexical token needs prefix interpretation. Accepted ordinary
// statements never run a second comment or mode reader here.
func unclosedCommentError(query string, index int, mode ExecutionMode) error {
	prefix := query[:index]
	prefix = strings.TrimSpace(prefix[queryGapEnd(prefix, 0):])
	prefix, _ = stripCypherPreamble(prefix)
	_, prefix, conflict := readExecutionModes(prefix, mode)
	code := "Neo.ClientError.Statement.SyntaxError"
	if queryGapEnd(prefix, 0) == len(prefix) {
		code = "Neo.DatabaseError.Statement.ExecutionFailed"
	} else if conflict {
		return executionModeConflictError()
	}
	return localizedStatusError(code, "UnexpectedSyntax", localization.CypherANTLRParseSyntaxError("Unclosed block comment"))
}

func executionModeConflictError() error {
	return nornicerrors.MarkCompileTime(localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument",
		localization.CypherANTLRParseSyntaxError("EXPLAIN and PROFILE cannot be combined")))
}

// statementExecutionModes admits only eligible leading mode/preamble text.
// Missing bodies are syntax errors, including when both modes are present.
func statementExecutionModes(cypher string) (ExecutionMode, string, bool) {
	query := strings.TrimSpace(cypher)
	query = strings.TrimSpace(query[queryGapEnd(query, 0):])
	if len(query) == 0 {
		return ModeNormal, query, false
	}
	switch asciiUpper(query[0]) {
	case 'E', 'P', 'C':
	default:
		return ModeNormal, query, false
	}
	query, _ = stripCypherPreamble(query)
	return readExecutionModes(query, ModeNormal)
}

// readQueryGap keeps ordinary tokens on an inlineable exit. The slow reader is
// shared by strict admission and permissive gap consumers; only comments need
// termination work, and ordinary whitespace never calls the comment reader.
func readQueryGap(query string, index int) (int, int) { return cyphertext.GapEnd(query, index) }

// ValidateStatementText admits raw transport text before any adapter can
// discard malformed comments. For example, CREATE (:N) /* fails before writes.
func ValidateStatementText(query string) error {
	if _, unclosed := inspectOutsideStrings(query, ";"); unclosed >= 0 {
		return unclosedCommentError(query, unclosed, ModeNormal)
	}
	if queryGapEnd(query, 0) == len(query) {
		return emptyStatementError()
	}
	return nil
}
