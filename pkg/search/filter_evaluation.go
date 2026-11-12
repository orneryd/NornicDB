package search

import (
	"context"
	"errors"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// ErrFilterEvaluation marks the error of a caller's Where or RelationshipWhere
// that could not decide a candidate. It ends the search that asked: no
// fallback, retry or other query chunk replaces it (finalSearchError), since
// a different candidate set would silently leave out the candidate the
// filter could not decide. The filter's own error stays its cause.
var ErrFilterEvaluation = errors.New("search filter evaluation failed")

// filterEvaluationError is a filter's error marked as ErrFilterEvaluation;
// its text and causes are the filter's own.
type filterEvaluationError struct{ err error }

func (e *filterEvaluationError) Error() string { return e.err.Error() }

func (e *filterEvaluationError) Unwrap() []error { return []error{ErrFilterEvaluation, e.err} }

func filterEvaluationFailed(err error) error {
	if errors.Is(err, ErrFilterEvaluation) {
		return err
	}
	return &filterEvaluationError{err: err}
}

// finalSearchError reports whether err ends a search with no fallback: a
// filter evaluation error. Every consumer that would otherwise replace a
// failed search with another one (Service.Search's vector and full-text
// fallbacks, the query-chunk fallbacks through fatalChunkedSearchError)
// returns such an error instead.
func finalSearchError(err error) bool {
	return errors.Is(err, ErrFilterEvaluation)
}

// where is Where for node under ctx, the one place a node filter is asked: its
// error is a filter evaluation error.
func (opts *SearchOptions) where(ctx context.Context, node *storage.Node) (bool, error) {
	var admitted bool
	var err error
	if verdicts, ok := ctx.Value(filterVerdictKey{}).(*filterVerdicts); ok {
		admitted, err = verdicts.evaluate(ctx, opts, node)
	} else {
		admitted, err = opts.Where(ctx, node)
	}
	if err != nil {
		return false, filterEvaluationFailed(err)
	}
	return admitted, nil
}

// relationshipWhere is RelationshipWhere for edge under ctx, the one place a
// relationship filter is asked: its error is a filter evaluation error.
func (opts *SearchOptions) relationshipWhere(ctx context.Context, edge *storage.Edge, start, end *storage.Node) (bool, error) {
	admitted, err := opts.RelationshipWhere(ctx, edge, start, end)
	if err != nil {
		return false, filterEvaluationFailed(err)
	}
	return admitted, nil
}
