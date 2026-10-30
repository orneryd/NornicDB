package search

import (
	"context"
	"errors"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
)

// testRerankAdmission admits scorer calls through a real owner, as the DB's
// provider owner does in production.
func testRerankAdmission() func(context.Context) (context.Context, func(), error) {
	work := lifecycle.NewWorkLifetime(context.Background())
	return func(ctx context.Context) (context.Context, func(), error) {
		_, release, accepted := work.Acquire(work.Context())
		if !accepted {
			return nil, nil, errors.New("reranker owner is closed")
		}
		return ctx, release, nil
	}
}
