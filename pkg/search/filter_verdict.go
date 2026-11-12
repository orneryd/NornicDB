package search

import (
	"context"
	"errors"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type filterVerdictKey struct{}

type filterVerdict struct {
	ready    bool
	admitted bool
	err      error
}

// filterVerdicts belongs to one Search invocation. Both retrieval branches and
// adaptive prefixes share each candidate's first predicate decision, but later
// requests (including continuation pulls) evaluate in their own read lifetime.
// Only identities and decisions are retained, never node bodies or vectors.
// A shared change signal is allocated only when branches meet an in-flight ID.
type filterVerdicts struct {
	mu      sync.Mutex
	nodes   map[storage.EntityIdentity]filterVerdict
	changed chan struct{}
}

func (v *filterVerdicts) evaluate(ctx context.Context, opts *SearchOptions, node *storage.Node) (bool, error) {
	id := node.Identity()
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		v.mu.Lock()
		decision, found := v.nodes[id]
		if found && decision.ready {
			v.mu.Unlock()
			if err := ctx.Err(); err != nil {
				return false, err
			}
			return decision.admitted, decision.err
		}
		if found {
			if v.changed == nil {
				v.changed = make(chan struct{})
			}
			changed := v.changed
			v.mu.Unlock()
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-changed:
			}
			continue
		}
		if v.nodes == nil {
			v.nodes = make(map[storage.EntityIdentity]filterVerdict)
		}
		v.nodes[id] = filterVerdict{}
		v.mu.Unlock()

		admitted, err := opts.Where(ctx, node)
		if canceled := ctx.Err(); canceled != nil {
			admitted = false
			if err == nil {
				err = canceled
			} else {
				err = errors.Join(err, canceled)
			}
		}
		v.mu.Lock()
		v.nodes[id] = filterVerdict{ready: true, admitted: admitted, err: err}
		if v.changed != nil {
			close(v.changed)
			v.changed = nil
		}
		v.mu.Unlock()
		// An error remains an error for every observer. It is never converted to
		// a rejection, and this state cannot escape into a subsequent request.
		return admitted, err
	}
}
