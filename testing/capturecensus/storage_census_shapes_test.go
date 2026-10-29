package capturecensus

// Author: storage-format-owners lane (Claude), #1693, 30 October 2026.

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The census must reject the real failing shapes, planted into the real
// storage package (a type-checked overlay), not into a toy fixture:
//   - nil: the narrow review's nonconstant error return (returnedContext) and
//     whole-struct reset (resetHolder); audit #30's overwritten lazy
//     initialization; NOR-339's nil stored into a cell a deferred closure
//     reads; NOR-337's Engine method passing nil to a context reader;
//   - substitution: a Background passed while the request is in scope, and a
//     TODO created where the request is in scope that reaches a reader only
//     through a field;
//   - silent: BindContext's lazy initialization under a mutex, and a wrapper
//     of a context normalized from nil (periodicBatchContext's shape).
func TestStorageCensusRealShapes(t *testing.T) {
	c := storageNilContextConfig(t, "./pkg/storage")
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "pkg", "storage", "zz_planted_shapes.go"): []byte(`package storage
import (
	"context"
	"sync"
)
func shapeReturnedContext(err error) (context.Context, error) { return nil, err }
func shapeUncheckedSuccess(b *BadgerEngine) error {
	ctx, err := shapeReturnedContext(nil)
	if err != nil {
		return err
	}
	_, err = b.OpenReadSnapshot(ctx)
	return err
}
type shapeResetHolder struct{ ctx context.Context }
func shapeResetHolderFn(h *shapeResetHolder) { *h = shapeResetHolder{} }
func shapeAfterReset(b *BadgerEngine) error {
	h := &shapeResetHolder{ctx: context.Background()}
	shapeResetHolderFn(h)
	_, err := b.OpenReadSnapshot(h.ctx)
	return err
}
type shapeLazyReset struct{ ctx context.Context }
func (l *shapeLazyReset) get() context.Context {
	if l.ctx == nil {
		l.ctx = context.Background()
	}
	l.ctx = nil
	return l.ctx
}
func shapeLazyResetRead(l *shapeLazyReset, b *BadgerEngine) error {
	_, err := b.OpenReadSnapshot(l.get())
	return err
}
func shapeLimitCheck(ctx context.Context) (context.Context, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ctx, nil
}
func shapeDeferredRead(ctx context.Context, b *BadgerEngine) error {
	defer func() { _, _ = b.OpenReadSnapshot(ctx) }()
	var err error
	ctx, err = shapeLimitCheck(ctx)
	if err != nil {
		return err
	}
	return nil
}
func shapeRemoteNil(r *RemoteEngine) (int64, error) {
	return r.remoteCount(nil, "MATCH (n) RETURN count(n)", nil, RemoteAccessRead)
}
func (tx *BadgerTransaction) shapeSubstitute(ctx context.Context, b *BadgerEngine) error {
	_ = ctx
	_, err := b.OpenReadSnapshot(context.Background())
	return err
}
type shapeStash struct{ ctx context.Context }
func (tx *BadgerTransaction) shapeStashTODO(ctx context.Context, s *shapeStash) {
	_ = ctx
	s.ctx = context.TODO()
}
func shapeStashRead(s *shapeStash, b *BadgerEngine) error {
	if s.ctx == nil {
		return nil
	}
	_, err := b.OpenReadSnapshot(s.ctx)
	return err
}
type shapeBind struct {
	mu  sync.Mutex
	ctx context.Context
}
func (p *shapeBind) bind(ctx context.Context) context.Context {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ctx == nil {
		if ctx == nil {
			ctx = context.Background()
		}
		p.ctx, _ = context.WithCancel(ctx)
	}
	return p.ctx
}
func shapeBound(ctx context.Context, p *shapeBind, b *BadgerEngine) error {
	_, err := b.OpenReadSnapshot(p.bind(ctx))
	return err
}
type shapeWrap struct{ context.Context }
func shapeWrapped(ctx context.Context, b *BadgerEngine) error {
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := b.OpenReadSnapshot(shapeWrap{ctx})
	return err
}
var _ = []any{shapeUncheckedSuccess, shapeAfterReset, shapeLazyResetRead, shapeDeferredRead, shapeRemoteNil, (*BadgerTransaction).shapeSubstitute, (*BadgerTransaction).shapeStashTODO, shapeStashRead, shapeBound, shapeWrapped}
`)}
	facts, err := Extract(c, StoragePolicy())
	require.NoError(t, err)
	nil_ := strings.Join(nilContextFlows(facts), "\n")
	all := strings.Join(facts.Violations, "\n")
	for _, reported := range []string{"shapeUncheckedSuccess ->", "shapeAfterReset ->", "shapeLazyResetRead ->", "shapeDeferredRead", "shapeRemoteNil ->"} {
		require.Contains(t, nil_, reported, "a planted nil shape must be reported")
	}
	require.Contains(t, all, "shapeSubstitute creates context.Background: a request context is in scope; pass it")
	require.Contains(t, all, "shapeStashTODO creates context.TODO: a request context is in scope; pass it")
	for _, silent := range []string{"shapeBound", "shapeWrapped"} {
		require.NotContains(t, nil_, silent, "a real silent shape must stay silent")
		require.NotContains(t, all, silent+" creates", "a real silent shape must stay silent")
	}
	counts := AcceptanceCounts(facts.Acceptances)
	for _, kind := range ForbiddenAcceptanceKinds {
		require.Zero(t, counts[kind], "forbidden acceptance kind %s", kind)
	}
}
