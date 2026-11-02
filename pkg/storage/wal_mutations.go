package storage

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"sync/atomic"
)

// beginNativeMutation joins wrapper admission to the native writer's existing
// final publication decision. The binding is call-local and never stored on a handle.
type nativeWALAdmission struct {
	owner *WALEngine
	state atomic.Uint32 // local, transferred to the existing receipt, released
}

func (a *nativeWALAdmission) releaseAccepted() {
	if a.state.CompareAndSwap(1, 2) {
		a.owner.mutationMu.RUnlock()
	}
}

func (w *WALEngine) finishNativeMutation(a *nativeWALAdmission) {
	if a == nil || a.state.CompareAndSwap(0, 2) {
		w.mutationMu.RUnlock()
	}
}

func (w *WALEngine) beginNativeMutation(ctx context.Context) (context.Context, *nativeWALAdmission, error) {
	if err := w.mutationMu.RLockContext(ctx); err != nil {
		return nil, nil, err
	}
	if !config.IsWALEnabled() {
		return ctx, nil, nil
	}
	if _, ok := UnwrapEngine(w.engine).(*BadgerEngine); !ok {
		w.mutationMu.RUnlock()
		return nil, nil, ErrInvalidData
	}
	admission := &nativeWALAdmission{owner: w}
	return context.WithValue(ctx, nativeWALKey{}, admission), admission, nil
}
func (w *WALEngine) CreateNode(ctx context.Context, node *Node) (NodeID, error) {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return "", err
	}
	defer w.finishNativeMutation(admission)
	return w.engine.CreateNode(selected, node)
}
func (w *WALEngine) UpdateNode(ctx context.Context, request NodeUpdateRequest) error {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return err
	}
	defer w.finishNativeMutation(admission)
	return w.engine.UpdateNode(selected, request)
}
func (w *WALEngine) DeleteNode(ctx context.Context, id NodeID) error {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return err
	}
	defer w.finishNativeMutation(admission)
	return w.engine.DeleteNode(selected, id)
}
func (w *WALEngine) CreateEdge(ctx context.Context, edge *Edge) error {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return err
	}
	defer w.finishNativeMutation(admission)
	return w.engine.CreateEdge(selected, edge)
}
func (w *WALEngine) UpdateEdge(ctx context.Context, request EdgeUpdateRequest) error {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return err
	}
	defer w.finishNativeMutation(admission)
	return w.engine.UpdateEdge(selected, request)
}
func (w *WALEngine) DeleteEdge(ctx context.Context, id EdgeID) error {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return err
	}
	defer w.finishNativeMutation(admission)
	return w.engine.DeleteEdge(selected, id)
}
func (w *WALEngine) BulkCreateNodes(ctx context.Context, nodes []*Node) error {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return err
	}
	defer w.finishNativeMutation(admission)
	if config.IsWALEnabled() {
		database := ""
		for _, node := range nodes {
			if node == nil {
				return ErrInvalidData
			}
			candidate := w.databaseFromNode(node)
			if database == "" {
				database = candidate
			} else if database != candidate {
				return fmt.Errorf("wal: bulk create contains multiple databases: %q vs %q", database, candidate)
			}
		}
	}
	return w.engine.BulkCreateNodes(selected, nodes)
}
func (w *WALEngine) BulkCreateEdges(ctx context.Context, edges []*Edge) error {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return err
	}
	defer w.finishNativeMutation(admission)
	if config.IsWALEnabled() {
		database := ""
		for _, edge := range edges {
			if edge == nil {
				return ErrInvalidData
			}
			candidate, err := w.databaseFromEdge(edge)
			if err != nil {
				return err
			}
			if database == "" {
				database = candidate
			} else if database != candidate {
				return fmt.Errorf("wal: bulk create contains multiple databases: %q vs %q", database, candidate)
			}
		}
	}
	return w.engine.BulkCreateEdges(selected, edges)
}
func (w *WALEngine) BulkDeleteNodes(ctx context.Context, ids []NodeID) error {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return err
	}
	defer w.finishNativeMutation(admission)
	return w.engine.BulkDeleteNodes(selected, ids)
}
func (w *WALEngine) BulkDeleteEdges(ctx context.Context, ids []EdgeID) error {
	selected, admission, err := w.beginNativeMutation(ctx)
	if err != nil {
		return err
	}
	defer w.finishNativeMutation(admission)
	return w.engine.BulkDeleteEdges(selected, ids)
}
