package auth

// SCRATCH DIAGNOSTIC (not for merge): logs every storage.ErrConflict raised
// by the FGAC write view, with the observed (Before) image, the stored image
// the view compared it with, which comparison failed, and which writer last
// wrote that entity through an FGAC view (same view = same statement/tx).

import (
	"fmt"
	"log"
	"reflect"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type fgDiagWrite struct {
	view      string
	engine    string
	seq       int64
	updatedAt time.Time
	execAt    time.Time
	at        time.Time
	keys      []string
}

var (
	fgDiagSeq    atomic.Int64
	fgDiagWrites sync.Map // entity id -> fgDiagWrite
)

func fgDiagKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (g *fineGrainedGraph) diagRecordWrite(id string, updatedAt, execAt time.Time, props map[string]any) {
	fgDiagWrites.Store(id, fgDiagWrite{view: fmt.Sprintf("%p", g), engine: fmt.Sprintf("%T/%p", g.engine, g.engine),
		seq: fgDiagSeq.Add(1), updatedAt: updatedAt, execAt: execAt, at: time.Now(), keys: fgDiagKeys(props)})
}

func fgDiagPropDiff(before, visible map[string]any) string {
	var parts []string
	seen := map[string]bool{}
	for k, b := range before {
		seen[k] = true
		v, ok := visible[k]
		if !ok {
			parts = append(parts, fmt.Sprintf("%s: before=%T(%v) visible=<absent>", k, b, b))
		} else if !reflect.DeepEqual(b, v) {
			parts = append(parts, fmt.Sprintf("%s: before=%T(%v) visible=%T(%v)", k, b, b, v, v))
		}
	}
	for k, v := range visible {
		if !seen[k] {
			parts = append(parts, fmt.Sprintf("%s: before=<absent> visible=%T(%v)", k, v, v))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "; ")
}

func (g *fineGrainedGraph) diagConflict(kind, id string, beforeCreated, storedCreated, beforeUpdated, storedUpdated time.Time,
	labelsDiffer bool, beforeProps, visibleProps map[string]any) {
	var failed []string
	if !beforeCreated.Equal(storedCreated) {
		failed = append(failed, "CreatedAt")
	}
	if !beforeUpdated.Equal(storedUpdated) {
		failed = append(failed, "UpdatedAt")
	}
	if labelsDiffer {
		failed = append(failed, "Labels")
	}
	propDiff := ""
	if !reflect.DeepEqual(beforeProps, visibleProps) {
		failed = append(failed, "Properties")
		propDiff = fgDiagPropDiff(beforeProps, visibleProps)
	}
	last := "none recorded"
	if value, ok := fgDiagWrites.Load(id); ok {
		w := value.(fgDiagWrite)
		same := "OTHER view/statement"
		if w.view == fmt.Sprintf("%p", g) {
			same = "SAME view (this statement)"
		}
		last = fmt.Sprintf("seq=%d view=%s engine=%s (%s) forwardedUpdatedAt=%s executorAfterUpdatedAt=%s at=%s keys=%v forwardedEqualsStored=%v executorEqualsBefore=%v",
			w.seq, w.view, w.engine, same, w.updatedAt.Format(time.RFC3339Nano), w.execAt.Format(time.RFC3339Nano), w.at.Format(time.RFC3339Nano), w.keys,
			w.updatedAt.Equal(storedUpdated), w.execAt.Equal(beforeUpdated))
	}
	principal := ""
	if g.request != nil {
		principal = fmt.Sprintf("%+v roles=%v", g.request.identity, g.request.roles)
	}
	stack := string(debug.Stack())
	if len(stack) > 3000 {
		stack = stack[:3000]
	}
	log.Printf("CONFLICTDIAG site=fgac-%s id=%s view=%p engine=%T/%p principal=%s failed=%v beforeCreated=%s storedCreated=%s beforeUpdated=%s storedUpdated=%s propDiff=[%s] lastWriter=[%s]\nCONFLICTDIAG-STACK %s",
		kind, id, g, g.engine, g.engine, principal, failed,
		beforeCreated.Format(time.RFC3339Nano), storedCreated.Format(time.RFC3339Nano),
		beforeUpdated.Format(time.RFC3339Nano), storedUpdated.Format(time.RFC3339Nano), propDiff, last,
		strings.ReplaceAll(stack, "\n", " | "))
}
