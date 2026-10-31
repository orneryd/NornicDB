package storage

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
)

// vectorWorkFallbacks counts, per owner site, writes whose storage owner had to
// admit vector work itself because the caller carried no admission. Request
// paths must never reach these sites: the request's own admission is bound to
// its transaction (BeginAdmittedTransaction) or its write handle
// (EngineWithVectorWorkOrigin). Process-internal writes (startup, recovery,
// background jobs, offline tools) legitimately admit here.
var vectorWorkFallbacks sync.Map // site -> *atomic.Int64

// fallbackVectorWorkOrigin admits work at an owner site that received none.
func fallbackVectorWorkOrigin(site string) uint64 {
	counter, ok := vectorWorkFallbacks.Load(site)
	if !ok {
		counter, _ = vectorWorkFallbacks.LoadOrStore(site, new(atomic.Int64))
	}
	counter.(*atomic.Int64).Add(1)
	return CaptureVectorWorkOrigin()
}

// AdmitUnrequestedVectorWork is fallbackVectorWorkOrigin for an owner site
// outside this package (a write entry that received no admission).
func AdmitUnrequestedVectorWork(site string) uint64 { return fallbackVectorWorkOrigin(site) }

// ContinueVectorWork is the one rule for an interior site (work that a
// request entry normally admits before reaching it): it returns the admission
// ctx carries; reached without one (an internal or test caller, never a request:
// request tests assert that no site fires), the work is admitted at site and
// counted.
func ContinueVectorWork(ctx context.Context, site string) (context.Context, uint64) {
	if origin, admitted := VectorWorkOrigin(ctx); admitted {
		return ctx, origin
	}
	origin := fallbackVectorWorkOrigin(site)
	return WithVectorWorkOrigin(ctx, origin), origin
}

// VectorWorkFallback is one owner site's fallback admission count.
type VectorWorkFallback struct {
	Site  string
	Count int64
}

// VectorWorkFallbacks reports how often each owner site admitted vector work
// that arrived without an admission, sorted by site. Request-path tests assert
// that driving a request adds nothing here.
func VectorWorkFallbacks() []VectorWorkFallback {
	var out []VectorWorkFallback
	vectorWorkFallbacks.Range(func(site, counter any) bool {
		out = append(out, VectorWorkFallback{Site: site.(string), Count: counter.(*atomic.Int64).Load()})
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Site < out[j].Site })
	return out
}

// SystemStoreAdmission admits system-store writes: users, roles, privileges,
// database settings and catalog records. System records enqueue no vector
// work, so this admission reads no retirement clock. Every vector-work enqueue
// owner (the pending-marker encoder and the search reservation) refuses it, so
// a system-store write that would enqueue vector work fails instead.
const SystemStoreAdmission uint64 = 1 << 63

// ErrSystemStoreVectorWork reports vector work requested under the
// system-store admission.
var ErrSystemStoreVectorWork = errors.New("system-store write cannot enqueue vector work")

// RefuseSystemStoreVectorWork is the enqueue owners' check of the
// system-store admission.
func RefuseSystemStoreVectorWork(origin uint64) error {
	if origin == SystemStoreAdmission {
		return ErrSystemStoreVectorWork
	}
	return nil
}

// SystemStoreEngine returns engine's write handle under the system-store
// admission (nil for a nil engine). A handle that already carries an
// admission keeps it.
func SystemStoreEngine(engine Engine) Engine {
	if engine == nil {
		return nil
	}
	if _, carried := asVectorWorkOriginCarrier(engine); carried {
		return engine
	}
	return newVectorWorkOriginEngine(engine, SystemStoreAdmission)
}

// BeginSystemStoreTransaction opens a transaction for system-store records
// under the system-store admission.
func BeginSystemStoreTransaction(engine TransactionBeginner) (*BadgerTransaction, error) {
	if engine == nil {
		return nil, errors.New("system-store transaction needs a transactional engine")
	}
	tx, err := engine.BeginTransaction()
	if err != nil {
		return nil, err
	}
	tx.mu.Lock()
	tx.bindVectorWorkOriginLocked(SystemStoreAdmission)
	tx.mu.Unlock()
	return tx, nil
}

// TransactionBeginner opens a native transaction.
type TransactionBeginner interface {
	BeginTransaction() (*BadgerTransaction, error)
}

// BeginAdmittedTransaction is the one owner that opens a transaction for a
// request: the transaction carries the request's admitted origin (read from
// ctx, never captured here), so every write staged in it enqueues vector work
// under that admission.
func BeginAdmittedTransaction(ctx context.Context, engine TransactionBeginner) (*BadgerTransaction, error) {
	origin, admitted := VectorWorkOrigin(ctx)
	if !admitted {
		return nil, ErrVectorWorkNotAdmitted
	}
	if engine == nil {
		return nil, errors.New("admitted transaction needs a transactional engine")
	}
	tx, err := engine.BeginTransaction()
	if err != nil {
		return nil, err
	}
	tx.mu.Lock()
	tx.bindVectorWorkOriginLocked(origin)
	tx.mu.Unlock()
	return tx, nil
}

// VectorWorkOriginDeleter deletes under an admitted vector-work origin. A
// deletion has no entity argument to carry the admission, so every engine
// layer on a request path forwards it explicitly, single and bulk, each to its
// own deletion of that kind (a bulk deletion stays one bulk operation).
type VectorWorkOriginDeleter interface {
	DeleteNodeWithVectorWorkOrigin(id NodeID, origin uint64) error
	DeleteEdgeWithVectorWorkOrigin(id EdgeID, origin uint64) error
	BulkDeleteNodesWithVectorWorkOrigin(ids []NodeID, origin uint64) error
	BulkDeleteEdgesWithVectorWorkOrigin(ids []EdgeID, origin uint64) error
}

// admittedDeleter is the one dispatch decision for admitted deletions. engine's
// admitted form of an operation (plain is its plain method name) is used when
// it is declared at the same embedding depth as engine's plain method, or
// shallower: the admitted call then reaches the owner the plain call would
// reach, with the origin. When the plain method is declared shallower (an outer
// engine overrides it and only inherits the admitted form), the plain method is
// used, so that override is never skipped. Pointer and value engines and any
// embedding depth are handled alike; the decision is per dynamic type.
//
// Go has no API that says where a method is declared. declaresMethod reads the
// position the compiler gives its promotion wrappers, which is an undocumented
// compiler detail. So every doubt resolves to the plain method: the override is
// reached, and a lost origin stays visible as a counted self-admission.
func admittedDeleter(engine Engine, plain string) (VectorWorkOriginDeleter, bool) {
	deleter, ok := engine.(VectorWorkOriginDeleter)
	if !ok {
		return nil, false
	}
	t := reflect.TypeOf(engine)
	key := admittedDeletionKey{t, plain}
	if usable, known := admittedDeletionDecisions.Load(key); known {
		return deleter, usable.(bool)
	}
	plainDepth, plainFound := methodDepth(t, plain, true)
	admittedDepth, admittedFound := methodDepth(t, plain+"WithVectorWorkOrigin", false)
	usable := promotionDetectionWorks() && admittedFound && (!plainFound || admittedDepth <= plainDepth)
	admittedDeletionDecisions.Store(key, usable)
	return deleter, usable
}

type admittedDeletionKey struct {
	engine reflect.Type
	plain  string
}

var admittedDeletionDecisions sync.Map // admittedDeletionKey -> bool

// promotionDetectionWorks checks declaresMethod against known shapes in the
// running binary. If a toolchain stops reporting promotion wrappers the way
// declaresMethod expects, no admitted form is used.
var promotionDetectionWorks = sync.OnceValue(func() bool {
	t := reflect.TypeOf(promotionProbe{})
	return declaresMethod(t, "Declared", false) &&
		!declaresMethod(t, "PromotedByPointer", true) &&
		!declaresMethod(t, "PromotedByValue", true)
})

type promotionProbeByPointer struct{}

//go:noinline
func (*promotionProbeByPointer) PromotedByPointer() {}

type promotionProbeByValue struct{}

//go:noinline
func (promotionProbeByValue) PromotedByValue() {}

type promotionProbe struct {
	*promotionProbeByPointer
	promotionProbeByValue
}

//go:noinline
func (promotionProbe) Declared() {}

// methodDepth is the embedding depth at which t's method name is declared,
// following Go's selector rule breadth first: depth 0 is t itself; an embedded
// struct or pointer field adds one level; an embedded interface field declares
// the methods of its interface at its own depth. doubtDeclares is passed on to
// declaresMethod.
func methodDepth(t reflect.Type, name string, doubtDeclares bool) (int, bool) {
	type level struct {
		t     reflect.Type
		depth int
	}
	queue := []level{{t, 0}}
	seen := map[reflect.Type]bool{}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		base := current.t
		for base.Kind() == reflect.Pointer {
			base = base.Elem()
		}
		if seen[base] {
			continue
		}
		seen[base] = true
		if base.Kind() == reflect.Interface {
			if _, ok := base.MethodByName(name); ok {
				return current.depth, true
			}
			continue
		}
		if declaresMethod(base, name, doubtDeclares) {
			return current.depth, true
		}
		if base.Kind() != reflect.Struct {
			continue
		}
		for i := 0; i < base.NumField(); i++ {
			if field := base.Field(i); field.Anonymous {
				queue = append(queue, level{field.Type, current.depth + 1})
			}
		}
	}
	return 0, false
}

// declaresMethod reports whether t (a non-pointer type) or *t declares name
// itself. A method promoted from an embedded field is a compiler-generated
// wrapper, which the runtime reports at the file "<autogenerated>". A wrapper
// can have the promoted method inlined at its entry, and a declared method can
// start with an inlined call: the runtime then reports the inlined function, so
// the two cannot be told apart. doubtDeclares is the answer in that case. The
// plain method passes true and the admitted form false, so a doubt can only
// select the plain method.
func declaresMethod(t reflect.Type, name string, doubtDeclares bool) bool {
	for _, receiver := range []reflect.Type{t, reflect.PointerTo(t)} {
		method, ok := receiver.MethodByName(name)
		if !ok {
			continue
		}
		fn := runtime.FuncForPC(method.Func.Pointer())
		if fn == nil || !namesMethodOf(fn.Name(), t, name) {
			if doubtDeclares {
				return true
			}
			continue
		}
		if file, _ := fn.FileLine(fn.Entry()); file != "<autogenerated>" {
			return true
		}
	}
	return false
}

// namesMethodOf reports whether a runtime function name is t's own method.
func namesMethodOf(function string, t reflect.Type, name string) bool {
	prefix := t.PkgPath() + "."
	return function == prefix+"(*"+t.Name()+")."+name || function == prefix+t.Name()+"."+name
}

// deleteNodeAdmitted forwards an admitted deletion to engine, or reports that
// engine cannot carry it (handled false).
func deleteNodeAdmitted(engine Engine, id NodeID, origin uint64) (bool, error) {
	if deleter, ok := admittedDeleter(engine, "DeleteNode"); ok {
		return true, deleter.DeleteNodeWithVectorWorkOrigin(id, origin)
	}
	return false, nil
}

func deleteEdgeAdmitted(engine Engine, id EdgeID, origin uint64) (bool, error) {
	if deleter, ok := admittedDeleter(engine, "DeleteEdge"); ok {
		return true, deleter.DeleteEdgeWithVectorWorkOrigin(id, origin)
	}
	return false, nil
}

// bulkDeleteNodesAdmitted forwards an admitted bulk deletion to engine's own
// bulk deletion, or reports that engine cannot carry it (handled false).
func bulkDeleteNodesAdmitted(engine Engine, ids []NodeID, origin uint64) (bool, error) {
	if deleter, ok := admittedDeleter(engine, "BulkDeleteNodes"); ok {
		return true, deleter.BulkDeleteNodesWithVectorWorkOrigin(ids, origin)
	}
	return false, nil
}

func bulkDeleteEdgesAdmitted(engine Engine, ids []EdgeID, origin uint64) (bool, error) {
	if deleter, ok := admittedDeleter(engine, "BulkDeleteEdges"); ok {
		return true, deleter.BulkDeleteEdgesWithVectorWorkOrigin(ids, origin)
	}
	return false, nil
}
