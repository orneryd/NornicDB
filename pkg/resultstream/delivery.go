package resultstream

import (
 "context"
 "sync"
 "sync/atomic"
)

type deliveryKey struct{}

type deliveryCursor struct {
 qid string
 discard func()
}

// Delivery owns pages that have not reached their transport. Fork separates
// statement batches on one connection; a successful metadata write cannot
// acknowledge another batch's records. The zero value is ready to use.
type Delivery struct {
 mu sync.Mutex
 failed bool
 discards []deliveryCursor
 children map[*Delivery]struct{}
 parent *Delivery
 pending atomic.Bool
}

func WithDelivery(ctx context.Context, delivery *Delivery) context.Context {
 return context.WithValue(ctx,deliveryKey{},delivery)
}
func DeliveryFromContext(ctx context.Context) *Delivery {
 delivery,_:=ctx.Value(deliveryKey{}).(*Delivery)
 return delivery
}

// Fork scopes a statement's acknowledgement while retaining connection failure
// ownership. Empty batches never join the parent's pending set.
func (d *Delivery) Fork() *Delivery { return &Delivery{parent:d} }

func (d *Delivery) attach(child *Delivery) bool {
 d.mu.Lock()
 defer d.mu.Unlock()
 if d.failed { return false }
 if d.parent!=nil && !d.parent.attach(d) { return false }
 if d.children==nil { d.children=make(map[*Delivery]struct{}) }
 d.children[child]=struct{}{}
 d.pending.Store(true)
 return true
}
func (d *Delivery) detach(child *Delivery) {
 d.mu.Lock()
 delete(d.children,child)
 d.updatePendingLocked()
 d.mu.Unlock()
}
func (d *Delivery) updatePendingLocked() {
 pending:=len(d.discards)>0||len(d.children)>0
 d.pending.Store(pending)
 if !pending&&d.parent!=nil { d.parent.detach(d) }
}
func (d *Delivery) register(qid string,discard func()) {
 d.mu.Lock()
 failed:=d.failed
 if !failed&&d.parent!=nil { failed=!d.parent.attach(d) }
 if !failed {
  d.discards=append(d.discards,deliveryCursor{qid,discard})
  d.pending.Store(true)
 }
 d.mu.Unlock()
 if failed { discard() }
}

// Fail retires every undelivered page, including publication after failure.
func (d *Delivery) Fail() {
 d.mu.Lock()
 d.failed=true
 discards,children:=d.discards,d.children
 d.discards,d.children=nil,nil
 d.updatePendingLocked()
 d.mu.Unlock()
 for _,cursor:=range discards { cursor.discard() }
 for child:=range children { child.Fail() }
}

// Delivered acknowledges a complete response or statement result.
func (d *Delivery) Delivered() {
 if !d.pending.Load() { return }
 d.mu.Lock()
 children:=d.children
 d.discards,d.children=nil,nil
 d.updatePendingLocked()
 d.mu.Unlock()
 for child:=range children { child.Delivered() }
}

// DeliveredValues acknowledges only qids in records whose complete batch was
// flushed. Other statements and records waiting for a later PULL remain owned.
func (d *Delivery) DeliveredValues(value any) { d.finishValues(value,false) }

// DiscardValues retires pages in records explicitly discarded by the client.
func (d *Delivery) DiscardValues(value any) { d.finishValues(value,true) }

func (d *Delivery) finishValues(value any,discard bool) {
 if !d.pending.Load() { return }
 d.mu.Lock()
 var removed []deliveryCursor
 kept:=d.discards[:0]
 for _,cursor:=range d.discards {
  if deliveryContainsQID(value,cursor.qid) {
   if discard { removed=append(removed,cursor) }
  } else { kept=append(kept,cursor) }
 }
 clear(d.discards[len(kept):])
 d.discards=kept
 d.updatePendingLocked()
 d.mu.Unlock()
 for _,cursor:=range removed { cursor.discard() }
}
func deliveryContainsQID(value any,qid string) bool {
 switch value:=value.(type) {
 case string: return value==qid
 case []any:
  for _,item:=range value { if deliveryContainsQID(item,qid) { return true } }
 case [][]any:
  for _,row:=range value { if deliveryContainsQID(row,qid) { return true } }
 case map[string]any:
  for _,item:=range value { if deliveryContainsQID(item,qid) { return true } }
 }
 return false
}
func (r *Registry) trackDelivery(ctx context.Context,scope Scope,qid string) {
 if delivery:=DeliveryFromContext(ctx);delivery!=nil {
  delivery.register(qid,func(){ _=r.Discard(scope,qid) })
 }
}
