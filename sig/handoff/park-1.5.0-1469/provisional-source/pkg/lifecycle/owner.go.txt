// Package lifecycle owns admission, cancellation, resource generations and
// shutdown completion for a database and the components using it.
package lifecycle

import (
 "context"
 "errors"
 "sync"
 "sync/atomic"
)

type closedError struct{}
func (*closedError) Error() string { return "database is closed" }
func (*closedError) Unwrap() error { return context.Canceled }
// ErrClosed identifies shutdown, as distinct from an unavailable model or a
// caller canceling an otherwise live database operation.
var ErrClosed error = &closedError{}

type phase uint32
const (open phase = iota; quiescing; draining; releasing; closed)
type resourceState uint8
const (resourceLive resourceState = iota; resourceRetiring; resourceReleasing; resourceReleased)
type eventKind uint8
const (admit eventKind = iota; leave; suspend; resume; seal; beginDrain; startRelease; complete; sealGroup; completeGroup; own; retireResource; releaseResource; completeResource)
type event struct {
 kind eventKind
 activity *Activity
 group *Group
 resource *Resource
 err error
}

// Owner is the only writer of lifecycle state. All phase, resource, activity
// and completion changes pass through transition under mu. Atomic phase reads
// are a view of those transitions, not a second shutdown flag.
type Owner struct {
 mu sync.Mutex
 changed *sync.Cond
 phase atomic.Uint32
 active int
 ctx context.Context
 cancel context.CancelCauseFunc
 done chan struct{}
 err error
 resources []*Resource
 identities map[any]*Resource
}

// New constructs an open owner. It creates no worker goroutine.
func New() *Owner {
 ctx,cancel:=context.WithCancelCause(context.Background())
 o:=&Owner{ctx:ctx,cancel:cancel,done:make(chan struct{}),identities:make(map[any]*Resource)}
 o.changed=sync.NewCond(&o.mu)
 return o
}
func(o *Owner) Context() context.Context { return o.ctx }
func(o *Owner) Closing() bool { return o!=nil && phase(o.phase.Load())!=open }
func(o *Owner) Done() <-chan struct{} { return o.done }

func(o *Owner) transition(e event) bool {
 o.mu.Lock();defer o.mu.Unlock()
 return o.reduce(e)
}
// reduce is the single writer. The caller holds mu, including an Own lookup
// that must stay indivisible with publishing the resource generation.
func(o *Owner) reduce(e event) bool {
 accepted:=true
 a,g,r:=e.activity,e.group,e.resource
 switch e.kind {
 case admit,resume:
  if a!=nil {g=a.group;r=a.resource}
  if e.kind==resume {a.pauses--;if a.pauses!=0{return true}}
  if phase(o.phase.Load())!=open || (g!=nil && g.Closing()) || (r!=nil && r.state!=resourceLive) {return false}
  if a.ended {return false}
  if a.held {panic("lifecycle: admitting an active operation")}
  a.held=true;o.active++
  for p:=g;p!=nil;p=p.parent {p.active++};if r!=nil {r.active++}
 case leave,suspend:
  if e.kind==suspend {a.pauses++} else {a.ended=true}
  if !a.held {return true}
  a.held=false;o.active--
  for p:=a.group;p!=nil;p=p.parent {p.active--};if a.resource!=nil {a.resource.active--}
 case seal:
  if phase(o.phase.Load())!=open {return false}
  o.phase.Store(uint32(quiescing))
 case beginDrain:
  o.phase.Store(uint32(draining))
 case startRelease:
  if o.active!=0 {panic("lifecycle: release with active users")}
  o.phase.Store(uint32(releasing))
 case complete:
  o.err=e.err;o.phase.Store(uint32(closed));close(o.done)
 case sealGroup:
  if phase(g.phase.Load())!=open {return false}
  g.phase.Store(uint32(quiescing))
 case completeGroup:
  g.err=e.err;g.phase.Store(uint32(closed));close(g.done)
 case own:
  accepted=phase(o.phase.Load())==open
  o.resources=append(o.resources,r)
  if r.identity!=nil {o.identities[r.identity]=r}
  if !accepted {r.state=resourceRetiring}
 case retireResource:
  if r.state==resourceLive {r.state=resourceRetiring}
 case releaseResource:
  if r.state!=resourceRetiring || r.active!=0 {return false}
  r.state=resourceReleasing
 case completeResource:
  r.err=e.err;r.state=resourceReleased;close(r.done)
 }
 o.changed.Broadcast()
 return accepted
}

// Close seals admission and cancels all linked work. prepare stops component
// producers; finish closes storage after every actual resource user has left.
// Every caller observes the same completed result.
func(o *Owner) Close(prepare func(),finish func() error) error {
 if o.transition(event{kind:seal}) {
  o.cancel(ErrClosed)
  if prepare!=nil {prepare()}
  o.transition(event{kind:beginDrain})
  o.drain()
  o.transition(event{kind:startRelease})
  o.mu.Lock();resources:=append([]*Resource(nil),o.resources...);o.mu.Unlock()
  var errs []error
  for _,r:=range resources {if err:=r.Release();err!=nil {errs=append(errs,err)}}
  if finish!=nil {if err:=finish();err!=nil {errs=append(errs,err)}}
  o.transition(event{kind:complete,err:errors.Join(errs...)})
 }
 <-o.done
 o.mu.Lock();err:=o.err;o.mu.Unlock();return err
}
func(o *Owner) drain() {
 o.mu.Lock();defer o.mu.Unlock()
 for o.active!=0 {o.changed.Wait()}
}

// Group is a component's view of the same owner. Stopping a server or a queue
// seals just its group, retaining the database's independently owned resources.
type Group struct {
 owner *Owner
 parent *Group
 phase atomic.Uint32
 active int
 ctx context.Context
 cancel context.CancelCauseFunc
 done chan struct{}
 err error
}
type groupKey struct{}
type groupContext struct {context.Context;group *Group}
func(c *groupContext) Value(key any) any {if _,ok:=key.(groupKey);ok{return c.group};return c.Context.Value(key)}
func(o *Owner) NewGroup() *Group {
 return o.newGroup(nil)
}
func(o *Owner) newGroup(parent *Group)*Group {
 base:=o.ctx;if parent!=nil{base=parent.ctx}
 ctx,cancel:=context.WithCancelCause(base)
 g:=&Group{owner:o,parent:parent,cancel:cancel,done:make(chan struct{})}
 g.ctx=&groupContext{Context:ctx,group:g};return g
}
// NewGroup creates a restartable task generation under a permanent component.
func(g *Group) NewGroup()*Group{return g.owner.newGroup(g)}
func(g *Group) Context() context.Context {return g.ctx}
func(g *Group) Closing() bool {if g.owner.Closing(){return true};for p:=g;p!=nil;p=p.parent{if phase(p.phase.Load())!=open{return true}};return false}
// Wait joins the currently admitted component tasks without sealing the group.
func(g *Group) Wait() {g.owner.mu.Lock();defer g.owner.mu.Unlock();for g.active!=0{g.owner.changed.Wait()}}
// Go reserves a task before launching it; cancellation never detaches its work.
func(g *Group) Go(fn func(*Scope))bool{
 scope:=new(Scope);if err:=scope.EnterTask(g.ctx,g.owner);err!=nil{return false}
 go func(){defer scope.Finish();fn(scope)}();return true
}
// Cancel starts the same stop transition without waiting for its completion.
func(g *Group) Cancel(){g.startStop(nil)}
func(g *Group) startStop(stop func()error){
 if !g.owner.transition(event{kind:sealGroup,group:g}){return}
 g.cancel(ErrClosed)
 go func(){
  var err error;if stop!=nil{err=stop()}
  g.Wait();g.owner.transition(event{kind:completeGroup,group:g,err:err})
 }()
}
// Stop admits one stopper; all callers join its persisted completion. A caller
// deadline reports an incomplete stop, never a successful early return.
func(g *Group) Stop(ctx context.Context,stop func() error) error {
 g.startStop(stop)
 select {case <-g.done:g.owner.mu.Lock();err:=g.err;g.owner.mu.Unlock();return err;case <-ctx.Done():return ctx.Err()}
}

// Activity counts actual database/resource work, not the surrounding user
// callback. Its fields are written only by its owner's transition reducer.
type Activity struct {owner *Owner;group *Group;resource *Resource;held bool;ended bool;pauses int}

// Resource identifies a provider generation independently of cache/tracing
// aliases. Release waits for its actual calls, including uncancellable children.
type Resource struct {
 owner *Owner
 identity any
 state resourceState
 active int
 release func() error
 done chan struct{}
 err error
}
// Own registers a constructed resource before health checking or publication.
// identity must be comparable, or nil for an unkeyed resource.
func(o *Owner) Own(identity any,release func() error)(*Resource,bool) {
 o.mu.Lock()
 if identity!=nil {if r:=o.identities[identity];r!=nil {accepted:=phase(o.phase.Load())==open&&r.state==resourceLive;o.mu.Unlock();return r,accepted}}
 // Own is the constructor event; keep lookup and publication indivisible.
 r:=&Resource{owner:o,identity:identity,release:release,done:make(chan struct{})}
 accepted:=o.reduce(event{kind:own,resource:r})
 o.mu.Unlock()
 if !accepted {_=r.Release()}
 return r,accepted
}
func(r *Resource) Release() error {
 o:=r.owner;o.transition(event{kind:retireResource,resource:r})
 o.mu.Lock();for r.state==resourceRetiring && r.active!=0 {o.changed.Wait()};o.mu.Unlock()
 if o.transition(event{kind:releaseResource,resource:r}) {
  var err error;if r.release!=nil {err=r.release()}
  o.transition(event{kind:completeResource,resource:r,err:err})
 }
 <-r.done;o.mu.Lock();err:=r.err;o.mu.Unlock();return err
}
