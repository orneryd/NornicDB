package lifecycle

import (
 "context"
 "sync"
)

type scopeKey struct{}

// Scope adds lifetime to an existing request/statement context. Embed it in an
// already allocated statement context to keep ordinary admission allocation-free.
type Scope struct {
 context.Context
 token Activity
 activity *Activity
 parent *Scope
 root bool
 linked context.Context
 cancel context.CancelFunc
 unlink func() bool
 doneOnce sync.Once
 callbacks []func()
}

// Enter counts an operation and records its synchronous caller chain. A
// callback can suspend that chain without suspending independent sibling work.
func(s *Scope) Enter(ctx context.Context,o *Owner) error {
 s.Context=ctx
 if o==nil {return ctx.Err()}
 err:=s.enter(ctx,o,nil)
 if parent,ok:=ctx.Value(scopeKey{}).(*Scope);ok&&parent.activity!=nil&&parent.activity.owner==o{s.parent=parent}
 return err
}
func(s *Scope) enter(ctx context.Context,o *Owner,r *Resource) error {
 s.Context=ctx
 if err:=ctx.Err();err!=nil {return Error(ctx,err)}
 s.token=Activity{owner:o,resource:r}
 if g,ok:=ctx.Value(groupKey{}).(*Group);ok&&g.owner==o {s.token.group=g}
 s.activity=&s.token;s.root=true
 if !o.transition(event{kind:admit,activity:s.activity}) {s.root=false;return ErrClosed}
 return nil
}
func(s *Scope) EnterTask(ctx context.Context,o *Owner) error {return s.enter(ctx,o,nil)}
// EnterGroup admits a request to a server/transport group on the same DB owner.
func(s *Scope) EnterGroup(ctx context.Context,g *Group)error{
 s.Context=ctx
	 if g==nil{return ctx.Err()}
 if err:=ctx.Err();err!=nil{return err}
 s.token=Activity{owner:g.owner,group:g};s.activity=&s.token;s.root=true
 if !g.owner.transition(event{kind:admit,activity:s.activity}){s.root=false;return ErrClosed}
 if parent,ok:=ctx.Value(scopeKey{}).(*Scope);ok&&parent.activity!=nil&&parent.activity.owner==g.owner{s.parent=parent}
 return nil
}
func(r *Resource) Enter(ctx context.Context,s *Scope) error {return s.enter(ctx,r.owner,r)}
func(s *Scope) Check() error {
 if s.activity!=nil && (s.activity.owner.Closing() || (s.activity.group!=nil&&s.activity.group.Closing())) {return ErrClosed}
 return Error(s.Context,s.Context.Err())
}
func(s *Scope) Value(key any) any {
 if _,ok:=key.(scopeKey);ok {return s}
 if _,ok:=key.(groupKey);ok&&s.activity!=nil&&s.activity.group!=nil{return s.activity.group}
 return s.Context.Value(key)
}
func(s *Scope) Err() error {if s.Check()!=nil {if s.Context.Err()!=nil{return s.Context.Err()};return context.Canceled};return nil}
func(s *Scope) Done() <-chan struct{} {
 if s.activity==nil {return s.Context.Done()}
 cancelCtx:=s.activity.owner.ctx
 if s.activity.group!=nil{cancelCtx=s.activity.group.ctx}
 if s.Context.Done()==nil {return cancelCtx.Done()}
 s.doneOnce.Do(func(){
  s.linked,s.cancel=context.WithCancel(s.Context)
  s.unlink=context.AfterFunc(cancelCtx,s.cancel)
 })
 if s.linked==nil {return s.Context.Done()}
 return s.linked.Done()
}
func(s *Scope) Finish() {
 s.doneOnce.Do(func(){})
 if s.unlink!=nil {s.unlink();s.cancel()}
 if s.root {s.activity.owner.transition(event{kind:leave,activity:s.activity});s.root=false}
}
// Callback suspends the synchronous caller's admission only while user code is
// running. After Close, no subsequent database work can regain that admission.
func(s *Scope) Callback(fn func()) (err error) {
 if s.activity==nil {fn();return nil}
 for p:=s;p!=nil;p=p.parent {p.activity.owner.transition(event{kind:suspend,activity:p.activity})}
 defer func(){for p:=s;p!=nil;p=p.parent {if !p.activity.owner.transition(event{kind:resume,activity:p.activity})&&err==nil{err=ErrClosed}}}()
 fn()
 return s.Check()
}
// Callback runs an application-controlled callback outside its caller's DB work.
func Callback(ctx context.Context,fn func()) error {
 if s,ok:=ctx.Value(scopeKey{}).(*Scope);ok{return s.Callback(fn)}
 fn();return nil
}
// DeferCallback runs a notification after the producer's current DB work ends.
// Callback still supplies the suspension/resumption boundary for re-entry.
func(s *Scope) DeferCallback(fn func()){s.callbacks=append(s.callbacks,fn)}
func(s *Scope) FlushCallbacks()error{
 callbacks:=s.callbacks;s.callbacks=nil
 for _,fn:=range callbacks{if err:=s.Callback(fn);err!=nil{return err}}
 return s.Check()
}
// Error preserves shutdown identity across fallback and protocol boundaries.
// Caller cancellation in an open owner retains its original meaning.
func Error(ctx context.Context,err error) error {
 if err==nil {return nil}
 if s,ok:=ctx.Value(scopeKey{}).(*Scope);ok&&s.activity!=nil {
  if s.activity.owner.Closing()||(s.activity.group!=nil&&s.activity.group.Closing()) {return ErrClosed}
 }
 if context.Cause(ctx)==ErrClosed {return ErrClosed}
 return err
}
