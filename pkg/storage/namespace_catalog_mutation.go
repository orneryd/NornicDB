package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/util"
)

// NamespaceCatalogMutation changes one committed database entry. The persisted
// record remains a full catalog; the journal carries this namespace's mutation.
type NamespaceCatalogMutation struct {
	Namespace string
	Record *Node
	Expected json.RawMessage
	Value json.RawMessage
	Create bool
	Bootstrap bool
	Birth uint64
	BirthAssigned bool
}

// PublishNamespaceCatalogContext dispatches before unwrapping so native WAL and
// replication retain their admission and journal ownership.
func PublishNamespaceCatalogContext(ctx context.Context, engine Engine, mutation NamespaceCatalogMutation) error {
	if writer,ok:=engine.(interface{PublishNamespaceCatalogContext(context.Context,NamespaceCatalogMutation)error});ok{return writer.PublishNamespaceCatalogContext(ctx,mutation)}
	if wrapper,ok:=engine.(EngineUnwrapper);ok{return PublishNamespaceCatalogContext(ctx,wrapper.GetInnerEngine(),mutation)}
	return ErrNotImplemented
}

func (op *NamespaceDrop) SetCatalogMutation(mutation NamespaceCatalogMutation) {
	op.catalogMutation=&mutation
	if mutation.BirthAssigned {op.SetCatalogRevision(mutation.Birth)}
}

func (op *NamespaceDrop) dropsNamespace()bool{return op.catalogMutation==nil || len(op.catalogMutation.Value)==0}

func (op *NamespaceDrop) fencesNamespace()bool{
	if op.catalogMutation==nil || !op.catalogMutation.Bootstrap{return true}
	namespace,_,_:=ParseDatabasePrefix(string(op.catalogMutation.Record.ID))
	return namespace!=op.namespace
}

func (b *BadgerEngine) PublishNamespaceCatalogContext(ctx context.Context, mutation NamespaceCatalogMutation) error {
	if mutation.Namespace=="" || mutation.Record==nil || mutation.Record.ID==""{return ErrInvalidData}
	op:=NamespaceDropFromContext(ctx)
	if op==nil{op=NewNamespaceDrop(nil);ctx=WithNamespaceDrop(ctx,op)}
	op.SetCatalogMutation(mutation)
	if op.engine==nil {if err:=b.PrepareNamespaceDrop(ctx,mutation.Namespace+":",op);err!=nil{return err}}
	if op.phase.Load()==dropProposed {ctx=context.WithoutCancel(ctx)}
	release,err:=b.beginSchemaWriteContext(ctx)
	if err!=nil{op.Abort();return err}
	err=op.validateCatalogMutation()
	if err==nil {err=op.commitLocked(ctx)}
	release()
	if !op.Committed(){op.Abort();return err}
	return op.physicalComplete(ctx,err)
}

type namespaceCatalogDocument struct {
	node *Node
	entries map[string]json.RawMessage
	births map[string]uint64
}

func (op *NamespaceDrop) readCatalogDocument()(namespaceCatalogDocument,error){
	mutation:=op.catalogMutation
	document:=namespaceCatalogDocument{entries:make(map[string]json.RawMessage),births:make(map[string]uint64)}
	node,err:=op.engine.GetNode(mutation.Record.ID)
	if err!=nil && err!=ErrNotFound{return document,err}
	if err==ErrNotFound{document.node=copyNode(mutation.Record);return document,nil}
	document.node=node
	if data,ok:=node.Properties["data"].(string);ok{if err:=json.Unmarshal([]byte(data),&document.entries);err!=nil{return document,err}}
	if data,ok:=node.Properties["namespace_lifetimes"].(string);ok{if err:=json.Unmarshal([]byte(data),&document.births);err!=nil{return document,err}}
	return document,nil
}

func(op *NamespaceDrop)validateCatalogMutation()error{
	if op.catalogMutation==nil{return nil}
	document,err:=op.readCatalogDocument();if err!=nil{return err}
	mutation:=op.catalogMutation
	current:=document.entries[mutation.Namespace]
	if NamespaceCatalogRetired(op.engine,mutation.Namespace,document.births[mutation.Namespace]){current=nil}
	if mutation.Create && len(current)!=0{
		if mutation.Bootstrap{return nil}
		return fmt.Errorf("%w: database %q already exists",ErrAlreadyExists,mutation.Namespace)
	}
	if !mutation.Create && len(current)==0 && len(mutation.Expected)!=0{return fmt.Errorf("%w: database %q no longer exists",ErrConflict,mutation.Namespace)}
	if len(mutation.Expected)!=0 && !bytes.Equal(current,mutation.Expected){return fmt.Errorf("%w: database %q changed before catalog publication",ErrConflict,mutation.Namespace)}
	return nil
}

// Only the actual shared-record RMW is serialized. Snapshot admission and
// journal/consensus waits precede it; manager readers never acquire this gate.
func(op *NamespaceDrop)applyCatalogMutation()error{
	if op.catalogMutation==nil{return nil}
	b:=op.engine
	value,_:=b.catalogRecordGates.LoadOrStore(op.catalogMutation.Record.ID,newCatalogRecordGate())
	gate:=value.(chan struct{})
	<-gate
	defer func(){gate<-struct{}{}}()
	document,err:=op.readCatalogDocument();if err!=nil{return err}
	mutation:=op.catalogMutation
	if mutation.Create && mutation.Bootstrap && len(document.entries[mutation.Namespace])!=0 {b.publishNamespaceCatalog(document.node);return nil}
	if len(mutation.Value)==0{
		delete(document.entries,mutation.Namespace)
		delete(document.births,mutation.Namespace)
	}else{
		document.entries[mutation.Namespace]=mutation.Value
		document.births[mutation.Namespace]=op.catalogRevision
	}
	data,err:=json.Marshal(document.entries);if err!=nil{return err}
	births,err:=json.Marshal(document.births);if err!=nil{return err}
	if document.node.Properties==nil{document.node.Properties=make(map[string]any)}
	document.node.Properties["data"]=string(data)
	document.node.Properties["namespace_lifetimes"]=string(births)
	for key,value:=range mutation.Record.Properties{if key=="updated_at" || document.node.Properties[key]==nil{document.node.Properties[key]=value}}
	if _,err:=b.GetNode(document.node.ID);err==ErrNotFound{_,err=b.CreateNode(document.node);return err}else if err!=nil{return err}
	return b.UpdateNode(document.node)
}

func newCatalogRecordGate()chan struct{} {gate:=make(chan struct{},1);gate<-struct{}{};return gate}

// ValidateNamespaceCommandAdmission prevents rejected fenced writes from being
// journaled and later replayed into the dropped namespace as ghost writes.
func ValidateNamespaceCommandAdmission(engine Engine,namespaces []string)error{
	if owner:=namespaceLifecycleOwner(engine);owner!=nil{for _,namespace:=range namespaces{if err:=owner.checkNamespaceOperation(namespace);err!=nil{return err}}}
	return nil
}

func (w *WALEngine) PublishNamespaceCatalogContext(ctx context.Context, mutation NamespaceCatalogMutation)error{
	op:=NamespaceDropFromContext(ctx)
	if op==nil{op=NewNamespaceDrop(nil);ctx=WithNamespaceDrop(ctx,op)}
	op.SetCatalogMutation(mutation)
	release,err:=util.LockContext(ctx,w.mutationMu.RLock,w.mutationMu.RUnlock);if err!=nil{return err}
	err=PublishNamespaceCatalogContext(context.WithValue(ctx,deferredDropCompletionKey{},true),w.engine,mutation)
	release()
	if op.physicalDone && !op.consensus && op.phase.Load()!=dropCompleted {err=op.finish(err)}
	return err
}
