package nornicdb_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This provider and storage wrapper use only the worker's exported API. The
// pause gate lets MAIN arrive after a failed attempt without depending on scans.
type dlqPublicProvider struct {
	mu sync.Mutex
	calls []string
	calledAt []time.Time
	poison bool
}
func (p *dlqPublicProvider) Model() string { return "dlq-public" }
func (p *dlqPublicProvider) Dimensions() int { return 2 }
func (p *dlqPublicProvider) Backend() string { return "cpu" }
func (p *dlqPublicProvider) ChunkText(text string, _, _ int) ([]string,error) { return []string{text},nil }
func (p *dlqPublicProvider) Close() error { return nil }
func (p *dlqPublicProvider) Embed(ctx context.Context, text string) ([]float32, error) {
	v, err := p.EmbedBatch(ctx, []string{text}); if err != nil { return nil, err }; return v[0], nil
}
func (p *dlqPublicProvider) EmbedBatch(_ context.Context, texts []string) ([][]float32, error) {
	p.mu.Lock(); defer p.mu.Unlock()
	v := make([][]float32, len(texts))
	for i, text := range texts {
		p.calls = append(p.calls, text)
		p.calledAt=append(p.calledAt,time.Now())
		if p.poison && strings.Contains(text, "poison") { return nil, errors.New("permanent poison") }
		v[i] = []float32{1, 0}
	}
	return v, nil
}
type dlqPublicStore struct {
	storage.Engine
	disk *storage.BadgerEngine
	path string
	mu sync.Mutex
	pending []storage.NodeID
	attempts []storage.NodeID
	reject map[storage.NodeID]bool
	pause *atomic.Bool
	beforeWrite func(*storage.Node)
	beforeFind func()
}
func (s *dlqPublicStore) FindNodeNeedingEmbedding() *storage.Node {
	if s.beforeFind!=nil {s.beforeFind()}
	s.mu.Lock(); defer s.mu.Unlock()
	if len(s.pending)==0 { return nil }; n, _ := s.Engine.GetNode(s.pending[0]); return n
}
func (s *dlqPublicStore) RefreshPendingEmbeddingsIndex() int { return 0 }
func (s *dlqPublicStore) StreamParkedEmbeddingFailures(ctx context.Context, visit func(storage.NodeID,map[string]any)error) (int,error) {
	return s.Engine.(storage.EmbeddingFailureStreamer).StreamParkedEmbeddingFailures(ctx,visit)
}
func (s *dlqPublicStore) MarkNodeEmbedded(id storage.NodeID) {
	s.mu.Lock(); defer s.mu.Unlock()
	for i, v := range s.pending { if v==id { s.pending=append(s.pending[:i],s.pending[i+1:]...); return } }
}
func (s *dlqPublicStore) AddToPendingEmbeddings(id storage.NodeID) {
	s.mu.Lock(); defer s.mu.Unlock()
	for _, v := range s.pending { if v==id { return } }; s.pending=append(s.pending,id)
}
func (s *dlqPublicStore) UpdateNodeEmbeddingSidecar(n *storage.Node) error {
	if s.beforeWrite!=nil { s.beforeWrite(n) }
	s.mu.Lock(); s.attempts=append(s.attempts,n.ID); reject:=s.reject[n.ID]; s.mu.Unlock()
	if reject { if s.pause!=nil { s.pause.Store(true) }; return errors.New("sidecar unavailable") }
	return s.Engine.(storage.EmbeddingSidecarUpdater).UpdateNodeEmbeddingSidecar(n)
}
func dlqPublicFixture(t *testing.T, p *dlqPublicProvider, configure ...func(*nornicdb.EmbedWorkerConfig)) (*nornicdb.EmbedWorker,*dlqPublicStore) {
	t.Helper()
	path:=t.TempDir();base, err := storage.NewBadgerEngine(path); require.NoError(t,err)
	s := &dlqPublicStore{Engine:storage.NewNamespacedEngine(base,"dlq"),disk:base,path:path,reject:make(map[storage.NodeID]bool)}
	cfg:=nornicdb.DefaultEmbedWorkerConfig(); cfg.DeferWorkerStart=true; cfg.NumWorkers=1; cfg.EmbedBatchSize=1
	cfg.MaxRetries=2; cfg.BatchDelay=0; cfg.ScanInterval=time.Hour; cfg.TriggerDebounceDelay=0
	cfg.ProviderRetryBackoff=20*time.Millisecond; cfg.ProviderRetryBackoffMax=40*time.Millisecond
	for _,fn:=range configure {fn(cfg)}
	w:=nornicdb.NewEmbedWorker(p,s,cfg)
	t.Cleanup(func(){w.Close(); require.NoError(t,s.disk.Close())})
	return w,s
}
func dlqPublicAdd(t *testing.T,s *dlqPublicStore,id string) {
	t.Helper(); _,err:=s.CreateNode(&storage.Node{ID:storage.NodeID(id),Properties:map[string]any{"text":id}}); require.NoError(t,err); s.AddToPendingEmbeddings(storage.NodeID(id))
}

func TestEmbedWorkerDLQPublicMainBeforeReadyRetry(t *testing.T) {
	w,s:=dlqPublicFixture(t,&dlqPublicProvider{})
	var paused atomic.Bool; s.pause=&paused; w.SetShouldYield(paused.Load)
	dlqPublicAdd(t,s,"bad"); s.reject["bad"]=true; w.StartWorkers()
	require.Eventually(t,paused.Load,3*time.Second,time.Millisecond)
	dlqPublicAdd(t,s,"healthy")
	// Both the old one-second save backoff and the new per-node backoff are due.
	time.Sleep(1100*time.Millisecond)
	paused.Store(false); w.TriggerImmediate()
	require.Eventually(t,func()bool{return w.Stats().Processed==1},3*time.Second,time.Millisecond)
	s.mu.Lock(); attempts:=append([]storage.NodeID(nil),s.attempts...); s.mu.Unlock()
	require.GreaterOrEqual(t,len(attempts),2)
	require.Equal(t,storage.NodeID("healthy"),attempts[1],"MAIN must precede an already-ready rejected save")
}

func TestEmbedWorkerDLQPublicProviderTerminal(t *testing.T) {
	p:=&dlqPublicProvider{poison:true}; w,s:=dlqPublicFixture(t,p)
	dlqPublicAdd(t,s,"poison"); dlqPublicAdd(t,s,"healthy"); w.StartWorkers()
	require.Eventually(t,func()bool{return w.Stats().Processed==1 && w.Stats().Parked==1},4*time.Second,time.Millisecond)
	p.mu.Lock(); calls:=append([]string(nil),p.calls...); p.mu.Unlock()
	require.Len(t,calls,4,"one initial attempt, two DLQ retries, and one healthy node")
	require.Contains(t,calls[1],"healthy","MAIN drains before poison is retried")
	time.Sleep(150*time.Millisecond)
	p.mu.Lock(); require.Len(t,p.calls,4,"terminal failures are never automatically retried"); p.mu.Unlock()
}

func dlqPublicAttempts(s *dlqPublicStore) []storage.NodeID {
	s.mu.Lock(); defer s.mu.Unlock(); return append([]storage.NodeID(nil),s.attempts...)
}
func TestEmbedWorkerDLQPublicTailAndArrivingMain(t *testing.T) {
	w,s:=dlqPublicFixture(t,&dlqPublicProvider{})
	var paused atomic.Bool; s.pause=&paused; w.SetShouldYield(paused.Load)
	dlqPublicAdd(t,s,"a"); dlqPublicAdd(t,s,"b"); s.reject["a"]=true;s.reject["b"]=true
	// MAIN arrives while the first DLQ write is executing. The next selection
	// must take it before the other ready DLQ entry.
	s.beforeWrite=func(n *storage.Node){
		if n.ID=="a" && len(dlqPublicAttempts(s))==2 {
			_,err:=s.CreateNode(&storage.Node{ID:"arrived",Properties:map[string]any{"text":"arrived"}})
			if err!=nil {panic(err)};s.AddToPendingEmbeddings("arrived");w.Enqueue("arrived")
		}
	}
	w.StartWorkers()
	for _,count:=range []int{1,2,3,5} {
		paused.Store(false);w.TriggerImmediate()
		require.Eventually(t,func()bool{return len(dlqPublicAttempts(s))>=count && paused.Load()},4*time.Second,time.Millisecond)
		if count==2 {time.Sleep(1100*time.Millisecond)} // old and new DLQ timers ready
		if count==3 {time.Sleep(100*time.Millisecond)}
	}
	attempts:=dlqPublicAttempts(s)
	require.Equal(t,[]storage.NodeID{"a","b","a","arrived","b"},attempts[:5],"a requeued at tail; MAIN arriving during a takes precedence over b")
}

func TestEmbedWorkerDLQPublicUpdatePullsMainAndRejectsStale(t *testing.T) {
	p:=&dlqPublicProvider{};w,s:=dlqPublicFixture(t,p)
	var paused atomic.Bool;s.pause=&paused;w.SetShouldYield(paused.Load)
	dlqPublicAdd(t,s,"bad");s.reject["bad"]=true
	w.StartWorkers();require.Eventually(t,paused.Load,3*time.Second,time.Millisecond)
	n,err:=s.GetNode("bad");require.NoError(t,err)
	n.Properties["text"]="updated source";require.NoError(t,s.UpdateNode(n))
	s.mu.Lock();s.reject["bad"]=false;s.mu.Unlock()
	w.Enqueue("bad") // the supported mutation notification clears DLQ metadata
	paused.Store(false);w.TriggerImmediate()
	require.Eventually(t,func()bool{return w.Stats().Processed==1},3*time.Second,time.Millisecond)
	p.mu.Lock();calls:=append([]string(nil),p.calls...);p.mu.Unlock()
	require.Len(t,calls,2,"old retained payload must be invalidated by the update")
	require.Contains(t,calls[1],"updated source")
	stored,err:=s.GetNode("bad");require.NoError(t,err);require.NotEmpty(t,stored.ChunkEmbeddings)
}

func TestEmbedWorkerDLQPublicTerminalSurvivesRestart(t *testing.T) {
	p:=&dlqPublicProvider{poison:true};w,s:=dlqPublicFixture(t,p)
	dlqPublicAdd(t,s,"poison");w.StartWorkers()
	require.Eventually(t,func()bool{return w.Stats().Parked==1},4*time.Second,time.Millisecond)
	w.Close()
	require.NoError(t,s.disk.Close())
	reopened,err:=storage.NewBadgerEngine(s.path);require.NoError(t,err)
	s.disk=reopened;s.Engine=storage.NewNamespacedEngine(reopened,"dlq")
	cfg:=nornicdb.DefaultEmbedWorkerConfig();cfg.DeferWorkerStart=true
	restarted:=nornicdb.NewEmbedWorker(p,s,cfg);defer restarted.Close()
	failures,err:=restarted.ParkedEmbeddingFailures(context.Background(),10)
	require.NoError(t,err);require.Len(t,failures,1);require.Equal(t,storage.NodeID("poison"),failures[0].NodeID)
	retried,err:=restarted.RetryParkedEmbeddingFailures(context.Background(),nil)
	require.NoError(t,err);require.Equal(t,1,retried,"explicit operator retry clears durable terminal metadata")
}

func TestEmbedWorkerDLQPublicBackoffNeverDelaysMain(t *testing.T) {
	p:=&dlqPublicProvider{poison:true}
	w,s:=dlqPublicFixture(t,p,func(c *nornicdb.EmbedWorkerConfig){c.MaxRetries=1;c.ProviderRetryBackoff=400*time.Millisecond;c.ProviderRetryBackoffMax=400*time.Millisecond})
	dlqPublicAdd(t,s,"poison");dlqPublicAdd(t,s,"healthy");w.StartWorkers()
	require.Eventually(t,func()bool{return w.Stats().Processed==1},3*time.Second,time.Millisecond)
	p.mu.Lock();calls:=append([]string(nil),p.calls...);at:=append([]time.Time(nil),p.calledAt...);p.mu.Unlock()
	require.GreaterOrEqual(t,len(calls),2);require.Contains(t,calls[1],"healthy")
	require.Less(t,at[1].Sub(at[0]),400*time.Millisecond,"another MAIN item never waits through the failed node's backoff")
	require.Eventually(t,func()bool{return w.Stats().Parked==1},3*time.Second,time.Millisecond)
	p.mu.Lock();at=append([]time.Time(nil),p.calledAt...);p.mu.Unlock()
	require.Len(t,at,3);require.GreaterOrEqual(t,at[2].Sub(at[0]),400*time.Millisecond)
}

func TestEmbedWorkerDLQPublicUpdateResetsRetryLimit(t *testing.T) {
	w,s:=dlqPublicFixture(t,&dlqPublicProvider{},func(c *nornicdb.EmbedWorkerConfig){c.MaxRetries=1})
	var paused atomic.Bool;s.pause=&paused;w.SetShouldYield(paused.Load)
	dlqPublicAdd(t,s,"bad");s.reject["bad"]=true;w.StartWorkers()
	require.Eventually(t,paused.Load,3*time.Second,time.Millisecond)
	n,err:=s.GetNode("bad");require.NoError(t,err);n.Properties["text"]="new attempt";n.UpdatedAt=time.Now();require.NoError(t,s.UpdateNode(n))
	w.Enqueue("bad");paused.Store(false);w.TriggerImmediate()
	require.Eventually(t,func()bool{return len(dlqPublicAttempts(s))>=2 && paused.Load()},3*time.Second,time.Millisecond)
	s.mu.Lock();s.reject["bad"]=false;s.mu.Unlock();paused.Store(false);w.TriggerImmediate()
	require.Eventually(t,func()bool{return w.Stats().Processed==1},3*time.Second,time.Millisecond)
	require.Zero(t,w.Stats().Parked,"old DLQ failures must not consume the updated node's retry allowance")
}

func TestEmbedWorkerDLQPublicUpdateRejectsInFlightRetainedWrite(t *testing.T) {
	p:=&dlqPublicProvider{};w,s:=dlqPublicFixture(t,p)
	started,release:=make(chan struct{}),make(chan struct{});var once sync.Once
	t.Cleanup(func(){once.Do(func(){close(release)})})
	s.beforeWrite=func(n *storage.Node){if n.ID=="bad" && len(dlqPublicAttempts(s))==1 {close(started);<-release}}
	dlqPublicAdd(t,s,"bad");s.reject["bad"]=true
	// Stop after the initial rejection, then let the already computed payload
	// reach storage before landing the business write.
	var paused atomic.Bool;s.pause=&paused;w.SetShouldYield(paused.Load);w.StartWorkers()
	require.Eventually(t,paused.Load,3*time.Second,time.Millisecond)
	s.mu.Lock();s.reject["bad"]=false;s.mu.Unlock();paused.Store(false);w.TriggerImmediate()
	select {case <-started:case <-time.After(3*time.Second):t.Fatal("retained write never started")}
	n,err:=s.GetNode("bad");require.NoError(t,err);n.Properties["text"]="changed in flight";n.UpdatedAt=time.Now();require.NoError(t,s.UpdateNode(n));w.Enqueue("bad")
	once.Do(func(){close(release)})
	require.Eventually(t,func()bool{return w.Stats().Processed==1},3*time.Second,time.Millisecond)
	p.mu.Lock();calls:=append([]string(nil),p.calls...);p.mu.Unlock();require.Len(t,calls,2);require.Contains(t,calls[1],"changed in flight")
	require.Equal(t,1,w.Stats().Processed,"only the result for the updated body may publish")
}

func TestEmbedWorkerDLQPublicRestartRequeuesVolatilePayload(t *testing.T) {
	p:=&dlqPublicProvider{};w,s:=dlqPublicFixture(t,p)
	var paused atomic.Bool;s.pause=&paused;w.SetShouldYield(paused.Load)
	dlqPublicAdd(t,s,"bad");s.reject["bad"]=true;w.StartWorkers()
	require.Eventually(t,paused.Load,3*time.Second,time.Millisecond)
	w.Close()
	s.mu.Lock();s.reject["bad"]=false;s.mu.Unlock()
	cfg:=nornicdb.DefaultEmbedWorkerConfig();cfg.NumWorkers=1;cfg.BatchDelay=0;cfg.DeferWorkerStart=true;cfg.TriggerDebounceDelay=0
	restarted:=nornicdb.NewEmbedWorker(p,s,cfg);defer restarted.Close();restarted.StartWorkers()
	require.Eventually(t,func()bool{return restarted.Stats().Processed==1},3*time.Second,time.Millisecond)
	p.mu.Lock();defer p.mu.Unlock();require.Len(t,p.calls,2,"payload/backoff are volatile; unfinished work returns to MAIN after restart")
}

type dlqPublicDocument struct{ *dlqPublicProvider }
func(p *dlqPublicDocument) EmbedDocumentChunks(ctx context.Context,text string,size,overlap int)(*embed.DocumentChunkResult,error){
	r,err:=p.EmbedDocumentBatchChunks(ctx,[]string{text},size,overlap);if err!=nil{return nil,err};return r[0],nil
}
func(p *dlqPublicDocument) EmbedDocumentBatchChunks(ctx context.Context,texts []string,_,_ int)([]*embed.DocumentChunkResult,error){
	v,err:=p.EmbedBatch(ctx,texts);if err!=nil{return nil,err};r:=make([]*embed.DocumentChunkResult,len(texts))
	for i,text:=range texts {r[i]=&embed.DocumentChunkResult{Chunks:[]string{text},Embeddings:[][]float32{v[i]},Model:p.Model()}};return r,nil
}
type dlqPublicProperties struct{*dlqPublicDocument}
func(*dlqPublicProperties) UsesDocumentProperties()bool{return true}
func(p *dlqPublicProperties) EmbedDocumentPropertyChunks(ctx context.Context,text string,_ map[string]any,size,overlap int)(*embed.DocumentChunkResult,error){return p.EmbedDocumentChunks(ctx,text,size,overlap)}
type dlqPublicPropertyBatch struct{*dlqPublicProperties}
func(p *dlqPublicPropertyBatch) EmbedDocumentPropertyBatchChunks(ctx context.Context,texts []string,_ []map[string]any,size,overlap int)([]*embed.DocumentChunkResult,error){return p.EmbedDocumentBatchChunks(ctx,texts,size,overlap)}

func TestEmbedWorkerDLQPublicFailureRoutes(t *testing.T){
	for _,route:=range []string{"whole","document-single","document-batch","resolver-single","resolver-batch","property-single","property-batch"}{
		t.Run(route,func(t *testing.T){
			p:=&dlqPublicProvider{poison:true};w,s:=dlqPublicFixture(t,p,func(c *nornicdb.EmbedWorkerConfig){if strings.HasSuffix(route,"batch"){c.EmbedBatchSize=8}})
			doc:=&dlqPublicDocument{p};properties:=&dlqPublicProperties{doc};var provider embed.Embedder=p
			switch route{case "document-single","document-batch","resolver-batch":provider=doc;case "property-single":provider=properties;case "property-batch":provider=&dlqPublicPropertyBatch{properties}}
			w.SetEmbedder(provider)
			if strings.HasPrefix(route,"resolver") || route=="property-batch" {w.SetEmbedderResolver(func(storage.NodeID)(embed.Embedder,error){return provider,nil})}
			dlqPublicAdd(t,s,"poison");dlqPublicAdd(t,s,"healthy");w.StartWorkers()
			require.Eventually(t,func()bool{return w.Stats().Processed==1 && w.Stats().Parked==1},4*time.Second,time.Millisecond)
			n,err:=s.GetNode("healthy");require.NoError(t,err);require.NotEmpty(t,n.ChunkEmbeddings)
			failures,err:=w.ParkedEmbeddingFailures(context.Background(),10);require.NoError(t,err);require.Len(t,failures,1)
		})
	}
}

func TestEmbedWorkerDLQPublicEnqueueDoesNotWaitForClaim(t *testing.T) {
	w,s:=dlqPublicFixture(t,&dlqPublicProvider{})
	started,release:=make(chan struct{}),make(chan struct{})
	var once,releaseOnce sync.Once
	t.Cleanup(func(){releaseOnce.Do(func(){close(release)})})
	s.beforeFind=func(){once.Do(func(){close(started);<-release})}
	w.StartWorkers()
	select{case <-started:case <-time.After(3*time.Second):t.Fatal("no storage claim")}
	dlqPublicAdd(t,s,"arriving")
	done:=make(chan struct{});go func(){w.Enqueue("arriving");close(done)}()
	select{case <-done:case <-time.After(time.Second):t.Fatal("enqueue waited for the empty claim it must wake")}
	releaseOnce.Do(func(){close(release)})
	require.Eventually(t,func()bool{return w.Stats().Processed==1},3*time.Second,time.Millisecond)
}
