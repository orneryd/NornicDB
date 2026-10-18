$ErrorActionPreference='Stop'
$wt='wt-rc144-embed-lifecycle-fix'
$p="$wt/pkg/nornicdb/embed_queue.go"
$s=(Get-Content -LiteralPath $p -Raw).Replace("`r`n","`n")
$s=$s.Replace('"github.com/orneryd/nornicdb/pkg/localization"','"github.com/orneryd/nornicdb/pkg/localization"'+"`n"+' "github.com/orneryd/nornicdb/pkg/lifecycle"')
$s=$s.Replace('wg          sync.WaitGroup','owner *lifecycle.Owner'+"`n"+' queueGroup *lifecycle.Group'+"`n"+' workers *lifecycle.Group')
$s=[regex]::Replace($s,'\s+closed\s+atomic.Bool[^\n]*','')
$s=$s.Replace('onEmbedded func(node *storage.Node)','onEmbedded func(node *storage.Node)'+"`n"+' onEmbeddedInternal func(node *storage.Node)')
$s=$s.Replace('type EmbedWorkerConfig struct {','type EmbedWorkerConfig struct {'+"`n"+' // Lifetime attaches worker generations and inference children to the DB owner.'+"`n"+' Lifetime *lifecycle.Owner')
$s=$s.Replace('ctx, cancel := context.WithCancel(context.Background())','owner:=config.Lifetime;if owner==nil {owner=lifecycle.New()}'+"`n"+' queueGroup:=owner.NewGroup();workers:=queueGroup.NewGroup();ctx:=workers.Context();cancel:=workers.Cancel')
$s=$s.Replace('embedder:          embedder,','embedder:          embedder,'+"`n"+' owner:owner,queueGroup:queueGroup,workers:workers,')
$s=[regex]::Replace($s,'ew\.wg.Add\(1\)\s+go ew.worker\(\)','ew.workers.Go(func(scope *lifecycle.Scope){ew.worker(scope)})')
$s=$s.Replace('ew.wg.Wait()','_ = ew.workers.Stop(context.Background(),nil)')
$s=$s.Replace('ew.closed.Load()','ew.isClosed()')
$s=$s.Replace('ew.closed.Store(true)','')
$s=$s.Replace('ew.ctx, ew.cancel = context.WithCancel(context.Background())','ew.workers=ew.queueGroup.NewGroup();ew.ctx=ew.workers.Context();ew.cancel=ew.workers.Cancel')
$s=$s.Replace('func (ew *EmbedWorker) worker() {'+"`n"+"`tdefer ew.wg.Done()",'func (ew *EmbedWorker) worker(scopes ...*lifecycle.Scope) {'+"`n"+' scope:=queueScope(scopes);if scope==nil {scope=new(lifecycle.Scope);if err:=scope.EnterTask(ew.ctx,ew.owner);err!=nil{return}};defer scope.Finish();scopes=[]*lifecycle.Scope{scope}')
$names=@('processUntilEmpty','processNextBatch','processNextResolvedBatch','processNextNode','processClaimedNode','processNextDocumentBatch','persistEmbeddedNode')
foreach($name in $names){
 $pattern='func \(ew \*EmbedWorker\) '+$name+'\(([^\n]*)\)'
 $s=[regex]::Replace($s,$pattern,{param($m) $argsText=$m.Groups[1].Value; if($argsText){$argsText+=', '};'func (ew *EmbedWorker) '+$name+'('+$argsText+'scopes ...*lifecycle.Scope)'})
 # All call expressions in this file are single-line and have no nested call
 # arguments except documentResultMeta; handle those by matching to line end.
 $pattern='ew\.'+$name+'\(([^\n]*)\)'
 $s=[regex]::Replace($s,$pattern,{param($m) $argsText=$m.Groups[1].Value; if($argsText){$argsText+=', '};'ew.'+$name+'('+$argsText+'scopes...)'})
}
$s=$s.Replace('didWork := ew.processNextBatch(scopes...)','didWork := ew.processNextBatch(scopes...)'+"`n"+' if scope:=queueScope(scopes);scope!=nil {if err:=scope.FlushCallbacks();err!=nil{return}}')
$s=$s.Replace("`t`t`tew.processNextBatch(scopes...)","`t`t`tew.processNextBatch(scopes...)`n"+' if err:=scope.FlushCallbacks();err!=nil{return}')
$s=$s.Replace('if ew.onEmbedded != nil {'+"`n"+"`t`tew.onEmbedded(node)`n`t}",'if ew.onEmbeddedInternal!=nil {ew.onEmbeddedInternal(node)}'+"`n"+' if cb:=ew.onEmbedded;cb!=nil {if scope:=queueScope(scopes);scope!=nil {scope.DeferCallback(func(){cb(node)})}else{cb(node)}}')
$s=$s.Replace('if shouldYield != nil && shouldYield() {','yield:=false'+"`n"+' if shouldYield!=nil {if scope:=queueScope(scopes);scope!=nil {if err:=scope.Callback(func(){yield=shouldYield()});err!=nil{return false}}else{yield=shouldYield()}}'+"`n"+' if yield {')
# Close seals the permanent component. Reset seals only a worker generation.
$start=$s.IndexOf('func (ew *EmbedWorker) Close()')
$end=$s.IndexOf('// scheduleClusteringDebounced',$start)
$part=$s.Substring($start,$end-$start).Replace('ew.cancel()','ew.queueGroup.Cancel()').Replace('_ = ew.workers.Stop(context.Background(),nil)','_ = ew.queueGroup.Stop(context.Background(),nil)')
$s=$s.Substring(0,$start)+$part+$s.Substring($end)
# Reserve the actual inference child before it is launched, not its waiting parent.
$s=$s.Replace('resultCh := make(chan embedResult, 1)'+"`n"+"`t`tgo func() {",'resultCh := make(chan embedResult, 1)'+"`n"+' child:=new(lifecycle.Scope);if err:=child.EnterTask(ew.ctx,ew.owner);err!=nil{return nil,err}'+"`n"+"`t`tgo func() {`n"+' defer child.Finish()')
$s=$s.Replace('typed.EmbedBatchWithInputType(ew.ctx, chunks, embed.InputTypeDocument)','typed.EmbedBatchWithInputType(child, chunks, embed.InputTypeDocument)')
$s=$s.Replace('provider.EmbedBatch(ew.ctx, chunks)','provider.EmbedBatch(child, chunks)')
Set-Content -LiteralPath $p -Value $s -NoNewline
