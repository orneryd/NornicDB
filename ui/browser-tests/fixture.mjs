export async function fixture(page) {
 const users=[{username:'alex',email:'alex@example.test',roles:['admin'],disabled:false,last_login:'2026-01-01T12:00:00Z'},{username:'sam',email:'sam@example.test',roles:['viewer'],disabled:false}];
 const databases=[{name:'catalog',status:'online',default:true,type:'standard',nodeCount:12840,edgeCount:56210},{name:'research',status:'online',default:false,type:'standard',nodeCount:320,edgeCount:840}];
 const node={id:'record-1',labels:['Document'],properties:{title:'Getting started',published:true,revision:3},created_at:''};
 const state={users,databases,node,updates:[],failSave:false,requests:[],queries:[],demoDatabases:new Map(),queryGraph:false,failSeed:false,seedBatches:0,failSeedAfter:Infinity,seedDelay:0,roles:['admin'],tokens:[],operations:[],failOperation:false,denySchema:false,policyWrites:[],failBinding:false,
 schema:{indexes:[{name:'document_title',type:'RANGE',labelsOrTypes:['Document'],properties:['title']}],constraints:[{name:'document_id',type:'UNIQUENESS',labelsOrTypes:['Document'],properties:['id']}]},profiles:[{kind:'bundle',Name:'memory_retention',HalfLifeSeconds:604800,VisibilityThreshold:0.1,ScoreFloor:0,Function:'exponential',Scope:'NODE',DecayEnabled:true,ScoreFrom:'VERSION',Enabled:true}],bindings:[]};
 await page.route('**/*',async route=>{
  const request=route.request(),url=new URL(request.url()),p=url.pathname.replace(/\/$/,'') || '/';
  if(!url.hostname.match(/^(127\.0\.0\.1|localhost)$/))return route.abort();
  let data,status=200;
  if(p==='/' && !request.isNavigationRequest())data={default_database:'catalog',bolt_enabled:false};
  else if(p==='/auth/config')data={securityEnabled:true,devLoginEnabled:true,oauthProviders:[]};
  else if(p==='/auth/me')data={username:'alex',roles:state.roles,email:'alex@example.test',auth_method:'password'};
  else if(p==='/auth/api-token'){state.tokens.push(request.postDataJSON());data={token:'fixture-personal-token',subject:request.postDataJSON().subject,roles:state.roles,expires_at:'2027-01-01T00:00:00Z'};}
  else if(p==='/admin/backup'||p==='/admin/restore'){state.operations.push({path:p,body:request.postDataJSON()});if(state.failOperation){status=503;data={message:'Backup storage unavailable'};}else data={status:p.endsWith('restore')?'restore complete':'backup complete',path:request.postDataJSON().path};}
  else if(p==='/auth/roles')data=['admin','editor','viewer'];
  else if(p==='/auth/users')data=users;
  else if(p.startsWith('/auth/users/') && request.method()==='PUT'){
   const changes=request.postDataJSON();state.updates.push(changes);
   if(state.failSave){data={message:'Save failed. Try again.'};status=503}else{Object.assign(users.find(u=>u.username===decodeURIComponent(p.split('/').at(-1))),changes);data={success:true};}
  }
  else if(p==='/status')data={database:{nodes:13160,edges:57050},server:{version:'1.4.4',uptime_seconds:5400}};
  else if(p.match(/^\/db\/[^/]+$/))data=databases.find(db=>db.name===decodeURIComponent(p.split('/').at(-1))) ?? {name:decodeURIComponent(p.split('/').at(-1)),nodeCount:0,edgeCount:0};
  else if(p.endsWith('/tx/commit')){
   const query=request.postDataJSON()?.statements?.[0]?.statement || '';
   state.queries.push(request.postDataJSON());
   const parameters=request.postDataJSON().statements[0].parameters||{};
   const database=decodeURIComponent(p.split('/')[2]);
   const demo=state.demoDatabases.get(database);
   const records=rows=>{const columns=Object.keys(rows[0]??{});return {results:[{columns,data:rows.map(row=>({row:columns.map(column=>row[column])}))}],errors:[]};};
   if(query==='SHOW INDEXES'||query==='SHOW CONSTRAINTS')data=records(state.schema[query.endsWith('INDEXES')?'indexes':'constraints']);
   else if(/^CREATE (INDEX|CONSTRAINT) /.test(query)){
    if(state.denySchema)data={results:[],errors:[{message:'Schema permission denied'}]};
    else{const name=((match)=>String(match[1]??match[2]).replace(/``/g,'`'))(query.match(/^CREATE (?:INDEX|CONSTRAINT) (?:`((?:``|[^`])+)`|(\w+))/));state.schema[query.startsWith('CREATE INDEX')?'indexes':'constraints'].push({name,type:query.startsWith('CREATE INDEX')?'RANGE':'UNIQUENESS',labelsOrTypes:['Person'],properties:['email']});data={results:[],errors:[]};}
   }else if(/^DROP (INDEX|CONSTRAINT) /.test(query)){
    const kind=query.startsWith('DROP INDEX')?'indexes':'constraints',name=query.match(/^DROP (?:INDEX|CONSTRAINT) `((?:``|[^`])+)`/)[1].replace(/``/g,'`');state.schema[kind]=state.schema[kind].filter(row=>row.name!==name);data={results:[],errors:[]};
   }else if(query==='CALL db.labels()')data=records(['Document','MemoryEpisode','Person'].map(label=>({label})));
   else if(query==='CALL db.relationshipTypes()')data=records(['LINKS_TO','SUPPORTS'].map(relationshipType=>({relationshipType})));
   else if(query==='CALL kp.profiles()')data=records([...state.profiles,...state.bindings]);
   else if(query==='CALL kp.info()')data=records([{enabled:true}]);
   else if(query==='CALL kp.policies()'||query==='CALL kp.deindexStatus()')data=records([]);
   else if(query.startsWith('CALL kp.resolve'))data=records([{TargetID:'record-1',TargetScope:'NODE',ResolvedDecayProfileID:'memory_retention',FinalScore:0.5,BaseScore:0.5,EffectiveThreshold:0.1,EffectiveMultiplier:1,NoDecay:false,SuppressionEligible:false,Explanation:'Resolved from the matched label binding.'}]);
   else if(query.startsWith('CREATE DECAY PROFILE')){
    state.policyWrites.push(query);
    if(query.includes(' FOR ')){
     if(state.failBinding)data={results:[],errors:[{message:'Binding rejected by server'}]};
     else{state.bindings.push({kind:'binding',Name:query.match(/^CREATE DECAY PROFILE ([^ ]+)/)[1],TargetLabels:['MemoryEpisode'],IsEdge:false,IsWildcard:false,ProfileRef:'memory_retention',Apply:'DECAY PROFILE memory_retention'});data={results:[],errors:[]};}
    }else{state.profiles.push({kind:'bundle',Name:query.match(/^CREATE DECAY PROFILE ([^ ]+)/)[1],HalfLifeSeconds:Number(query.match(/halfLifeSeconds: (-?[\d.]+)/)[1]),VisibilityThreshold:Number(query.match(/visibilityThreshold: ([\d.]+)/)[1]),ScoreFloor:Number(query.match(/scoreFloor: ([\d.]+)/)[1]),Function:query.match(/function: '([^']+)'/)[1],Scope:query.match(/scope: '([^']+)'/)[1],ScoreFrom:query.match(/scoreFrom: '([^']+)'/)[1],DecayEnabled:true,Enabled:true});data={results:[],errors:[]};}
   }else if(query.includes('AS id LIMIT 10'))data=records([{id:'record-1'}]);
   else if(query.startsWith('CREATE DATABASE')){
    const name=query.match(/CREATE DATABASE `?([^` ]+)/)[1];
    state.demoDatabases.set(name,{kind:null,ready:false,sales:new Map()});data={results:[],errors:[]};
   }else if(query.includes('MERGE (m:DemoDataset')){
    if(demo){demo.kind=parameters.kind;demo.ready=false;}data={results:[],errors:[]};
   }else if(query.includes('SET m.ready=true')){
    if(demo)demo.ready=true;data={results:[],errors:[]};
   }else if(query.includes('RETURN m.kind AS kind')){
    data={results:[{columns:['kind','ready','version'],data:demo?[{row:[demo.kind,demo.ready,1]}]:[]}],errors:[]};
   }else if(query.includes('UNWIND $rows AS row')){
    if(state.seedDelay)await new Promise(resolve=>setTimeout(resolve,state.seedDelay));
    if(state.failSeed||state.seedBatches>=state.failSeedAfter){status=503;data={message:'Seed request failed'};}
    else{state.seedBatches++;if(demo&&query.includes('MERGE (sale:DemoSale'))for(const sale of parameters.rows)demo.sales.set(sale.orderId,sale);data={results:[],errors:[]};}
   }else if(query.includes('WHERE sale.')&&query.includes('LIMIT 100')){
    const dimension=query.match(/WHERE sale\.(\w+)=/)[1];
    const orders=[...(demo?.sales.values()||[])].filter(sale=>sale[dimension]===parameters.group).sort((a,b)=>a.orderId.localeCompare(b.orderId)).slice(0,100);
    const columns=['orderId','account','product','quarter','channel','revenue','units'];
    data={results:[{columns,data:orders.map(sale=>({row:columns.map(column=>sale[column])}))}],errors:[]};
   }else if(query.includes('MATCH (sale:DemoSale)')){
    const records=[...(demo?.sales.values()||[])];
    const row=query.match(/sale\.(\w+) AS rowKey/)[1],column=query.match(/sale\.(\w+) AS columnKey/)[1];
    const field=query.match(/sum\(sale\.(\w+)\)/)?.[1];
    const groups=new Map();for(const record of records){const key=JSON.stringify([record[row],record[column]]);const g=groups.get(key)||[record[row],record[column],0,0,0,0];g[2]+=field?record[field]:1;g[3]++;g[4]+=record.revenue;g[5]+=record.units;groups.set(key,g);}
    data={results:[{columns:['rowKey','columnKey','amount','records','revenue','units'],data:[...groups.values()].map(row=>({row}))}],errors:[]};
   }else{
   data=state.queryGraph&&!query.includes('SHOW DATABASES')?{results:[{columns:['n','r','m'],data:[{row:[{elementId:'4:catalog:record-1',labels:node.labels,properties:node.properties},{elementId:'5:catalog:edge-1',type:'LINKS_TO',startNodeElementId:'4:catalog:record-1',endNodeElementId:'4:catalog:record-2',properties:{weight:2}},{elementId:'4:catalog:record-2',labels:['Guide'],properties:{title:'Vector indexing'}}]}]}],errors:[]}:query.includes('SHOW DATABASES') ? {results:[{columns:['name'],data:databases.map(db=>({row:[db.name]}))}],errors:[]} : {results:[{columns:['n'],data:[{row:[{elementId:'record-1',labels:node.labels,properties:node.properties}],meta:[]}]}],errors:[]};
   }
  }
  else if(p==='/nornicdb/embed/stats')data={enabled:true,total_embeddings:12800,pending_nodes:40,stats:{running:false,processed:120,failed:0}};
  else if(p==='/nornicdb/embed/trigger'){state.requests.push(url);data={accepted:true,database:url.searchParams.get('database'),message:'Regeneration accepted'};status=202;}
  else if(p.endsWith('/neighborhood')){const id=request.postDataJSON()?.node_ids?.[0]||node.id;data={nodes:[id===node.id?node:{...node,id,properties:{title:'Vector indexing'}}],edges:[],meta:{database:'catalog',node_count:1,edge_count:0,truncated:false}};}
  else if(p.endsWith('/mvcc/status'))data={database:'catalog',enabled:true,running:true,automatic:true,paused:false,cycle_interval:'30s',pressure_band:'normal',snapshot_readers:[]};
  else if(p.endsWith('/mvcc/debt'))data={keys:[]};
  else if(p==='/admin/retention/status')data={enabled:true,policy_count:1,hold_count:0,erasure_count:0};
  else if(p==='/admin/retention/policies')data=[{id:'documents',name:'Documents',description:'Retain published documentation for one year.',category:'USER',retention_period:{Duration:31536000000000000,Indefinite:false},archive_before_delete:false,active:true}];
  else if(p==='/admin/retention/holds' || p==='/admin/retention/erasures')data=[];
  else if(p.startsWith('/auth/access/') || p==='/auth/entitlements' || p==='/auth/role-entitlements')data=[];
  else if(p.includes('/config/keys'))data=[];
  else if(p.includes('/config'))data={effective:{},overrides:{}};
  else if(/^\/(auth|admin|nornicdb|db|api)(\/|$)/.test(p) && !request.isNavigationRequest()){status=404;data={message:'Fixture endpoint not configured: '+p};}
  else return route.continue();
  return route.fulfill({status,contentType:'application/json',body:JSON.stringify(data)});
 });
 return state;
}
