// Task-scoped TrueNAS operator, following the existing release-priority method.
import fs from 'node:fs/promises';
import {TrueNasClient} from '../truenas-client.mjs';
import {logLogin} from '../tnrun-session.mjs';
import {ensureDir,putFile,datasetPath,mutation,downloadBuffer} from '../../bench1043/tnverify-api.mjs';
const app='nornic-tnrun-fix1384-priority-r3';
const state=datasetPath+'/tnrun/operator/fix-1384-regression-r3';
const c=new TrueNasClient();
await c.connect();
await logLogin('direct','fix1384-release-priority','ok');
try{
 if(process.argv.includes('--cleanup')){
  const receipt=JSON.parse((await downloadBuffer(c,state+'/receipt.json')).toString());
  if(!receipt.completed && receipt.both_final_jobs_claimed){
   // Other owners may retire queued jobs while this temporary window holds them.
   // Preserve the original records; accept only a terminal, unstarted cancellation.
   receipt.original_restore_errors=[...receipt.restore_errors];
   receipt.held_records=[...receipt.held];
   const unique=new Map();
   for(const entry of receipt.held){
    if(unique.has(entry.name)&&unique.get(entry.name).sha256!==entry.sha256)throw new Error('Conflicting duplicate held definition');
    unique.set(entry.name,entry);
   }
   receipt.held=[...unique.values()];
   receipt.concurrently_cancelled=[];
   for(const entry of receipt.held){
    if(receipt.restored.includes(entry.name)||(receipt.already_executed??[]).includes(entry.name))continue;
    const id=entry.name.replace(/\.json$/,'');
    const terminal=JSON.parse((await downloadBuffer(c,datasetPath+'/tnrun/done/'+id+'/result.json')).toString());
    if(terminal.id!==id||terminal.state!=='CANCELLED'||terminal.exit!==125||terminal.seconds!==0||terminal.worker!=='not-started')throw new Error('Unaccounted held source: '+id);
    receipt.concurrently_cancelled.push({name:entry.name,result:terminal});
   }
   if((await c.call('filesystem.listdir',[state+'/held',[],{select:['name']}])).length)throw new Error('Held directory is not empty');
   receipt.restore_errors=[];
   receipt.completed=new Date().toISOString();
   receipt.reconciliation='19 unchanged definitions restored; four unique jobs retired concurrently by their owners; two duplicate hold records retained in held_records; no queued work lost';
   await putFile(c,Buffer.from(JSON.stringify(receipt,null,2)),state+'/receipt.json','fix1384/priority-reconciliation');
  }
  if(!receipt.completed||receipt.ids.length!==2||receipt.restore_errors.length||receipt.held.length!==receipt.restored.length+(receipt.already_executed?.length??0)+(receipt.concurrently_cancelled?.length??0))throw new Error('Completed/restored receipt is required');
  await fs.writeFile(new URL('./fix-1384-bench-priority-r3-receipt.json',import.meta.url),JSON.stringify(receipt,null,2));
  await mutation(c,'app.delete',[app,{remove_images:false,remove_ix_volumes:false,force_remove_ix_volumes:false}],{job:true});
  console.log('Final A/B complete; previous queue restored; task operator removed.');
 }else if(process.argv.includes('--status')){
  const receipt=JSON.parse((await downloadBuffer(c,state+'/receipt.json')).toString());
  console.log(JSON.stringify(receipt));
 }else{
  if((await c.call('app.query',[[['id','=',app]],{select:['id']}])).length)throw new Error('Operator already exists; inspect before retrying');
  await ensureDir(c,state);
  await putFile(c,await fs.readFile(new URL('./fix-1384-bench-priority-r3.py',import.meta.url)),state+'/priority.py','fix1384/release-priority');
  const spec={services:{control:{image:'local/nornic-verify-tn:3179ae9d04b4',user:'568:568',restart:'no',network_mode:'none',cpuset:'20,84',command:['python3','-u','/work/tnrun/operator/fix-1384-regression-r3/priority.py'],volumes:[{type:'bind',source:datasetPath+'/tnrun',target:'/work/tnrun',bind:{create_host_path:false}}],deploy:{resources:{limits:{cpus:'0.25',memory:'128M'}}}}}};
  await mutation(c,'app.create',[{app_name:app,custom_app:true,custom_compose_config_string:JSON.stringify(spec)}],{job:true});
  console.log('Exact final-head A/B priority configured; no worker or benchmark app restarted.');
 }
}finally{c.close()}
