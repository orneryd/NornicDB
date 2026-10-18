// Read-only TrueNAS queue diagnostic using the existing tnrun broker.
import {SessionClient} from '../tnrun-session.mjs';
import {datasetPath,downloadBuffer} from '../../bench1043/tnverify-api.mjs';
const client=await SessionClient.open(async()=>{throw new Error('Existing tnrun broker is required')});
const root=datasetPath+'/tnrun';
try {
 const running=await client.call('filesystem.listdir',[root+'/running/bench',[],{select:['name']}]);
 for(const entry of running){
  const job=JSON.parse((await downloadBuffer(client,root+'/running/bench/'+entry.name)).toString());
  console.log(JSON.stringify({running:job.id,label:job.label,rounds:job.rounds,args:job.binArgs}));
  try{const log=(await downloadBuffer(client,root+'/done/'+job.id+'/out.txt')).toString();console.log(log.split(/\r?\n/).slice(-5).join('\n')); const rounds=[...log.matchAll(/^=== round (\d+) (base|head)$/gm)]; const last=rounds.at(-1); if(last){try{const partial=(await downloadBuffer(client,root+'/done/'+job.id+'/'+last[2]+'.r'+last[1])).toString();console.log(partial.split(/\r?\n/).slice(-4).map(l=>l.length>500?l.slice(0,500):l).join('\n'))}catch{}} }catch(e){console.log('No partial log: '+e.message)}
 }
 const pending=await client.call('filesystem.listdir',[root+'/queue/ab',[],{select:['name']}]);
 console.log('Pending A/B jobs: '+pending.length);
 try{const r=JSON.parse((await downloadBuffer(client,root+'/operator/fix-1384-regression-r4/receipt.json')).toString());console.log(JSON.stringify({held:r.held.length,restored:r.restored.length,retired:r.retired,completed:r.completed,restore_errors:r.restore_errors}))}catch(e){console.log('Priority receipt: '+e.message)}
} finally {client.close()}
