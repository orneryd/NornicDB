import {SessionClient} from '../tnrun-session.mjs';
import {datasetPath,downloadBuffer} from '../../bench1043/tnverify-api.mjs';
const c=await SessionClient.open(async()=>{throw Error('Existing broker required')});
const root=datasetPath+'/tnrun';
try{
 for(const f of await c.call('filesystem.listdir',[root+'/running/bench',[],{select:['name']}])){
  const j=JSON.parse((await downloadBuffer(c,root+'/running/bench/'+f.name)).toString());
  let phase='building',finished=0;
  try{const out=(await downloadBuffer(c,root+'/done/'+j.id+'/out.txt')).toString();phase=[...out.matchAll(/^=== round .*$/gm)].at(-1)?.[0]??phase;finished=[...out.matchAll(/^=== round .*: exit /gm)].length;}catch{}
  console.log(JSON.stringify({running:j.id,rounds:j.rounds,phase,finishedSides:finished}));
 }
 const r=JSON.parse((await downloadBuffer(c,root+'/operator/fix-1384-regression-r4/receipt.json')).toString());
 const heldNow=await c.call('filesystem.listdir',[root+'/operator/fix-1384-regression-r4/held',[],{select:['name']}]); console.log(JSON.stringify({heldNow:heldNow.length,restored:r.restored.length,completed:!!r.completed,restoreErrors:r.restore_errors?.length??0}));
}finally{c.close()}
