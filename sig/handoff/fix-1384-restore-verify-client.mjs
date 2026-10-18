import fs from 'node:fs/promises';
import crypto from 'node:crypto';
import {SessionClient} from '../tnrun-session.mjs';
import {datasetPath,downloadBuffer} from '../../bench1043/tnverify-api.mjs';
const c=await SessionClient.open(async()=>{throw Error('Existing broker required')});const root=datasetPath+'/tnrun',state=root+'/operator/fix-1384-regression-r4';
try{
 const r=JSON.parse((await downloadBuffer(c,state+'/queue-restored.json')).toString());
 const held=await c.call('filesystem.listdir',[state+'/held',[],{select:['name']}]);
 const ready=(await c.call('filesystem.listdir',[root+'/queue/ab',[],{select:['name']}])).map(x=>x.name).sort();
 const hashes=[];
 for(const name of r.restored){const data=await downloadBuffer(c,root+'/queue/ab/'+name);const digest=crypto.createHash('sha256').update(data).digest('hex');const original=r.held.find(x=>x.name===name);if(digest!==original.sha256)throw Error('Restored hash mismatch: '+name);hashes.push(name);}
 const report={held:held.length,restoredVerified:hashes.length,firstFour:ready.slice(0,4),remainingOrderPreserved:JSON.stringify(ready.slice(4).filter(x=>r.restored.includes(x)))===JSON.stringify([...r.restored].sort()),promotions:r.promotions};
 if(held.length||!report.remainingOrderPreserved)throw Error('Queue restoration verification failed');
 await fs.writeFile(new URL('./fix-1384-ready-queue-receipt.json',import.meta.url),JSON.stringify({...r,verification:report},null,2));console.log(JSON.stringify(report));
}finally{c.close()}
