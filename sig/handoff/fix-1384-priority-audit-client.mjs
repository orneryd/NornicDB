import fs from 'node:fs/promises';
import crypto from 'node:crypto';
import {SessionClient} from '../tnrun-session.mjs';
import {datasetPath,downloadBuffer} from '../../bench1043/tnverify-api.mjs';
const c=await SessionClient.open(async()=>{throw Error('Existing broker required')});
const root=datasetPath+'/tnrun',state=root+'/operator/fix-1384-regression-r3';
try {
 const receipt=JSON.parse((await downloadBuffer(c,state+'/receipt.json')).toString());
 await fs.writeFile(new URL('./fix-1384-priority-r3-audit.json',import.meta.url),JSON.stringify(receipt,null,2));
 console.log(JSON.stringify(receipt));
 console.log('HELD',JSON.stringify(await c.call('filesystem.listdir',[state+'/held',[],{select:['name']}])));
 for(const name of [...new Set(receipt.held.filter(x=>!receipt.restored.includes(x.name)).map(x=>x.name))]){
  const id=name.replace(/\.json$/,'');
  for(const path of [state+'/held/'+name,root+'/queue/ab/'+name,root+'/running/bench/'+name,root+'/done/'+id+'/result.json']){
   try{const buf=await downloadBuffer(c,path);console.log(JSON.stringify({path,sha256:crypto.createHash('sha256').update(buf).digest('hex'),text:buf.toString()}))}catch{console.log(JSON.stringify({path,missing:true}))}
  }
 }
}finally{c.close()}
