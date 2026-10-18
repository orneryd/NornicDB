// Read-only TrueNAS client: fetch terminal result records for owned jobs.
import fs from 'node:fs/promises';
import { SessionClient } from '../tnrun-session.mjs';
import { downloadBuffer } from '../../bench1043/tnverify-api.mjs';
const client=await SessionClient.open(async()=>{throw new Error('Existing broker unavailable');});
const results=[];
try{
  for(const id of process.argv.slice(2)){
    if(!/^[ij]\d{14}-\d+-[a-z0-9]{4}$/.test(id))throw new Error('Invalid owned Go job ID');
    try{
      const result=JSON.parse((await downloadBuffer(client,`/mnt/data/nornic-bench-1043/tnrun/done/${id}/result.json`)).toString());
      results.push({id,result});
    }catch(e){results.push({id,missingTerminalResult:e.message});}
  }
  await fs.writeFile('sig/tnrun-results/rc144-r73-and-1472-terminal-results.json',JSON.stringify(results,null,2));
  console.log(JSON.stringify({count:results.length,missing:results.filter(x=>x.missingTerminalResult),sample:results[0]}));
}finally{client.close();}
