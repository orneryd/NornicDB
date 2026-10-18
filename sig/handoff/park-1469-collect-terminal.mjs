// Read-only tnrun client. Collection only: never submits or changes a lab job.
import fs from 'node:fs/promises';
import {SessionClient} from '../tnrun-session.mjs';
import {downloadBuffer} from '../../bench1043/tnverify-api.mjs';
const ids=['i20261018122450-90312-9742','i20261018124053-43900-8ad2'];
const client=await SessionClient.open(async()=>{throw new Error('Existing broker unavailable');});
const results=[];
try {
  for(const id of ids) {
    const name=`/mnt/data/nornic-bench-1043/tnrun/done/${id}/result.json`;
    await client.call('filesystem.stat',[name]);
    results.push({id,result:JSON.parse((await downloadBuffer(client,name)).toString())});
  }
  await fs.writeFile('sig/tnrun-results/park-1469-additional-terminal-results.json',JSON.stringify(results,null,2)+'\n');
  console.log(JSON.stringify(results));
} finally {client.close();}
