import fs from 'node:fs/promises';
import {SessionClient} from '../tnrun-session.mjs';
import {datasetPath,downloadBuffer} from '../../bench1043/tnverify-api.mjs';
const c=await SessionClient.open(async()=>{throw Error('Existing broker required')});
try{const b=await downloadBuffer(c,datasetPath+'/tnrun/done/a20261018045718-87272-de63/result.json');await fs.writeFile(new URL('./fix-1384-valid-job-finished.json',import.meta.url),b);console.log(b.toString())}finally{c.close()}
