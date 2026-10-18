// Retrieve go/format output produced only on TrueNAS, validating the target root.
import fs from 'node:fs/promises';
import path from 'node:path';
import {createHash} from 'node:crypto';
import { SessionClient } from '../tnrun-session.mjs';
import { downloadBuffer } from '../../bench1043/tnverify-api.mjs';
const [id,wt]=process.argv.slice(2);
if(!/^[ij]\d{14}-\d+-[a-z0-9]{4}$/.test(id)||!/^wt-(rc144-embed-lifecycle-fix|embed-health-develop)$/.test(wt))throw new Error('Invalid format target');
const c=await SessionClient.open(async()=>{throw new Error('Existing broker unavailable');});
try{
 const raw=(await downloadBuffer(c,`/mnt/data/nornic-bench-1043/tnrun/done/${id}/out.txt`)).toString();
 const out=raw.split('\n').flatMap(line=>{try{const e=JSON.parse(line);return e.Output?[e.Output]:[];}catch{return[];}}).join('');
 const written=[],stale=[];
 for(const line of out.split('\n')){if(!line.startsWith('FAMILY_FORMAT '))continue;const item=JSON.parse(line.slice(14));if(!/^pkg\/[\w/]+\.go$/.test(item.path))throw new Error('Invalid formatted path');const dest=path.resolve(wt,item.path);if(!dest.startsWith(path.resolve(wt)+path.sep))throw new Error('Outside worktree');if(!item.sha||createHash('sha256').update(await fs.readFile(dest)).digest('hex')!==item.sha){stale.push(item.path);continue;}await fs.writeFile(dest,Buffer.from(item.data,'base64'));written.push(item.path);}
 console.log(JSON.stringify({id,wt,written,stale}));
}finally{c.close();}
