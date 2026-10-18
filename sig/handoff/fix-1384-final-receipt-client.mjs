import fs from 'node:fs/promises';
import {SessionClient} from '../tnrun-session.mjs';
import {datasetPath,downloadBuffer} from '../../bench1043/tnverify-api.mjs';
const c=await SessionClient.open(async()=>{throw Error('Existing broker required')});
const pairs=[['a20261018045918-48476-6ade','a00000000000001-1384-default','524b71ba3365be6f453a107d77c0816b7396b41a'],['a20261018045918-61784-2d57','a00000000000002-1384-control-default','e138156772abea4912fdea1c3f151ef8065e386b'],['a20261018045918-84716-05d2','a00000000000003-1384-control-antlr','e138156772abea4912fdea1c3f151ef8065e386b'],['a20261018045919-67144-d619','a00000000000004-1384-antlr','524b71ba3365be6f453a107d77c0816b7396b41a']];
const root=datasetPath+'/tnrun',head='fc375efa4ba55253cc8a775172fd4933856f00ea';
try{
 const jobs=[];
 for(const [request,execution,base] of pairs){
  const buf=await downloadBuffer(c,root+'/done/'+execution+'/result.json'), result=JSON.parse(buf.toString());
  const out=(await downloadBuffer(c,root+'/done/'+execution+'/out.txt')).toString();
  const alias=JSON.parse((await downloadBuffer(c,root+'/done/'+request+'/result.json')).toString());
  if(result.state!=='DONE'||result.exit!==0||result.rounds!==10||!out.includes('base '+base+' | head '+head+' |')||alias.priorityExecutionId!==execution)throw Error('Final artifact/source guard failed: '+execution);
  await fs.writeFile(new URL('../tnrun-results/ab-'+execution+'/result.json',import.meta.url),buf);
  jobs.push({request,execution,base,head,result,aliasVerified:true});
 }
 const held=await c.call('filesystem.listdir',[root+'/operator/fix-1384-regression-r4/held',[],{select:['name']}]);
 const apps=await c.call('app.query',[[['id','=','nornic-tnrun-fix1384-priority-r4']],{select:['id']}]);
 if(held.length||apps.length)throw Error('Temporary controller or held work remains');
 const report={jobs,heldDirectoryEmpty:true,temporaryAppRemoved:true,allOwnedJobsComplete:true};
 await fs.writeFile(new URL('./fix-1384-final-jobs-receipt.json',import.meta.url),JSON.stringify(report,null,2));
 console.log(JSON.stringify({verifiedNativeJobs:jobs.length,allDoneExit0:true,held:held.length,temporaryAppRemoved:!apps.length}));
}finally{c.close()}
