import fs from 'node:fs/promises';
import {TrueNasClient} from '../truenas-client.mjs';
import {logLogin} from '../tnrun-session.mjs';
import {ensureDir,putFile,datasetPath,mutation,downloadBuffer} from '../../bench1043/tnverify-api.mjs';
const app='nornic-tnrun-fix1384-stop1391',id='a20261018011000-25632-758b',remote=datasetPath+'/tnrun/operator/fix-1384-stop-1391';
const c=new TrueNasClient();
try{
 await c.connect(); await logLogin('direct','fix1384-approved-stop1391','ok');
 if(process.argv.includes('--cleanup')){
  const receipt=JSON.parse((await downloadBuffer(c,remote+'/receipt.json')).toString());
  if(!receipt.stopped)throw new Error('Exact job has not stopped');
  await fs.writeFile(new URL('./fix-1384-stop-1391-receipt.json',import.meta.url),JSON.stringify(receipt,null,2));
  if((await c.call('app.query',[[['id','=',app]],{select:['id']}])).length)await mutation(c,'app.delete',[app,{remove_images:false,remove_ix_volumes:false,force_remove_ix_volumes:false}],{job:true});
  console.log(JSON.stringify({stopped:receipt.job,groups:receipt.signals.length,workerRestarted:receipt.workerRestarted,operatorRemoved:true}));
 }else{
  if((await c.call('app.query',[[['id','=',app]],{select:['id']}])).length)throw new Error('Stop operator already exists; inspect before retrying');
  const j=JSON.parse((await downloadBuffer(c,datasetPath+'/tnrun/running/bench/'+id+'.json')).toString());
  if(j.id!==id||j.head!=='cc453b3862902611f18d658e0d3acd53bea94d63'||j.base!=='bc8a4717089a7a6e8dd90639ca747d831ba85095')throw new Error('Exact job guard failed');
  const choices=Object.values(await c.call('app.container_console_choices',['nornic-tnrun-bench'])).filter(x=>x.service_name==='tnr-bench'&&x.state==='running');
  if(choices.length!==1)throw new Error('Exact benchmark container not found');
  await ensureDir(c,remote);
  await putFile(c,Buffer.from(JSON.stringify(j)),remote+'/guard.json','fix1384/approved-stop-guard');
  await putFile(c,await fs.readFile(new URL('./fix-1384-stop-1391.py',import.meta.url)),remote+'/control.py','fix1384/approved-stop-controller');
  const spec={services:{control:{image:'local/nornic-verify-tn:3179ae9d04b4',user:'568:568',restart:'no',network_mode:'none',pid:'container:'+choices[0].id,cpuset:'20,84',command:['python3','-u','/work/tnrun/operator/fix-1384-stop-1391/control.py'],volumes:[{type:'bind',source:datasetPath+'/tnrun',target:'/work/tnrun',bind:{create_host_path:false}}],deploy:{resources:{limits:{cpus:'0.25',memory:'128M'}}}}}};
  await mutation(c,'app.create',[{app_name:app,custom_app:true,custom_compose_config_string:JSON.stringify(spec)}],{job:true});
  console.log('Exact approved benchmark stop controller created; worker left running.');
 }
}finally{c.close()}
