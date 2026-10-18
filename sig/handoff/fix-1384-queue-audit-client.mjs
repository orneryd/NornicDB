import {SessionClient} from '../tnrun-session.mjs';
import {datasetPath,downloadBuffer} from '../../bench1043/tnverify-api.mjs';
const c=await SessionClient.open(async()=>{throw Error('Existing broker required')});
try{
 const dir=datasetPath+'/tnrun/queue/ab';
 const entries=await c.call('filesystem.listdir',[dir,[],{select:['name']}]);
 for(const f of entries){const j=JSON.parse((await downloadBuffer(c,dir+'/'+f.name)).toString());console.log(JSON.stringify({id:j.id,label:j.label,args:j.binArgs}));}
 const apps=await c.call('app.query',[[],{select:['id','state']}]);
 console.log('OPERATORS',JSON.stringify(apps.filter(x=>/priority|startup|fix1384|release/.test(x.id))));
}finally{c.close()}
