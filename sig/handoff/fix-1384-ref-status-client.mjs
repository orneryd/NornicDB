// Read-only diagnostic for this task's current TrueNAS reference jobs.
import {SessionClient} from '../tnrun-session.mjs';
import {datasetPath,downloadBuffer} from '../../bench1043/tnverify-api.mjs';
const client=await SessionClient.open(async()=>{throw new Error('Existing tnrun broker required')});
try {
 for(const id of ['i20261018035534-30616-4ba4','i20261018035827-48940-ed60','i20261018035833-101708-1680']){
  console.log(id);
  try{const log=(await downloadBuffer(client,datasetPath+'/tnrun/done/'+id+'/out.txt')).toString();console.log(log.split(/\r?\n/).slice(-8).map(l=>l.length>500?l.slice(0,500)+'…':l).join('\n'))}catch(e){console.log(e.message)}
 }
}finally{client.close()}
