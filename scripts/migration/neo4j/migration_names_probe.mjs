// Lab test adapter: execute the actual standalone writer bodies. Replace only
// driver loading and CLI entry; source sessions below capture both pages.
import fs from "node:fs";
import vm from "node:vm";
import { quoteName } from '../../../internal/testutil/cyphernames/peer_recording.mjs';
let source = fs.readFileSync(new URL("migrate.mjs", import.meta.url), "utf8");
const driver = 'import neo4j from "neo4j-driver";';
const entry = 'main().catch((err) => {\n  console.error(err);\n  process.exit(1);\n});';
if (source.split(driver).length !== 2 || source.split(entry).length !== 2) throw Error("CLI adapter source changed");
source = source.replace('import { quoteName } from "../../../ui/src/utils/cypherNames.mjs";', "").replace(driver, "const neo4j = {int: x => x};").replace(entry, "");
const names = JSON.parse(fs.readFileSync(0, "utf8"));
source += `
(async () => {
  const out = {};
  for (const name of names) {
    const statements = [{Route:'create',Text:edgeCreateCypher(name)}, {Route:'props',Text:edgePropsCypher(name)}];
    for (const [route, stream] of [['node',streamNodes],['edge',streamEdges]]) {
      let page = 0;
      const sess = {run: async text => {
        statements.push({Route:route+(page===0?'-first':'-cursor'),Text:text});
        return {records:page++===0?[{get:key=>key==='id'?'cursor':null}]:[]};
      }};
      for await (const chunk of stream(sess,name,1)) {}
      if(page!==2)throw Error('pagination population');
    }
    for (const type of ['UNIQUENESS','NODE_KEY','NODE_PROPERTY_EXISTENCE','RELATIONSHIP_PROPERTY_EXISTENCE','RELATIONSHIP_UNIQUENESS']) {
      const row={name,type,entityType:type.startsWith('RELATIONSHIP')?'RELATIONSHIP':'NODE',labelsOrTypes:[name],properties:[name]};
      statements.push({Route:type,Text:constraintToCypher(row)});
      if(type==='UNIQUENESS'||type==='NODE_KEY'){row.properties=[name,'second\x60key'];statements.push({Route:type+'-composite',Text:constraintToCypher(row)});}
    }
    for(const type of ['RANGE','BTREE','FULLTEXT','VECTOR']){
      const row={name,type,entityType:'NODE',labelsOrTypes:[name],properties:[name],options:{indexConfig:{'vector.dimensions':3}}};
      statements.push({Route:type,Text:indexToCypher(row)});
      if(type!=='VECTOR'){row.properties=[name,'second\x60key'];statements.push({Route:type+'-composite',Text:indexToCypher(row)});}
    }
    out[name]=statements;
  }
  return out;
})()
`;
const output = await vm.runInNewContext(source, {names, console, quoteName}, {filename:new URL('migrate.mjs', import.meta.url).pathname});
process.stdout.write(JSON.stringify(output));
