// Capture actual production bodies; only network drivers and TS annotations
// are replaced. Go executes these statements and observes stored names/values.
import fs from 'node:fs';
import vm from 'node:vm';
import {quoteName as recordingQuoteName} from './peer_recording.mjs';
const root = new URL('../../../', import.meta.url);
const read = path => fs.readFileSync(new URL(path, root), 'utf8');
const names = [' ', '  ', '\t', 'two words', 'space ', 'a.b', 'a,b', 'a(b)', 'tick`key'];
const out = [];
const api = read('ui/src/utils/api.ts');
const method = api.slice(api.indexOf('  async updateNodeProperties('));
let body = method.slice(method.indexOf('    const dbName ='), method.indexOf('\n  /** Per-database config:'));
body = body.slice(0, body.lastIndexOf('\n  }'))
  .replace('const setParts: string[]', 'const setParts')
  .replace('const parameters: Record<string, unknown>', 'const parameters');
let quoteName;
quoteName = recordingQuoteName;
// Test-only removal control: run the same production bodies with identity.
if(process.env.NORNIC_TEST_NAME_QUOTES === 'off') quoteName = name => name;
const AsyncFunction = Object.getPrototypeOf(async function(){}).constructor;
const offset = api.indexOf('    const dbName =', api.indexOf('  async updateNodeProperties('));
const update = new AsyncFunction('nodeId', 'properties', 'database', 'quoteName', '\n'.repeat(api.slice(0, offset).split('\n').length - 3) + body + '\n//# sourceURL=' + new URL('ui/src/utils/api.ts', root));
for(const name of names) {
  await update.call({runCypher: async (_, Text, Parameters) => {
    out.push({Route:'browser', Name:name, Text, Parameters}); return {};
  }}, 'subject', {[name]:2}, 'neo4j', quoteName);
}
let qdrant = read('scripts/migration/qdrant/migrate.mjs')
  .replace(/^import .*;\r?\n/gm, line => line.replace(/[^\n]/g, ''))
  .replace(/main\(\)\.catch\(\(err\) => \{[\s\S]*$/, '');
qdrant += `
(async () => {
  for (const prefix of ['customer-data', 'tick\x60prefix', 'space ']) {
    const Name = labelFor(prefix, 'docs');
    const run = async (Text, Parameters) => {
      const Route = Text.startsWith('MATCH') ? 'read' : Text.includes('MERGE') ? 'upsert' : 'schema';
      out.push({Route, Name, Text, Parameters});
      return {records:[{get:()=>0}]};
    };
    const src = {getCollection:async()=>({config:{params:{vectors:{size:3}}}}), scroll:async()=>({points:[{id:'point',vector:[1,2,3],payload:{}}]})};
    await migrateCollection(src, {run}, 'docs', 10, prefix, true, false);
  }
})()
`;
await vm.runInNewContext(qdrant, {out, quoteName, console:{log(){},warn(){}}}, {filename:new URL('scripts/migration/qdrant/migrate.mjs', root).pathname});
process.stdout.write(JSON.stringify(out));
