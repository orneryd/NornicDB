import {test} from 'node:test';
import assert from 'node:assert/strict';
import {rankEntryCandidates,isTestSource} from '../src/utils/graphifyEntry.ts';
test('entry conventions outrank connectivity across languages and paths',()=>{
 for(const [label,source_file] of [['main','cmd/tool/main.go'],['Main','src/Program.cs'],['main','src/main.rs'],['main','src/App.java'],['bootstrap','src/main.ts'],['run','src/__main__.py'],['handler','src/server.ts']]){
  const ranked=rankEntryCandidates([{id:'utility',label:'parse',source_file:'src/util.ts',degree:10000},{id:'entry',label,source_file,degree:1}]);
  assert.equal(ranked[0].id,'entry',source_file);assert.ok(ranked[0].reason);
 }
});
test('explicit Main wins, tests and dependencies do not become inferred roots',()=>{
 const ranked=rankEntryCandidates([{id:'test',label:'main',source_file:'src/__tests__/main.ts',explicit:true},{id:'vendor',label:'main',source_file:'vendor/main.go',explicit:true},{id:'app',label:'main',source_file:'main.go'},{id:'chosen',label:'launch',source_file:'src/launch.ts',explicit:true},{id:'chosen',label:'launch',source_file:'src/launch.ts'}]);
 assert.deepEqual(ranked.map(x=>x.id),['chosen','app']);
 for(const path of ['C:\\repo\\tests\\Main.cs','src/widget.spec.ts','lib/test_app.py','src/AppTest.java','spec/service_spec.rb'])assert.equal(isTestSource(path),true,path);
 assert.equal(isTestSource('src/contest.ts'),false);
});
test('library candidates are deterministic and do not pretend to be executable entries',()=>{
 const nodes=[{id:'b',label:'read',source_file:'lib/read.go',degree:10},{id:'a',label:'write',source_file:'lib/write.go',degree:10}];
 assert.deepEqual(rankEntryCandidates(nodes).map(x=>x.id),['a','b']);
 assert.equal(rankEntryCandidates(nodes)[0].reason,'Connected source symbol');
 assert.deepEqual(rankEntryCandidates([]),[]);
});
