import {test} from 'node:test';
import assert from 'node:assert/strict';
import {decodeViewState} from '../src/utils/useUrlState.ts';

test('URL view state decodes typed controls and treats omitted defaults consistently',()=>{
 assert.equal(decodeViewState(null,false),false);
 assert.equal(decodeViewState('true',false),true);
 assert.equal(decodeViewState('12',3),12);
 assert.equal(decodeViewState('search','query'),'search');
 assert.deepEqual(decodeViewState('["record-1","record-2"]',[]),['record-1','record-2']);
});
test('malformed or mismatched control state falls back without throwing',()=>{
 assert.equal(decodeViewState('{broken',false),false);
 assert.equal(decodeViewState('"true"',false),false);
 assert.equal(decodeViewState('null',3),3);
 assert.deepEqual(decodeViewState('{"id":"record"}',[]),[]);
});
