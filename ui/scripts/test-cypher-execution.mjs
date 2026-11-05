// Exercise the actual store with its real Zustand state and splitting owner.
// Only API I/O and module URLs are substituted; no execution logic is copied.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { stripTypeScriptTypes } from 'node:module';

let source = stripTypeScriptTypes(readFileSync(new URL('../src/store/appStore.ts', import.meta.url), 'utf8'));
const apiURL = 'data:text/javascript,' + encodeURIComponent('export const api = globalThis.__cypherExecutionAPI');
for (const [old, target] of [
  ["from 'zustand'", new URL('../node_modules/zustand/esm/index.mjs', import.meta.url).href],
  ["from '../utils/api'", apiURL],
  ["from '../utils/cypherStatements'", new URL('../src/utils/cypherStatements.ts', import.meta.url).href],
]) {
  assert.equal(source.split(old).length, 2, `production import changed: ${old}`);
  source = source.replace(old, `from ${JSON.stringify(target)}`);
}
let calls = [];
let failure = null;
globalThis.__cypherExecutionAPI = {
  async executeCypher(statement, params, database) {
    calls.push({ statement, params, database });
    if (calls.length === 1 && failure === 'throw') throw new Error('synthetic backend diagnostic');
    return { results: [], errors: calls.length === 1 && failure === 'response' ? [{ message: 'synthetic backend diagnostic' }] : [] };
  },
};
const { useAppStore } = await import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));
const initial = useAppStore.getState();
const quoted = String.raw`RETURN \u0027a;b\u0027;RETURN 2`;
const quotedParts = [String.raw`RETURN \u0027a;b\u0027`, 'RETURN 2'];
const malformed = String.raw`RETURN \u12;RETURN 2`;
const malformedParts = [malformed];
const cases = [
  { name: 'encoded quoted literal', script: quoted, expected: quotedParts },
  { name: 'encoded separator', script: String.raw`RETURN 1\u003bRETURN 2`, expected: ['RETURN 1', 'RETURN 2'] },
  { name: 'stop on response error', script: quoted, expected: quotedParts, failure: 'response' },
  { name: 'continue on response error', script: quoted, expected: quotedParts, failure: 'response', continueOnError: true },
  { name: 'malformed source reaches backend', script: malformed, expected: malformedParts, failure: 'throw', malformed: true },
  { name: 'malformed source continue policy', script: malformed, expected: malformedParts, failure: 'throw', continueOnError: true, malformed: true },
];
let checked = 0;
for (const test of cases) {
  if (process.argv.includes('--malformed-only') && !test.malformed) continue;
  calls = [];
  failure = test.failure ?? null;
  useAppStore.setState({ ...initial, selectedDatabase: 'tenant_fixture', cypherQuery: test.script, queryHistory: [] }, true);
  await useAppStore.getState().executeCypher({ continueOnError: test.continueOnError });
  const expectedCalls = failure && !test.continueOnError ? test.expected.slice(0, 1) : test.expected;
  assert.deepEqual(calls, expectedCalls.map(statement => ({ statement, params: undefined, database: 'tenant_fixture' })), test.name);
  const state = useAppStore.getState();
  assert.equal(state.queryLoading, false, test.name);
  assert.deepEqual(state.cypherResults.map(result => result.statement), test.expected, test.name);
  assert.deepEqual(state.cypherResults.map((_, index) => index === 0 && failure ? 'error' : index >= expectedCalls.length ? 'pending' : 'success'), state.cypherResults.map(result => result.status), test.name);
  assert.equal(state.queryError, failure ? 'Statement 1 failed: synthetic backend diagnostic' : null, test.name);
  assert.deepEqual(state.queryHistory, [test.script], test.name);
  checked++;
}
console.log(`PASS ${checked} actual editor execution contracts`);
