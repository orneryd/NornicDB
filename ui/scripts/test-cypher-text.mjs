import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { cypherStatementSpans } from '../src/utils/cypherText.mjs';

const fixtures = JSON.parse(readFileSync(new URL('../../internal/cyphertext/testdata/statement_spans.json', import.meta.url), 'utf8'));
for (const fixture of fixtures) {
  assert.deepEqual(cypherStatementSpans(fixture.source).map(([start, end]) => fixture.source.slice(start, end)), fixture.statements);
}
console.log(`PASS ${fixtures.length} browser statement extent fixtures`);
