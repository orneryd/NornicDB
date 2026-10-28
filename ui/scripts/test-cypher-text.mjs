import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { cypherStatementSpans } from '../src/utils/cypherText.mjs';

const fixtures = JSON.parse(readFileSync(new URL('../../internal/cyphertext/testdata/statement_spans.json', import.meta.url), 'utf8'));
for (const fixture of fixtures) {
  assert.deepEqual(cypherStatementSpans(fixture.source).map(([start, end]) => fixture.source.slice(start, end)), fixture.statements);
}
console.log(`PASS ${fixtures.length} browser statement extent fixtures`);

// Independent original UTF16 coordinates: astral prefix and zero-width edits.
assert.deepEqual(cypherStatementSpans("RETURN '😀';RETURN \\u0027a;b\\u0027;RETURN 2"), [[0, 11], [12, 34], [35, 43]]);
assert.deepEqual(cypherStatementSpans(String.raw`\u200b;RETURN 2`), [[6, 6], [7, 15]]);
assert.deepEqual(cypherStatementSpans(String.raw`\u200b\u200c；RETURN 2`), [[12, 12], [13, 21]]);
console.log('PASS 3 original UTF16 coordinate controls');

// Byte-pinned opaque-span witness for admitted malformed raw-Unicode source.
const malformedWitnessHex = '52455455524e205c7530303237615c7531323b43524541544520283a42726f777365724c69746572616c5769746e657373293b5c7530303237625c7530303237';
const malformedWitnessSource = Buffer.from(malformedWitnessHex, 'hex').toString('utf8');
assert.equal(Buffer.from(malformedWitnessSource, 'utf8').toString('hex'), malformedWitnessHex);
assert.deepEqual(cypherStatementSpans(malformedWitnessSource), [[0, malformedWitnessSource.length]]);
console.log('PASS 1 byte-pinned malformed opaque-span witness');
