// Test-only Node adapter; the browser writer owns no filesystem dependency.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {quoteName as escapeName} from '../../../ui/src/utils/cypherNames.mjs';
const root = fileURLToPath(new URL('../../../', import.meta.url));
const seen = new Set();
const Subject = path.relative(root, process.argv[1]).replaceAll('\\', '/');
function record(Role, Input, stack) {
  for (const line of stack.split('\n').slice(1)) {
    const match = line.match(/at (?:(.*?) \()?((?:file:\/\/\/|\/).*?):(\d+):\d+\)?$/);
    if (!match) continue;
    const absolute = match[2].startsWith('file:') ? fileURLToPath(match[2]) : match[2];
    const File = path.relative(root, absolute).replaceAll('\\', '/');
    if (!/^(scripts|ui)\//.test(File) || /(?:_probe|cypherNames|test_)/.test(File)) continue;
    const record = JSON.stringify({Runtime:'js', File, Function:match[1] || '<anonymous>', Line:Number(match[3]), Role, Input, Subject});
    if (!seen.has(record)) { seen.add(record); fs.appendFileSync(process.env.NORNIC_NAME_RECORDING, record+'\n'); }
  }
}

// Probes bind this wrapper in place of the production import. The escaping
// implementation remains the shipped peer; only this test adapter observes.
export function quoteName(name, role = 'variable') {
  if (process.env.NORNIC_NAME_RECORDING) record(role, 'decoded', new Error().stack);
  return escapeName(name, role);
}
