import {readFileSync,readdirSync,mkdirSync,writeFileSync} from 'node:fs';
import {join} from 'node:path';
const roots=['src/pages','src/components','Bifrost'];
const rows=[];
function scan(dir){for(const entry of readdirSync(dir,{withFileTypes:true})){const file=join(dir,entry.name);if(entry.isDirectory())scan(file);else if(file.endsWith('.tsx')){
 const text=readFileSync(file,'utf8');
 for(const match of text.matchAll(/<(button|input|select|textarea|details|Modal|UiGrid|StatefulGrid|FormInput|Button|Disclosure)\b/g)) {
  rows.push({file:file.replaceAll('\\','/'),line:text.slice(0,match.index).split('\n').length,tag:match[1]});
 }

 }} }
for(const root of roots)scan(root);
mkdirSync('review',{recursive:true});writeFileSync('review/control-inventory.json',JSON.stringify(rows,null,2)+'\n');
console.log(`${rows.length} control declarations inventoried across ${new Set(rows.map(row=>row.file)).size} files.`);
