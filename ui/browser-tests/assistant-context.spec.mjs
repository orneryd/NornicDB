import {test,expect} from '@playwright/test';
import {fixture} from './fixture.mjs';

test('context usage updates after background compaction without removing the transcript',async({page})=>{
 await fixture(page);
 let compacted=false,requests=0;
 const thread={id:'context-chat',title:'Schema questions',archived:false,database:'catalog',updated_at:new Date().toISOString(),context_tokens:200000,context_limit:8192};
 const messages=[{id:'u1',role:'user',content:'Keep this question in my transcript.'},{id:'a1',role:'assistant',content:'Keep this answer too.'}];
 await page.route('**/api/bifrost/**',async route=>{
  const path=new URL(route.request().url()).pathname;let data;
  if(path.endsWith('/workspace'))data={configured:true,database:'alex-chats',username:'alex'};
  else if(path.endsWith('/status'))data={model:'qwen3.5-0.8b',context_window:262144,max_output_tokens:1024,conversation_compaction:true,heimdall:{enabled:true}};
  else if(path.endsWith('/compact')){requests++;compacted=true;await route.fulfill({status:202,json:{status:'running'}});return;}
  else if(path.endsWith('/context'))data={...thread,context_tokens:1200,compaction_status:'complete'};
  else if(path.endsWith('/conversations/context-chat'))data={thread:compacted?{...thread,context_tokens:1200,compaction_status:'complete'}:thread,messages};
  else if(path.endsWith('/conversations'))data=[thread];
  else if(path.endsWith('/autocomplete'))data={suggestion:''};
  else throw new Error('Unexpected request: '+path);
  await route.fulfill({json:data});
 });
 await page.goto('/workspace/catalog?assistant=open&chat=context-chat');
 const context=page.getByRole('region',{name:'Conversation context'});
 await expect(context).toContainText('200,000 / 262,144 tokens');
 await expect(context).toContainText('61,120 available');
 await page.getByRole('button',{name:'Compact context',exact:true}).click();
 await expect(page.getByRole('button',{name:'Compacting…',exact:true})).toBeDisabled();
 await expect(context).toContainText('in the background');
 await expect(context).toContainText('1,200 / 262,144 tokens');
 await expect(context).toContainText('Context compacted. Full transcript retained.');
 await expect(page.locator('.assistant-message')).toHaveCount(2);
 await expect(page.locator('.assistant-message-user')).toContainText(messages[0].content);
 expect(requests).toBe(1);
 await page.reload();
 await expect(context).toContainText('1,200 / 262,144 tokens');
 await expect(page.locator('.assistant-message')).toHaveCount(2);
 await page.screenshot({path:'review/prototype/31-assistant-context-compacted.png',fullPage:false});
});
