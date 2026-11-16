import {test,expect} from '@playwright/test';
import {fixture} from './fixture.mjs';

async function chatFixture(page,mode='saved'){
 await fixture(page);let messages=[];
 const thread={id:'chat-1',title:'Database questions',archived:false,database:'catalog',updated_at:new Date().toISOString()};
 await page.route('**/api/bifrost/**',async route=>{
  const request=route.request(),url=new URL(request.url());let data;
  if(url.pathname.endsWith('/workspace'))data={configured:true,database:'admin-chats',suggested_database:'admin-chats',username:'admin'};
  else if(url.pathname.endsWith('/status'))data={model:'qwen3.5-0.8b',heimdall:{enabled:true}};
  else if(url.pathname.endsWith('/autocomplete'))data={suggestion:''};
  else if(url.pathname.endsWith('/conversations/chat-1'))data={thread,messages};
  else if(url.pathname.endsWith('/conversations'))data=[thread];
  else if(url.pathname.endsWith('/chat/completions')){
   const body=request.postDataJSON(),now=new Date().toISOString();
   const tool={id:'tool-1',name:'heimdall_watcher_query',status:'completed',input:{cypher:'RETURN 1 AS ready'},result:{data:{rows:[{ready:1}]}},duration_ms:3};
   const user={id:body.message_id,role:'user',content:body.messages[0].content,created_at:now};
   const assistant={id:'assistant-'+body.message_id,role:'assistant',content:'This database has test records.',reasoning_content:'Inspecting the selected schema.',tool_calls:[tool],status:'completed',created_at:now};
   messages=mode==='saved'?[user,assistant]:[];
   let stream='event: nornic.reasoning\ndata: '+JSON.stringify({text:assistant.reasoning_content})+'\n\n';
   stream+='event: nornic.tool\ndata: '+JSON.stringify(tool)+'\n\n';
   stream+='data: '+JSON.stringify({choices:[{delta:{content:'This database '}}]})+'\n\n';
   stream+='data: '+JSON.stringify({choices:[{delta:{content:'has test records.'}}]})+'\n\n';
   if(mode==='error')stream+='event: nornic.error\ndata: {"message":"Model overloaded"}\n\n';
   else if(mode!=='interrupted')stream+='data: [DONE]\n\n';
   await route.fulfill({contentType:'text/event-stream',body:stream});return;
  }else throw new Error('Unexpected assistant request: '+url.pathname);
  await route.fulfill({json:data});
 });
 await page.goto('/workspace/catalog?assistant=open&chat=chat-1');
 await page.getByRole('button',{name:'Explain this database',exact:true}).click();
 await page.getByRole('button',{name:'Send message',exact:true}).click();
}

test('a streamed turn retains both bubbles, reasoning and tool results after reload',async({page})=>{
 const errors=[];page.on('pageerror',error=>errors.push(error.message));await chatFixture(page);
 await expect(page.locator('.assistant-message-user')).toContainText('Explain the schema');
 await expect(page.locator('.assistant-message').filter({hasText:'This database has test records.'})).toBeVisible();
 const reasoning=page.locator('.assistant-reasoning');await expect(reasoning).not.toHaveAttribute('open','');
 await reasoning.locator('summary').click();await expect(reasoning).toContainText('Inspecting the selected schema.');
 const tool=page.locator('.assistant-tool');await tool.locator('summary').click();await expect(tool).toContainText('RETURN 1 AS ready');await expect(tool).toContainText('"ready": 1');
 await page.reload();await expect(page.locator('.assistant-message-user')).toContainText('Explain the schema');
 await expect(page.locator('.assistant-message')).toHaveCount(2);await expect(reasoning).toHaveAttribute('open','');await expect(tool).toHaveAttribute('open','');
 expect(errors).toEqual([]);
 await page.screenshot({path:'review/prototype/30-assistant-streamed-chat.png',fullPage:false});
});

for(const mode of ['error','interrupted','unsaved'])test(`a ${mode} response remains visible with a persistent retry`,async({page})=>{
 await chatFixture(page,mode);
 await expect(page.getByRole('button',{name:'Retry response',exact:true})).toBeVisible();
 await expect(page.locator('.assistant-message-user')).toContainText('Explain the schema');
 await expect(page.locator('.assistant-message').filter({hasText:'This database has test records.'})).toBeVisible();
 await expect(page.getByRole('button',{name:'Explain this database',exact:true})).not.toBeVisible();
});
