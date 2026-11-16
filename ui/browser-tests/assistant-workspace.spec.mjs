import {test,expect} from '@playwright/test';
import {fixture} from './fixture.mjs';

test('creating a private workspace opens an empty chat list and handles empty history',async({page})=>{
 await fixture(page);
 const errors=[];page.on('pageerror',error=>errors.push(error.message));
 let configured=false,searches=0;
 const thread={id:'first-chat',title:'New chat',archived:false,database:'catalog',updated_at:new Date().toISOString()};
 await page.route('**/api/bifrost/**',async route=>{
  const request=route.request(),url=new URL(request.url());let data;
  if(url.pathname.endsWith('/workspace')){
   if(request.method()==='POST')configured=true;
   data={configured,username:'admin',suggested_database:'admin-chats',...(configured?{database:'admin-chats'}:{})};
  }else if(url.pathname.endsWith('/status'))data={model:'qwen3.5-0.8b',heimdall:{enabled:true}};
  else if(url.pathname.endsWith('/conversations/search')){searches++;data=null;}
  else if(url.pathname.endsWith('/autocomplete'))data={suggestion:''};
  else if(url.pathname.endsWith('/conversations/first-chat'))data={thread,messages:null};
  else if(url.pathname.endsWith('/conversations'))data=request.method()==='POST'?thread:null;
  else throw new Error('Unexpected assistant request: '+url.pathname);
  await route.fulfill({json:data});
 });
 await page.goto('/workspace/catalog?assistant=open');
 await page.getByRole('button',{name:'Create private workspace',exact:true}).click();
 await expect(page.getByText('No chats yet.',{exact:true})).toBeVisible();
 await page.getByRole('button',{name:'New chat',exact:true}).first().click();
 await expect(page.getByRole('textbox',{name:'Message the assistant'})).toBeVisible();
 await page.getByRole('searchbox',{name:'Search previous chats'}).fill('missing');
 await expect(page.getByRole('textbox',{name:'Message the assistant'})).toBeVisible();
 await expect.poll(()=>searches).toBe(1);
 expect(errors).toEqual([]);
 await page.reload();
 await expect(page.getByRole('textbox',{name:'Message the assistant'})).toBeVisible();
 expect(errors).toEqual([]);
});
