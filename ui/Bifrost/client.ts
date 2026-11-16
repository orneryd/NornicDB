import {BASE_PATH,joinBasePath} from '../src/utils/basePath';

export type ChatWorkspace={configured:boolean;database?:string;suggested_database:string;username:string};
export type ChatThread={id:string;title:string;archived:boolean;database?:string;updated_at:string;context_tokens?:number;context_limit?:number;compaction_status?:'running'|'complete'|'failed'|'not_needed';compaction_error?:string;summary_count?:number};
export type ChatToolTrace={id:string;name:string;status:'running'|'completed'|'error';input:Record<string,unknown>;result?:unknown;error?:string;duration_ms?:number};
export type ChatAttachment={id:string;name:string;content_type:string;size:number;estimated_tokens:number;readable:boolean;chunks:number};
export type ChatMessage={id:string;role:'user'|'assistant';content:string;attachments?:ChatAttachment[];reasoning_content?:string;tool_calls?:ChatToolTrace[];created_at:string;status?:'running'|'completed'|'cancelled'|'error';request_id?:string};
export type ChatSource={conversation_id?:string;message_id?:string;title?:string;content?:string;method?:string;path?:string};
export type AssistantStatus={model:string;context_window?:number;max_output_tokens?:number;context_source?:string;max_attachment_bytes?:number;image_input?:boolean;conversation_compaction?:boolean;heimdall?:{enabled:boolean}};

async function request<T>(path:string,options:RequestInit={}):Promise<T>{
 const response=await fetch(joinBasePath(BASE_PATH,path),{credentials:'include',cache:'no-store',...options,headers:{...(options.body instanceof FormData?{}:{'Content-Type':'application/json'}),...options.headers}});
 if(!response.ok){const text=await response.text();let message=text;try{const error=JSON.parse(text);message=error.message??error.errors?.[0]?.message??`Request failed (${response.status})`;}catch{}throw new Error(message||`Request failed (${response.status})`);}
 return response.json();
}
// Older servers encode empty Go slices as null. Keep collection state an array
// at the API boundary, including saved messages and history search results.
function collection<T>(value:T[]|null):T[]{
 if(value===null)return [];
 if(!Array.isArray(value))throw new Error('The assistant returned an invalid collection.');
 return value;
}
export const chatClient={
 compact:(id:string)=>request<{status:string}>(`/api/bifrost/conversations/${encodeURIComponent(id)}/compact`,{method:'POST'}),
 context:(id:string)=>request<ChatThread>(`/api/bifrost/conversations/${encodeURIComponent(id)}/context`),
 upload:async(thread:string,file:File,signal?:AbortSignal)=>{const data=new FormData();data.append('file',file);return request<ChatAttachment>(`/api/bifrost/conversations/${encodeURIComponent(thread)}/attachments`,{method:'POST',body:data,signal});},
 attachmentUrl:(thread:string,id:string)=>joinBasePath(BASE_PATH,`/api/bifrost/conversations/${encodeURIComponent(thread)}/attachments/${encodeURIComponent(id)}`),
 workspace:()=>request<ChatWorkspace>('/api/bifrost/workspace'),
 provision:(database:string)=>request<ChatWorkspace>('/api/bifrost/workspace',{method:'POST',body:JSON.stringify({database})}),
 status:()=>request<AssistantStatus>('/api/bifrost/status'),
 list:async(archived=false)=>collection(await request<ChatThread[]|null>(`/api/bifrost/conversations?archived=${archived}`)),
 create:(database='')=>request<ChatThread>('/api/bifrost/conversations',{method:'POST',body:JSON.stringify({database})}),
 read:async(id:string)=>{const data=await request<{thread:ChatThread;messages:(Omit<ChatMessage,'tool_calls'>&{tool_calls?:ChatToolTrace[]|string|null})[]|null}>(`/api/bifrost/conversations/${encodeURIComponent(id)}`);return {...data,messages:collection(data.messages).map(message=>({...message,tool_calls:collection<ChatToolTrace>(typeof message.tool_calls==='string'?JSON.parse(message.tool_calls):message.tool_calls??[])}))};},
 update:(id:string,change:{archived?:boolean;title?:string;database?:string})=>request<ChatThread>(`/api/bifrost/conversations/${encodeURIComponent(id)}`,{method:'PATCH',body:JSON.stringify(change)}),
 remove:(id:string)=>request<{deleted:boolean}>(`/api/bifrost/conversations/${encodeURIComponent(id)}`,{method:'DELETE'}),
 search:async(query:string)=>collection(await request<ChatSource[]|null>(`/api/bifrost/conversations/search?q=${encodeURIComponent(query)}`)),
};

export type ChatStreamEvent={type:'text';text:string}|{type:'usage';tokens:number;limit:number}|{type:'reasoning';text:string}|{type:'tool';tool:ChatToolTrace}|{type:'activity';title:string;message?:string}|{type:'context';sources:ChatSource[];compacted?:boolean;contextTokens?:number}|{type:'navigation';title:string;path:string}|{type:'error';message:string};
/** SSE framing is independent of network chunks and preserves split UTF-8. */
export async function consumeChatStream(response:Response,onEvent:(event:ChatStreamEvent)=>void,signal?:AbortSignal){
 if(!response.ok){const text=await response.text();try{throw new Error(JSON.parse(text).message??text)}catch(error){throw error instanceof SyntaxError?new Error(text||`Chat failed (${response.status})`):error}}
 if(!response.body)throw new Error('The server returned no response stream.');
 const reader=response.body.getReader(),decoder=new TextDecoder();let buffer='',terminated=false,failed=false;
 const frame=(value:string)=>{
  const lines=value.split(/\r?\n/);const name=lines.find(line=>line.startsWith('event:'))?.slice(6).trim();const data=lines.filter(line=>line.startsWith('data:')).map(line=>line.slice(5).trimStart()).join('\n');
  if(data==='[DONE]'){terminated=true;return;}if(!data)return;
  let item;try{item=JSON.parse(data)}catch{throw new Error('The server returned an invalid chat event.')}
  if(name==='nornic.context'){onEvent({type:'context',sources:item.sources??[],compacted:item.compacted,contextTokens:item.context_tokens});return;}
  if(name==='nornic.reasoning'){onEvent({type:'reasoning',text:item.text??''});return;}
  if(name==='nornic.tool'){onEvent({type:'tool',tool:item});return;}
  if(name==='nornic.usage'){onEvent({type:'usage',tokens:item.input_tokens_estimated??0,limit:item.context_window??0});return;}
  if(name==='nornic.navigation'){onEvent({type:'navigation',title:item.title,path:item.path});return;}
  if(name==='nornic.error'||item.error){failed=true;onEvent({type:'error',message:item.message??(typeof item.error==='string'?item.error:item.error?.message)??'Chat request failed'});return;}
  if(name==='nornic.activity'){onEvent({type:'activity',title:item.title??'Working',message:item.message});return;}
  if(item.choices?.[0]?.finish_reason)terminated=true;
  const delta=item.choices?.[0]?.delta,text=delta?.content;
  if(delta?.role==='heimdall'){if(text)onEvent({type:'activity',title:'Assistant activity',message:text});return;}
  if(typeof text==='string'&&text)onEvent({type:'text',text});
 };
 try{while(true){if(signal?.aborted)throw new DOMException('Cancelled','AbortError');const {done,value}=await reader.read();buffer+=decoder.decode(value,{stream:!done});let match;while((match=/\r?\n\r?\n/.exec(buffer))){frame(buffer.slice(0,match.index));buffer=buffer.slice(match.index+match[0].length);}if(done){if(buffer.trim())frame(buffer);break;}}if(!terminated&&!failed)throw new Error('The connection ended before the assistant completed its response.');}finally{reader.releaseLock();}
}
export async function sendChat(conversationId:string,messageId:string,content:string,onEvent:(event:ChatStreamEvent)=>void,signal:AbortSignal,endpoint=joinBasePath(BASE_PATH,'/api/bifrost/chat/completions'),thinking=false,attachmentIDs:string[]=[]){
 const response=await fetch(endpoint,{method:'POST',credentials:'include',headers:{'Content-Type':'application/json'},body:JSON.stringify({conversation_id:conversationId,message_id:messageId,messages:[{role:'user',content}],stream:true,thinking,attachment_ids:attachmentIDs}),signal});
 return consumeChatStream(response,onEvent,signal);
}
