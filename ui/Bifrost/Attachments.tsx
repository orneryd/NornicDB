import {createContext,useContext} from 'react';
import {AttachmentPrimitive,useAuiState,type AttachmentAdapter,type CompleteAttachment} from '@assistant-ui/react';
import {chatClient,type ChatAttachment} from './client';

export const AttachmentLimits=createContext({contextTokens:8192,maxBytes:8*1024*1024,imageInput:false});
export const AttachmentThread=createContext('');
export function MessageAttachments(){const files=useAuiState(state=>state.message.metadata.custom.attachments) as ChatAttachment[]|undefined;const thread=useContext(AttachmentThread);return files?.length?<SavedAttachments thread={thread} files={files}/>:null}
export function DraftAttachment(){
 const attachment=useAuiState(state=>state.attachment),limits=useContext(AttachmentLimits);
 const size=attachment.file?.size??0,large=size/4>limits.contextTokens/2;
 const image=attachment.content?.find(part=>part.type==='image');
 return <AttachmentPrimitive.Root className="assistant-attachment">{image?.type==='image'&&<img src={image.image} alt={attachment.name}/>}<div><AttachmentPrimitive.Name/><span>{(size/1024).toFixed(1)} KiB</span>{attachment.status.type==='incomplete'?<p role="alert">{attachment.status.message}</p>:large?<p className="assistant-attachment-warning">This file exceeds the inline context budget. It will be saved privately; the assistant can retrieve relevant sections.</p>:<p>Saved privately when you send.</p>}{attachment.type==='image'&&!limits.imageInput&&<p>This backend can store the image but cannot analyze its pixels yet.</p>}</div><AttachmentPrimitive.Remove aria-label={`Remove ${attachment.name}`}>Remove</AttachmentPrimitive.Remove></AttachmentPrimitive.Root>;
}

export function privateAttachmentAdapter(thread:string,maxBytes:number,onUpload:(name:string)=>void){
 const metadata=new Map<string,ChatAttachment>();const previews=new Set<string>();
 const revoke=(url:string)=>{if(previews.delete(url))URL.revokeObjectURL(url)};
 const adapter:AttachmentAdapter={
  accept:'*/*',
  async add({file}){
   const id=crypto.randomUUID(),image=file.type.startsWith('image/');
   let content:CompleteAttachment['content']=[];
   if(image){const url=URL.createObjectURL(file);previews.add(url);content=[{type:'image',image:url,filename:file.name}]}
   return {id,type:image?'image':'file',name:file.name,contentType:file.type,file,content,status:file.size>maxBytes?{type:'incomplete',reason:'error',message:`This file exceeds the ${(maxBytes/1024/1024).toFixed(0)} MiB upload limit. Choose a smaller file or split it into parts.`}:{type:'requires-action',reason:'composer-send'}};
  },
  async send(attachment,options){
   if(attachment.file.size>maxBytes)throw new Error('Choose a smaller file before sending.');
   onUpload(attachment.name);
   try{
    const stored=await chatClient.upload(thread,attachment.file,options?.signal);metadata.set(stored.id,stored);
    const content:CompleteAttachment['content']=stored.content_type.startsWith('image/')?[{type:'image',image:chatClient.attachmentUrl(thread,stored.id),filename:stored.name}]:[{type:'file',data:stored.id,sourceType:'id',filename:stored.name,mimeType:stored.content_type}];
    return {id:stored.id,type:attachment.type,name:stored.name,contentType:stored.content_type,file:attachment.file,content,status:{type:'complete'}};
   }finally{onUpload('')}
  },
  async remove(attachment){for(const part of attachment.content??[]){if(part.type==='image')revoke(part.image)}},
 };
 return {adapter,metadata,dispose:()=>{for(const url of previews)URL.revokeObjectURL(url);previews.clear()}};
}

export function SavedAttachments({thread,files}:{thread:string;files:ChatAttachment[]}){return <ul className="assistant-saved-attachments">{files.map(file=><li key={file.id}>{file.content_type.startsWith('image/')&&<img src={chatClient.attachmentUrl(thread,file.id)} alt={file.name}/>}<a href={chatClient.attachmentUrl(thread,file.id)} target="_blank" rel="noopener noreferrer">{file.name}</a><span>{(file.size/1024).toFixed(1)} KiB{file.readable?` · ${file.chunks} sections`:' · Stored file'}</span></li>)}</ul>}
