import {useRef,useState} from 'react';
import {useUnsavedChanges} from './useUnsavedChanges';
import {Plus,Undo2,X} from 'lucide-react';
import {Disclosure} from './Disclosure';
import {JsonPreview} from './JsonPreview';
import {isReadOnlyProperty} from '../../utils/nodeUtils';

type ValueType='string'|'number'|'boolean'|'json';
type Draft={id:string; originalKey:string|null; key:string; value:string; type:ValueType; removed:boolean};
function parseValue(draft:Draft):unknown {
 if(draft.type==='string')return draft.value;
 if(draft.type==='boolean'){
  if(draft.value!=='true'&&draft.value!=='false')throw Error('Choose True or False.');
  return draft.value==='true';
 }
 if(draft.type==='number'){
  if(!draft.value.trim()||!Number.isFinite(Number(draft.value)))throw Error('Enter a finite number.');
  return Number(draft.value);
 }
 try{return JSON.parse(draft.value)}catch{throw Error('Enter valid JSON, such as [1, 2] or {"key": "value"}.');}
}
function initialDrafts(properties:Record<string,unknown>):Draft[]{
 return Object.entries(properties).filter(([key])=>!isReadOnlyProperty(key)).map(([key,value],i)=>{
  const type:ValueType=typeof value==='string'?'string':typeof value==='number'?'number':typeof value==='boolean'?'boolean':'json';
  return {id:`property-${i}`,originalKey:key,key,value:type==='string'?String(value):JSON.stringify(value),type,removed:false};
 });
}

/** One explicit, staged edit session. No writes occur until Save changes. */
export function PropertyEditor({properties,onSave,onSaved,onCancel}:{properties:Record<string,unknown>;onSave:(properties:Record<string,unknown>)=>Promise<void>;onSaved:()=>void;onCancel:()=>void}){
 const [drafts,setDrafts]=useState(()=>initialDrafts(properties));
 const [errors,setErrors]=useState<Record<string,string>>({});
 const [saveError,setSaveError]=useState('');
 const [saving,setSaving]=useState(false);
 const nextId=useRef(drafts.length);
 const form=useRef<HTMLFormElement>(null);
 const update=(id:string,changes:Partial<Draft>)=>{setDrafts(current=>current.map(d=>d.id===id?{...d,...changes}:d));setSaveError('');};
 const validate=()=>{
  const found:Record<string,string>={};
  for(const draft of drafts.filter(d=>!d.removed)){
   if(!draft.key.trim())found[`${draft.id}-key`]='Enter a property name.';
   else if(isReadOnlyProperty(draft.key))found[`${draft.id}-key`]='This name is managed by the server. Choose another name.';
   else if(drafts.some(other=>other.id!==draft.id&&!other.removed&&other.key===draft.key))found[`${draft.id}-key`]='This name is already used. Choose a unique name.';
   try{parseValue(draft)}catch(error){found[`${draft.id}-value`]=(error as Error).message;}
  }
  return found;
 };
 const changes=drafts.filter(d=>{
  if(d.removed)return d.originalKey!==null;
  if(d.originalKey===null||d.key!==d.originalKey)return true;
  try{return JSON.stringify(parseValue(d))!==JSON.stringify(properties[d.originalKey]);}catch{return true;}
 });
 const {allowLeave,warning}=useUnsavedChanges(changes.length>0,['graph.edge','inspectorView']);
 const save=async()=>{
  const found=validate();setErrors(found);setSaveError('');
  if(Object.keys(found).length){requestAnimationFrame(()=>{const input=form.current?.querySelector<HTMLElement>('[aria-invalid="true"]');const disclosure=input?.closest('details');if(disclosure)disclosure.open=true;input?.focus();});return;}
  setSaving(true);
  try{await onSave(Object.fromEntries(drafts.filter(d=>!d.removed).map(d=>[d.key,parseValue(d)])));allowLeave();onSaved();}
  catch(error){setSaveError(error instanceof Error?error.message:'Could not save changes. Try again.');}
  finally{setSaving(false);}
 };
 const control=(draft:Draft)=>{
  const error=errors[`${draft.id}-value`];
  const shared={id:`${draft.id}-value`,'aria-label':`Value for ${draft.key || 'new property'}`,'aria-invalid':Boolean(error),'aria-describedby':error?`${draft.id}-value-error`:undefined,value:draft.value,onChange:(event:React.ChangeEvent<HTMLInputElement|HTMLTextAreaElement|HTMLSelectElement>)=>update(draft.id,{value:event.target.value}),onBlur:()=>{const found=validate();setErrors(previous=>({...previous,[`${draft.id}-value`]:found[`${draft.id}-value`]||''}));},className:'property-value'};
  return <>{draft.type==='boolean'?<select {...shared}><option value="true">True</option><option value="false">False</option></select>:draft.type==='number'?<input {...shared} type="number" step="any"/>:<textarea {...shared} rows={draft.type==='json'?4:draft.value.includes('\n')?3:1} spellCheck={draft.type==='json'?false:undefined}/>}
   {error&&<p id={`${draft.id}-value-error`} className="field-error">{error}</p>}
   {draft.type==='json'&&<p className="field-hint">JSON array, object, or null.</p>}</>;
 };
 const readOnly=Object.entries(properties).filter(([key])=>isReadOnlyProperty(key));
 return <form ref={form} className="property-edit-session" noValidate onSubmit={event=>{event.preventDefault();void save();}} aria-label="Edit node properties">
  <p className="edit-guidance">Changes stay here until you save. Cancel discards this edit session.</p>
  <fieldset disabled={saving} className="property-fields">
   {drafts.map(draft=><section className={`property-field ${draft.removed?'property-removed':''}`} key={draft.id} aria-label={`Property ${draft.originalKey ?? 'new'}`}>
    {draft.removed?<div className="removed-property"><span><strong>{draft.key || 'New property'}</strong><span className="block">Will be removed on save</span></span><button type="button" onClick={()=>update(draft.id,{removed:false})}><Undo2 size={14}/> Undo</button></div>:<>
     <div className="property-field-heading"><label htmlFor={`${draft.id}-value`}>{draft.key || 'New property'}</label><span>{draft.type==='json'?'JSON':draft.type}</span><button type="button" aria-label={`Remove property ${draft.key || 'new property'}`} title="Stage removal" onClick={()=>update(draft.id,{removed:true})}><X size={15}/></button></div>
     {errors[`${draft.id}-key`]&&<p className="field-error" id={`${draft.id}-key-error`}>{errors[`${draft.id}-key`]}</p>}
     {control(draft)}
     <Disclosure compact id={`edit-${draft.id}`} title={`Name and type for ${draft.key || 'new property'}`} defaultOpen={draft.originalKey===null}>
      <label className="field-label" htmlFor={`${draft.id}-key`}>Property name</label>
      <input id={`${draft.id}-key`} aria-label={`Property name for ${draft.key || 'new property'}`} value={draft.key} onChange={event=>update(draft.id,{key:event.target.value})} aria-invalid={Boolean(errors[`${draft.id}-key`])} aria-describedby={errors[`${draft.id}-key`]?`${draft.id}-key-error`:undefined} autoFocus={draft.originalKey===null} className="property-value"/>

      <label className="field-label" htmlFor={`${draft.id}-type`}>Value type</label>
      <select id={`${draft.id}-type`} aria-label={`Type for ${draft.key || 'new property'}`} value={draft.type} onChange={event=>update(draft.id,{type:event.target.value as ValueType})} className="property-value">
       <option value="string">String</option><option value="number">Number</option><option value="boolean">Boolean</option><option value="json">JSON</option>
      </select>
     </Disclosure>
    </>}
   </section>)}
   <button type="button" className="add-property" onClick={()=>{const id=`property-${nextId.current++}`;setDrafts(current=>[...current,{id,originalKey:null,key:'',value:'',type:'string',removed:false}]);}}><Plus size={15}/> Add property</button>
  </fieldset>
  {readOnly.length>0&&<Disclosure compact id="property-metadata" title={`Server-managed properties (${readOnly.length})`}><p className="field-hint">Read-only values maintained by NornicDB.</p>{readOnly.map(([key,value])=><div key={key} className="managed-property"><strong>{key}</strong><JsonPreview data={value} expanded/></div>)}</Disclosure>}
  {changes.length>0&&<Disclosure compact id="property-review" title={`Review ${changes.length} ${changes.length===1?'change':'changes'}`}><ul className="property-change-list">{changes.map(draft=><li key={draft.id}>{draft.removed?<>Remove <strong>{draft.originalKey}</strong></>:draft.originalKey===null?<>Add <strong>{draft.key || '(unnamed)'}</strong></>:<><strong>{draft.originalKey}</strong>{draft.key!==draft.originalKey?<> → {draft.key}</>:': value changed'}</>}</li>)}</ul></Disclosure>}
  <div className="property-save-bar">{Object.values(errors).some(Boolean)&&<p role="alert" className="field-error">Check the highlighted fields. No changes have been saved.</p>}{saveError&&<p role="alert" className="field-error">{saveError} Your changes are still here. Try saving again.</p>}<p role="status">{saving?'Saving changes…':`${changes.length} unsaved ${changes.length===1?'change':'changes'}`}</p><div><button type="submit" disabled={saving||changes.length===0} className="save-properties">{saving?'Saving…':'Save changes'}</button><button type="button" disabled={saving} onClick={()=>{allowLeave();onCancel();}}>Cancel</button></div></div>
 {warning}
 </form>;
}
