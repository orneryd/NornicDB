"""Restore all foreign jobs immediately; promote only our requests; mirror only our results."""
import hashlib, json, os, pathlib, shutil, time
root=pathlib.Path('/work/tnrun'); state=root/'operator/fix-1384-regression-r4'; queue=root/'queue/ab'; held=state/'held'
r=json.loads((state/'receipt.json').read_text())
r['original_hold_receipt']=dict(r)
r['user_correction']='Restore all other jobs NOW, prioritize only own four; never hold/remove others again'
r['restored']=[]; r['restore_errors']=[]; r['promotions']=[]
pairs=[('a20261018045918-48476-6ade','a00000000000001-1384-default'),('a20261018045918-61784-2d57','a00000000000002-1384-control-default'),('a20261018045918-84716-05d2','a00000000000003-1384-control-antlr'),('a20261018045919-67144-d619','a00000000000004-1384-antlr')]
def save():
 tmp=state/'receipt.tmp'; tmp.write_text(json.dumps(r,indent=2)); os.replace(tmp,state/'receipt.json')
# Promote only this task's four requests. Other ready files are never changed.
archive=state/'own-originals'; archive.mkdir(exist_ok=True)
for old,new in pairs:
 p=queue/(old+'.json'); target=queue/(new+'.json')
 assert p.is_file() and not target.exists(), 'Own queued request guard failed: '+old
 raw=p.read_bytes(); j=json.loads(raw)
 assert j['id']==old and j['head']=='fc375efa4ba55253cc8a775172fd4933856f00ea'
 assert j['base'] in {'524b71ba3365be6f453a107d77c0816b7396b41a','e138156772abea4912fdea1c3f151ef8065e386b'}
 assert j['rounds']==10 and not (root/'running/bench'/(old+'.json')).exists()
 (archive/(old+'.json')).write_bytes(raw)
 j['id']=new; j['originalRequestId']=old; j['priorityReason']='User: own four next; all other jobs remain ready in FIFO order'
 tmp=queue/(new+'.tmp'); tmp.write_text(json.dumps(j)); os.replace(tmp,target)
 os.unlink(p)
 (archive/(new+'.json')).write_text(json.dumps(j,indent=2))
 r['promotions'].append({'request':old,'execution':new,'sha256Original':hashlib.sha256(raw).hexdigest()})
 save()
# Restore foreign definitions under original names, preserving all their bytes/order.
expected={entry['name']:entry['sha256'] for entry in r['held']}
for p in sorted(held.glob('*.json')):
 target=queue/p.name
 assert p.parent.resolve()==held.resolve() and target.parent.resolve()==queue.resolve()
 data=p.read_bytes(); digest=hashlib.sha256(data).hexdigest()
 assert digest==expected[p.name], 'Held definition changed: '+p.name
 if target.exists():
  assert target.read_bytes()==data, 'Different ready duplicate: '+p.name
  os.unlink(p)
 else: os.rename(p,target)
 r['restored'].append(p.name); save()
assert not list(held.iterdir()), 'Held directory must be empty'
r['all_foreign_jobs_ready_at']=time.time(); r['heldDirectoryEmpty']=True
r['ready_order_after_restore']=[p.stem for p in sorted(queue.glob('*.json'))]
save(); (state/'queue-restored.json').write_text(json.dumps(r,indent=2))
print('RESTORED_ALL '+json.dumps({'restored':len(r['restored']),'heldEmpty':True,'firstFour':r['ready_order_after_restore'][:4]}),flush=True)
# Existing clients keep their logical request IDs. Mirror genuine terminal artifacts
# from the promoted execution IDs; preserve every failure and publish result last.
remaining=dict(pairs); deadline=time.monotonic()+3*3600
while remaining:
 if time.monotonic()>deadline: raise TimeoutError('Own result alias wait expired; all foreign jobs already ready')
 for old,new in list(remaining.items()):
  source=root/'done'/new; result=source/'result.json'
  if not result.is_file(): continue
  terminal=json.loads(result.read_text()); dest=root/'done'/old; dest.mkdir(exist_ok=True)
  for item in source.iterdir():
   if item.is_file() and item.name not in {'result.json','result.tmp'}: shutil.copyfile(item,dest/item.name)
  terminal['originalRequestId']=old; terminal['priorityExecutionId']=new; terminal['priorityAlias']=True
  tmp=dest/'result.tmp'; tmp.write_text(json.dumps(terminal)); os.replace(tmp,dest/'result.json')
  r.setdefault('aliased_results',{})[old]=terminal; del remaining[old]; save()
 time.sleep(1)
r['completed']=r['aliased_results']; r['finished']=time.time(); save()
print('OWN_FOUR_COMPLETE',flush=True)
