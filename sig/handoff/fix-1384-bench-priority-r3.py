"""Queue-only priority for the final two 1.4.4 regression checks. Never signal workers."""
import hashlib
import json
import os
from pathlib import Path
import time

root = Path('/work/tnrun')
queue = root / 'queue/ab'
state = root / 'operator/fix-1384-regression-r3'
held = state / 'held'
ids = {'a20261018043103-39980-bd08','a20261018043115-68584-6652'}
held.mkdir(exist_ok=True)
for job_id in sorted(ids):
 p = queue / (job_id + '.json')
 if not p.exists():
  p = root/'running/bench'/(job_id+'.json')
 if not p.exists() and (root/'done'/job_id/'result.json').exists():
  continue
 j = json.loads(p.read_text())
 if j['id'] != job_id or j['label'] != 'wt-fix-1384-red@e1381567 vs wt-fix-1384-dev@065bb258':
  raise RuntimeError('Exact review job guard failed: ' + job_id)
 if any('\r' in a for a in j['binArgs']):
  raise RuntimeError('Uncorrected transport argument: ' + job_id)

receipt = {'authorized':'Parent-thread release-priority decision: 1.4.4 before queued develop work; final regression jobs next; never preempt running job',
 'ids':sorted(ids),'running_before':sorted(p.stem for p in (root/'running/bench').glob('*.json')),
 'held':[], 'restored':[], 'workers_signalled':False, 'apps_restarted':False}
if list(held.iterdir()):
 receipt=json.loads((state/'receipt.json').read_text())
 if set(receipt['ids'])!=ids or receipt.get('completed'):
  raise RuntimeError('Cannot resume another or completed priority window')
 for entry in receipt['held']:
  p=held/entry['name']
  if not p.is_file() or hashlib.sha256(p.read_bytes()).hexdigest()!=entry['sha256']:
   raise RuntimeError('Held source changed before controller resume')
 receipt['resumed']=time.time()
def save():
 p = state/'receipt.tmp'
 p.write_text(json.dumps(receipt,indent=2))
 os.replace(p,state/'receipt.json')

def hold_queued():
 for p in sorted(queue.glob('*.json')):
  if p.stem in ids:
   continue
  target = held/p.name
  if p.parent.resolve() != queue.resolve() or target.parent.resolve() != held.resolve():
   raise RuntimeError('Queue path escaped expected directory')
  data = p.read_bytes()
  entry = {'name':p.name,'sha256':hashlib.sha256(data).hexdigest()}
  if target.exists():
   if hashlib.sha256(target.read_bytes()).hexdigest()!=entry['sha256']:
    raise RuntimeError('Different duplicate queued source: '+p.name)
   duplicate_dir=state/'duplicate-copies'; duplicate_dir.mkdir(exist_ok=True)
   os.rename(p,duplicate_dir/(str(time.time_ns())+'-'+p.name))
   receipt.setdefault('duplicate_copies',[]).append(p.name)
   save()
   continue
  os.rename(p,target)
  receipt['held'].append(entry)
  save()
try:
 hold_queued()
 receipt['started']=time.time()
 save()
 print('SOL_PRIORITY_STARTED '+json.dumps(receipt),flush=True)
 deadline = time.monotonic()+3*3600
 while not all(not (queue/(i+'.json')).exists() and ((root/'running/bench'/(i+'.json')).exists() or (root/'done'/i/'result.json').exists()) for i in ids):
  if time.monotonic()>deadline:
   raise TimeoutError('Priority window expired; restoring previous queue')
  time.sleep(2)
  hold_queued()
 receipt['both_final_jobs_claimed']=time.time()
finally:
 errors=[]
 receipt['already_executed']=[]
 for entry in receipt['held']:
  p=held/entry['name']; target=queue/entry['name']
  try:
   if not p.is_file() or hashlib.sha256(p.read_bytes()).hexdigest()!=entry['sha256']:
    raise RuntimeError('Held job changed or disappeared: '+entry['name'])
   job_id=p.stem
   if (root/'done'/job_id/'result.json').exists() or (root/'running/bench'/entry['name']).exists():
    receipt['already_executed'].append(entry['name'])
    continue
   if target.exists():
    if hashlib.sha256(target.read_bytes()).hexdigest()!=entry['sha256']:
     raise RuntimeError('Different queue destination already exists: '+entry['name'])
    receipt['restored'].append(entry['name'])
    continue
   os.rename(p,target)
   receipt['restored'].append(entry['name'])
  except Exception as e:
   errors.append(str(e))
 receipt['restore_errors']=errors
 receipt['restored_at']=time.time()
 save()
 print('SOL_PRIORITY_RESTORED '+json.dumps(receipt),flush=True)
 if errors:
  raise RuntimeError('; '.join(errors))
while not all((root/'done'/i/'result.json').is_file() for i in ids):
 if time.monotonic()>deadline:
  raise TimeoutError('Queued order restored; two-job result wait expired')
 time.sleep(2)
receipt['completed']={i:json.loads((root/'done'/i/'result.json').read_text()) for i in sorted(ids)}
receipt['finished']=time.time()
save()
print('SOL_PRIORITY_FINISHED '+json.dumps(receipt),flush=True)

