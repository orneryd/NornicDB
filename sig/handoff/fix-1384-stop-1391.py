"""User-approved stop of one exact superseded benchmark; never stop its worker."""
import json, os, pathlib, signal, time
T=pathlib.Path('/work/tnrun')
D=T/'operator/fix-1384-stop-1391'
j=json.loads((D/'guard.json').read_text())
assert j['id']=='a20261018011000-25632-758b'
assert j['head']=='cc453b3862902611f18d658e0d3acd53bea94d63'
running=T/'running/bench'/(j['id']+'.json')
def active():
 if not running.is_file(): return False
 cur=json.loads(running.read_text())
 assert all(cur[k]==j[k] for k in ['id','head','base','label','binArgs']), 'Exact job guard changed'
 return True
report={'job':j['id'],'head':j['head'],'authorization':'User explicitly approved stopping this superseded job','signals':[],'workerRestarted':False,'otherJobsUntouched':True}
sent=set()
limit=time.monotonic()+120
wanted=[a.encode() for a in j['binArgs']]
while active() and time.monotonic()<limit:
 for proc in pathlib.Path('/proc').glob('[0-9]*'):
  try:
   argv=(proc/'cmdline').read_bytes().split(b'\0')
   if not argv or pathlib.PurePosixPath(argv[0].decode(errors='replace')).name!='timeout': continue
   if b'/in/abround.py' not in argv or not any(a in argv for a in [b'/work/ab/base.test',b'/work/ab/head.test']): continue
   if not any(argv[i:i+len(wanted)]==wanted for i in range(len(argv))): continue
   stat=(proc/'stat').read_text().rsplit(')',1)[1].split()
   pid,pgid,born=int(proc.name),int(stat[2]),stat[19]
   if pid!=pgid or pid<=2 or (pid,born) in sent: continue
   if not active(): break
   current=(proc/'stat').read_text().rsplit(')',1)[1].split()
   if current[19]!=born: continue
   os.killpg(pgid,signal.SIGTERM)
   sent.add((pid,born))
   report['signals'].append({'pid':pid,'pgid':pgid,'signal':'TERM'})
   print('EXACT_BENCH_TERM '+json.dumps(report['signals'][-1]),flush=True)
  except (FileNotFoundError,ProcessLookupError): pass
 time.sleep(.1)
report['stopped']=not active()
result=T/'done'/j['id']/'result.json'
if result.exists(): report['workerResult']=json.loads(result.read_text())
(D/'receipt.json').write_text(json.dumps(report,indent=2))
print('EXACT_BENCH_STOP_RECEIPT '+json.dumps(report),flush=True)
if not report['stopped']: raise RuntimeError('Exact job still running; worker left untouched')
