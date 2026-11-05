#!/usr/bin/env python3
"""Inject a research-only test with a Go build overlay; production files stay intact."""
import argparse, json, os, subprocess, tempfile
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--go',required=True);p.add_argument('--mode',choices=['dictionary','property','property-names'],required=True);p.add_argument('--profile');p.add_argument('--delete-all',action='store_true');a=p.parse_args()
here=Path(__file__).resolve().parent;repo=here.parent.parent
with tempfile.TemporaryDirectory(prefix='uuid-spike-') as tmp:
 overlay=Path(tmp)/'overlay.json';overlay.write_text(json.dumps({'Replace':{str(repo/'pkg/storage/uuid_memory_spike_probe_test.go'):str(here/'storage_probe_test.go.txt')}}))
 env=dict(os.environ,GOTOOLCHAIN='local',GOMAXPROCS='4',GOGC='100',GOMEMLIMIT='off',SPIKE_MODE=a.mode,SPIKE_CORPUS='namespace')
 if a.delete_all:env['SPIKE_DELETE_ALL']='1'
 if a.profile:env['SPIKE_PROFILE']=str(Path(a.profile).resolve())
 subprocess.run([a.go,'test','-overlay',str(overlay),'./pkg/storage','-run','^TestUUIDMemorySpikeProbe$','-count=1','-v'],cwd=repo,env=env,check=True)
