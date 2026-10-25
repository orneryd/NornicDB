"""Successful staged-check receipts, scoped to the worktree and graph baseline."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

def receipt(root, tree, head, config, baseline, extra=()):
    scripts=Path(__file__).resolve().parent
    files=[scripts/name for name in ('pr_duplicates.py','graphify_local.py','code_intelligence.py','duplicate_check_cache.py')]+list(extra)
    extractor = Path(os.environ.get('CODE_INTEL_GRAPHIFY', config.get('graphify', '/nonexistent')))
    extractor_identity = (str(extractor), extractor.stat().st_size, extractor.stat().st_mtime_ns) if extractor.is_file() else None
    identity={'tree':tree,'head':head,'config':config,'baseline':baseline,
              'policy_env':{k:v for k,v in os.environ.items() if k.startswith('CODE_INTEL_')},
              'code':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},
              'graphify': extractor_identity}
    key=hashlib.sha256(json.dumps(identity,sort_keys=True,default=str).encode()).hexdigest()
    raw=subprocess.check_output(['git','-C',str(root),'rev-parse','--git-path','nornic-duplicate-pass.json'],text=True).strip()
    path=Path(raw)
    return (path if path.is_absolute() else Path(root)/path), key

def passed(path,key):
    try:
        row=json.loads(path.read_text())
        return row.get('status')=='passed' and row.get('key')==key
    except (OSError,ValueError):return False

def record(path,key):
    path.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w',dir=path.parent,delete=False) as output:
        json.dump({'status':'passed','key':key},output);temp=Path(output.name)
    temp.replace(path)

def baseline(db,repo):
    rows=db.cypher('MATCH (n:CodeRepository {name: $repo}) RETURN properties(n) AS baseline',repo=repo)
    if not rows:raise RuntimeError('Code graph baseline missing; ingest first; duplicate check cannot reuse a pass')
    return rows


def invalidate(root):
    try:
        raw=subprocess.check_output(['git','-C',str(root),'rev-parse','--git-path','nornic-duplicate-pass.json'],text=True,stderr=subprocess.DEVNULL).strip()
    except subprocess.CalledProcessError:
        return
    path=Path(raw)
    (path if path.is_absolute() else Path(root)/path).unlink(missing_ok=True)
