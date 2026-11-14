"""Linux-only isolated old/new Bolt comparison; input stores are always copied."""
import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time

p=argparse.ArgumentParser()
p.add_argument("--old-bin",required=True)
p.add_argument("--new-bin",required=True)
p.add_argument("--old-data",required=True)
p.add_argument("--new-data",required=True)
p.add_argument("--client",required=True)
p.add_argument("--docs",required=True)
p.add_argument("--out",required=True)
p.add_argument("--queries",type=int,default=4000)
p.add_argument("--prepared",action="store_true",help="source stores already contain the document corpus")
p.add_argument("--scans",action="store_true",help="run streaming label projections only")
p.add_argument("--new-source",default="working tree payload-skip tuning; public Badger 4af841ebc969")
p.add_argument("--old-experimental",action="store_true",help="compare two experimental builds")
args=p.parse_args()
out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
work=Path(tempfile.mkdtemp(prefix="combined-projection-"))
stores={}
for label,source in (("old",args.old_data),("new",args.new_data)):
    stores[label]=work/label
    shutil.copytree(source,stores[label])

def start(label,tag):
    config=out/(tag+".yaml")
    config.write_text("server:\n  bolt_port: 17687\n  http_port: 17474\n")
    env=os.environ.copy()
    env.update(NORNICDB_NO_AUTH="true",NORNICDB_EMBEDDING_ENABLED="false",NORNICDB_MEMORY_DECAY_ENABLED="false",NORNICDB_ASYNC_WRITES_ENABLED="false",NORNICDB_QUERY_CACHE_ENABLED="false",NORNICDB_PARSER="nornic",NORNICDB_CYPHER_QUERY_NORMALIZATION="false",NORNICDB_EXPERIMENTAL_PROPERTY_STORAGE_ENABLED=str(label=="new" or args.old_experimental).lower(),GOGC="400",GOMEMLIMIT="8GiB")
    command=[args.old_bin if label=="old" else args.new_bin,"serve","--config",str(config),"--data-dir",str(stores[label]),"--bolt-port","17687","--http-port","17474","--no-auth","--search-bm25-enabled=false","--search-vector-enabled=false","--query-cache-size=0","--gc-percent=400","--go-memory-limit-bytes=8589934592"]
    log=open(out/(tag+".server.log"),"wb")
    proc=subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT)
    for _ in range(160):
        if proc.poll() is not None:
            log.close();raise RuntimeError(f"server exited: {tag}; see log")
        try:
            with socket.create_connection(("127.0.0.1",17687),timeout=.2):
                return proc,log
        except OSError:
            time.sleep(.25)
    proc.terminate();proc.wait(timeout=30);log.close();raise RuntimeError("server readiness timeout")

def stop(proc,log):
    proc.terminate()
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill();proc.wait();raise RuntimeError("graceful shutdown timed out")
    finally:
        log.close()
    if proc.returncode not in (0,-15):
        raise RuntimeError(f"server exit {proc.returncode}")

for label in (() if args.prepared else ("old","new")):
    proc,log=start(label,label+"-seed")
    try:
        subprocess.run([args.client,"-docs",args.docs,"-seed"],check=True)
    finally:
        stop(proc,log)

for trial in (1,2,3):
    order=("old","new") if trial!=2 else ("new","old")
    for label in order:
        for clients in (1,8):
            tag=f"{label}-{clients}-{trial}"
            print(tag,flush=True)
            proc,log=start(label,tag)
            try:
                command=[args.client,"-docs",args.docs,"-pid",str(proc.pid),"-workers",str(clients),"-queries",str(args.queries),"-out",str(out/(tag+".json"))]
                if args.scans: command += ["-scan-only","-warmup","4"]
                subprocess.run(command,check=True)
            finally:
                stop(proc,log)

for clients in (1,8):
    for trial in (1,2,3):
        a=json.loads((out/f"old-{clients}-{trial}.json").read_text())
        b=json.loads((out/f"new-{clients}-{trial}.json").read_text())
        assert a["corpus_sha256"]==b["corpus_sha256"]
        for x,y in zip(a["phases"],b["phases"]):
            assert (x["name"],x["queries"],x["result_sha256"])==(y["name"],y["queries"],y["result_sha256"]),(clients,trial,x["name"])
metadata={"old_binary":args.old_bin,"new_binary":args.new_bin,"old_source":("496c3c934 experimental before point tuning" if args.old_experimental else "ddc59dc20 (stock upstream Badger)"),"new_source":args.new_source,"query_cache_size":0,"parser":"nornic","GOGC":400,"GOMEMLIMIT":"8GiB","work_directory_retained":str(work),"source_stores_unchanged":True}
(out/"metadata.json").write_text(json.dumps(metadata,indent=2)+"\n")
print("All old/new query fingerprints match.",flush=True)
