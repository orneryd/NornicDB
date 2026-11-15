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
import threading
import urllib.request

p=argparse.ArgumentParser()
p.add_argument("--old-bin",required=True)
p.add_argument("--new-bin",required=True)
p.add_argument("--old-data",required=True)
p.add_argument("--new-data",required=True)
p.add_argument("--client",required=True)
p.add_argument("--docs",required=True)
p.add_argument("--out",required=True)
p.add_argument("--queries",type=int,default=4000)
p.add_argument("--clients",default="1,8")
p.add_argument("--warmup",type=int,default=64)
p.add_argument("--trials",type=int,default=3)
p.add_argument("--query-cache-size",type=int,default=0)
p.add_argument("--workload",choices=("projected","northwind"),default="projected")
p.add_argument("--parser",choices=("nornic","antlr"),default="nornic")
p.add_argument("--old-source",required=True)
p.add_argument("--prepared",action="store_true",help="source stores already contain the document corpus")
p.add_argument("--phase",default="",help="restrict the workload for a focused comparison")
p.add_argument("--scans",action="store_true",help="run streaming label projections only")
p.add_argument("--memory-profiles",action="store_true",help="separate profiling pass with heap/alloc snapshots outside the query workload")
p.add_argument("--verify-corpus",action="store_true",help="verify all stored document fields against the frozen corpus, then exit")
p.add_argument("--new-source",required=True)
p.add_argument("--fixture-receipt",help="normalization receipt identifying both prepared sources and their table levels")
args=p.parse_args()
fixture_receipt=None
if args.fixture_receipt:
    if not args.prepared:
        raise ValueError("a settled fixture receipt requires prepared sources")
    fixture_receipt=json.loads(Path(args.fixture_receipt).read_text())
    prepared={str(Path(record['prepared']).resolve()):record for record in fixture_receipt['prepared'].values()}
    old=prepared[str(Path(args.old_data).resolve())]
    new=prepared[str(Path(args.new_data).resolve())]
    if set(old['after_levels']) != set(new['after_levels']):
        raise ValueError("comparison sources have different settled table levels")
client_counts=tuple(int(value) for value in args.clients.split(","))
trials=tuple(range(1,args.trials+1))
if args.workload=="northwind" and (not args.prepared or client_counts!=(1,)):
    raise ValueError("Northwind uses prepared stores and one sequential query client")
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
    env.update(NORNICDB_NO_AUTH="true",NORNICDB_EMBEDDING_ENABLED="false",NORNICDB_MEMORY_DECAY_ENABLED="false",NORNICDB_ASYNC_WRITES_ENABLED="false",NORNICDB_QUERY_CACHE_ENABLED="false",NORNICDB_PARSER=args.parser,NORNICDB_CYPHER_QUERY_NORMALIZATION="false",NORNICDB_PPROF_ENABLED=str(args.memory_profiles).lower(),NORNICDB_PPROF_LISTEN="127.0.0.1:19091",GOGC="400",GOMEMLIMIT="8GiB")
    command=[args.old_bin if label=="old" else args.new_bin,"serve","--config",str(config),"--data-dir",str(stores[label]),"--bolt-port","17687","--http-port","17474","--no-auth","--search-bm25-enabled=false","--search-vector-enabled=false","--query-cache-size="+str(args.query_cache_size),"--gc-percent=400","--go-memory-limit-bytes=8589934592"]
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

def memory_snapshot(tag, phase):
    def fetch(route, suffix):
        with urllib.request.urlopen("http://127.0.0.1:19091/debug/pprof/"+route,timeout=90) as response:
            (out/(tag+"-"+suffix+".pb.gz")).write_bytes(response.read())
    # Two GCs flush sync.Pool carry-over and delayed allocation accounting.
    for _ in range(2): fetch("heap?gc=1","heap-"+phase)
    fetch("allocs","alloc-"+phase)

for label in (() if args.prepared and not args.verify_corpus else ("old","new")):
    proc,log=start(label,label+"-seed")
    try:
        command=[args.client,"-docs",args.docs,"-verify" if args.verify_corpus else "-seed"]
        with open(out/(label+"-corpus.txt"),"wb") as receipt:
            subprocess.run(command,check=True,stdout=receipt,stderr=subprocess.STDOUT)
    finally:
        stop(proc,log)

if args.verify_corpus:
    print("Both binaries verified every document field against the source corpus.",flush=True)
    raise SystemExit(0)

for trial in trials:
    order=("old","new") if trial%2==1 else ("new","old")
    for label in order:
        for clients in client_counts:
            tag=f"{label}-{clients}-{trial}"
            print(tag,flush=True)
            shutil.rmtree(stores[label])
            shutil.copytree(args.old_data if label=="old" else args.new_data,stores[label])
            proc,log=start(label,tag)
            try:
                if args.memory_profiles: memory_snapshot(tag,"before")
                if args.workload=="northwind":
                    command=[args.client,"-uri","bolt://127.0.0.1:17687","-database","nornic","-no-auth","-skip-seed","-categories","96","-suppliers","144","-customers","1200","-products","48000","-orders","48000","-iterations",str(args.queries),"-warmup",str(args.warmup),"-label",tag,"-out",str(out/(tag+".json"))]
                    def rss():
                        for line in Path(f"/proc/{proc.pid}/status").read_text().splitlines():
                            if line.startswith("VmRSS:"): return int(line.split()[1])
                        raise RuntimeError("server RSS unavailable")
                    memory={"before_kib":rss(),"peak_kib":0}
                    sample_stop=threading.Event()
                    def sample():
                        while not sample_stop.wait(.01): memory["peak_kib"]=max(memory["peak_kib"],rss())
                    sampler=threading.Thread(target=sample);sampler.start()
                    try:
                        with open(out/(tag+".driver.log"),"wb") as driver_log:
                            subprocess.run(command,check=True,stdout=driver_log,stderr=subprocess.STDOUT)
                    finally:
                        sample_stop.set();sampler.join()
                    memory["after_kib"]=rss();memory["peak_kib"]=max(memory.values())
                    receipt=json.loads((out/(tag+".json")).read_text())
                    receipt["sampled_server_rss"]=memory
                    (out/(tag+".json")).write_text(json.dumps(receipt,indent=2)+"\n")
                else:
                    command=[args.client,"-docs",args.docs,"-pid",str(proc.pid),"-workers",str(clients),"-queries",str(args.queries),"-warmup",str(args.warmup),"-out",str(out/(tag+".json"))]
                    if args.scans: command += ["-scan-only","-warmup","4"]
                    if args.phase: command += ["-phase",args.phase]
                    subprocess.run(command,check=True)
                if args.memory_profiles: memory_snapshot(tag,"after")
            finally:
                stop(proc,log)

for clients in client_counts:
    for trial in trials:
        a=json.loads((out/f"old-{clients}-{trial}.json").read_text())
        b=json.loads((out/f"new-{clients}-{trial}.json").read_text())
        if args.workload=="northwind":
            assert len(a["queries"])==len(b["queries"])==14
            for x,y in zip(a["queries"],b["queries"]):
                assert x["correctness_ok"] and y["correctness_ok"]
                assert (x["name"],x["row_count"],x["result_hash"])==(y["name"],y["row_count"],y["result_hash"]),(trial,x["name"])
        else:
            assert a["corpus_sha256"]==b["corpus_sha256"]
            assert len(a["phases"])==len(b["phases"])
            for x,y in zip(a["phases"],b["phases"]):
                assert (x["name"],x["queries"],x["result_sha256"])==(y["name"],y["queries"],y["result_sha256"]),(clients,trial,x["name"])
metadata={"old_binary":args.old_bin,"new_binary":args.new_bin,"old_source":args.old_source,"new_source":args.new_source,"clients":client_counts,"trials":trials,"warmup":args.warmup,"phase":args.phase,"queries":args.queries,"ordinary_storage_format":True,"query_cache_size":args.query_cache_size,"workload":args.workload,"parser":args.parser,"GOGC":400,"GOMEMLIMIT":"8GiB","measurement_kind":"memory-profile" if args.memory_profiles else "timing","work_directory_retained":str(work),"source_stores_unchanged":True,"fixture_receipt":fixture_receipt}
(out/"metadata.json").write_text(json.dumps(metadata,indent=2)+"\n")
print("All old/new query fingerprints match.",flush=True)
