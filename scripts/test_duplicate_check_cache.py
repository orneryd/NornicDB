import sys
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parent))
import duplicate_check_cache as c
class CacheTests(unittest.TestCase):
 def test_pass_only_exact_identity_and_changes_invalidate(self):
  with tempfile.TemporaryDirectory() as tmp, patch.object(c.subprocess,'check_output',return_value=str(Path(tmp)/'receipt.json')):
   root=Path(tmp);config={'repo':'o/r'};baseline=[{'commit':'a'}]
   path,key=c.receipt(root,'tree','head',config,baseline)
   self.assertFalse(c.passed(path,key));c.record(path,key);self.assertTrue(c.passed(path,key))
   for tree,head,cfg,base in [('other','head',config,baseline),('tree','other',config,baseline),('tree','head',{'repo':'o/other'},baseline),('tree','head',config,[{'commit':'b'}])]:
    _,other=c.receipt(root,tree,head,cfg,base);self.assertFalse(c.passed(path,other))
   c.invalidate(root);self.assertFalse(path.exists())
   path.write_text('{invalid');self.assertFalse(c.passed(path,key))
 def test_missing_baseline_blocks(self):
  class DB:
   def cypher(self,*args,**kwargs):return []
  with self.assertRaises(RuntimeError):c.baseline(DB(),'o/r')


class SearchWorkerTests(unittest.TestCase):
 def test_bounded_order_and_failure(self):
  import threading,time
  from types import SimpleNamespace
  import pr_duplicates as pd
  class DB:
   def __init__(self):self.active=0;self.peak=0;self.lock=threading.Lock()
   def search(self,query,**kwargs):
    with self.lock:self.active+=1;self.peak=max(self.peak,self.active)
    time.sleep(.005)
    with self.lock:self.active-=1
    if query=='fail':raise RuntimeError('search failed')
    return query
  db=DB();items=[SimpleNamespace(query=str(i)) for i in range(12)]
  self.assertEqual(list(pd.search_results(db,items,1000,4,time.monotonic()+10)),list(map(str,range(12))))
  self.assertGreater(db.peak,1);self.assertLessEqual(db.peak,4)
  with self.assertRaises(RuntimeError):list(pd.search_results(db,[SimpleNamespace(query='fail')],1000,4,time.monotonic()+10))


class RepositorySchemaTests(unittest.TestCase):
 def test_cache_uses_ingestion_repository_name(self):
  class DB:
   def cypher(self,statement,**params):
    # Importer stores name, never repo, on CodeRepository nodes.
    if 'CodeRepository {name: $repo}' in statement and params['repo']=='o/r':
     return [{'baseline':{'name':'o/r','last_commit':'abc'}}]
    return []
  self.assertEqual(c.baseline(DB(),'o/r')[0]['baseline']['last_commit'],'abc')
