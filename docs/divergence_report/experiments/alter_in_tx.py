"""ALTER [COMPOSITE] DATABASE inside an explicit transaction: does it work, and what do COMMIT / ROLLBACK do with it?"""
import sys
from neo4j import GraphDatabase
d = GraphDatabase.driver(f'bolt://{sys.argv[1]}:7687', auth=None)
def auto(q):
    try:
        with d.session(database='system') as s:
            return [r.data() for r in s.run(q)]
    except Exception as e:
        return 'ERR ' + str(e)[:160]
def cons(): 
    r = auto('SHOW CONSTITUENTS FOR COMPOSITE DATABASE cdb')
    return r if isinstance(r, str) else sorted(str(x.get('name') or x) for x in r)
for q in ['CREATE DATABASE a1', 'CREATE DATABASE a2', 'CREATE DATABASE a3', 'CREATE COMPOSITE DATABASE cdb ALIAS x1 FOR DATABASE a1']:
    print(q, '->', auto(q))
print('constituents before:', cons())
for end in ['rollback', 'commit']:
    for q in ['ALTER COMPOSITE DATABASE cdb ADD ALIAS x2 FOR DATABASE a2' if end == 'rollback' else 'ALTER COMPOSITE DATABASE cdb ADD ALIAS x3 FOR DATABASE a3']:
        with d.session(database='system') as s:
            tx = s.begin_transaction()
            try:
                print(f'[tx/{end}]', q, '->', [r.data() for r in tx.run(q)])
            except Exception as e:
                print(f'[tx/{end}]', q, '-> ERR', str(e)[:200])
            try:
                getattr(tx, end)()
            except Exception as e:
                print('  end ERR', str(e)[:160])
        print(f'constituents after {end}:', cons())
with d.session(database='system') as s:
    tx = s.begin_transaction()
    for q in ["ALTER DATABASE a1 SET LIMIT max_nodes = 1000", "SHOW LIMITS FOR DATABASE a1"]:
        try:
            print('[tx/rollback]', q, '->', [r.data() for r in tx.run(q)][:3])
        except Exception as e:
            print('[tx/rollback]', q, '-> ERR', str(e)[:200])
    tx.rollback()
print('limits after rollback:', auto('SHOW LIMITS FOR DATABASE a1'))
