#!/usr/bin/env python3
"""Generate an append-only string arena variant, without unsafe or runtime edits."""
import argparse,subprocess
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--go',required=True);p.add_argument('--directory',required=True);a=p.parse_args()
here=Path(__file__).resolve().parent;dest=Path(a.directory);dest.mkdir(parents=True,exist_ok=True)
s=(here/'main.go').read_text()
s=s.replace('arena      []byte','arena      strings.Builder').replace('d.sh[i].arena = []byte{0}','d.sh[i].arena.WriteByte(0)')
s=s.replace('func (s *packedShard) key(off uint32) []byte {','func (s *packedShard) key(off uint32) string {')
s=s.replace('s.arena[int(off)+8:]','[]byte(s.arena.String()[int(off)+8:])').replace('return s.arena[int(off)+12 : int(off)+12+int(n)]','return s.arena.String()[int(off)+12 : int(off)+12+int(n)]')
s=s.replace('for _, c := range k {\n\t\t\th ^= uint64(c)','for j := range len(k) {\n\t\t\th ^= uint64(k[j])')
s=s.replace('len(s.arena)','s.arena.Len()')
s=s.replace('s.arena = binary.LittleEndian.AppendUint64(s.arena, num)\n\ts.arena = binary.LittleEndian.AppendUint32(s.arena, uint32(len(id)))\n\ts.arena = append(s.arena, id...)','var header [12]byte\n binary.LittleEndian.PutUint64(header[:8],num)\n binary.LittleEndian.PutUint32(header[8:],uint32(len(id)))\n s.arena.Write(header[:]);s.arena.WriteString(id)')
s=s.replace('binary.LittleEndian.Uint64(s.arena[s.slots[i]:])','binary.LittleEndian.Uint64([]byte(s.arena.String()[s.slots[i]:]))')
s=s.replace('"mode": *mode,','"arena_backing":"append-only-string-builder", "mode": *mode,')
(dest/'main.go').write_text(s)
for name in ['go.mod','main_test.go']:(dest/name).write_bytes((here/name).read_bytes())
subprocess.run([a.go,'fmt','./...'],cwd=dest,check=True)
subprocess.run([a.go,'test','-race','-count=1','-timeout=30s','./...'],cwd=dest,check=True)
subprocess.run([a.go,'build','-o',str(dest/'uuid-memory'),'main.go'],cwd=dest,check=True)
